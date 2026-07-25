package httpadapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
)

type Config struct {
	Timeout               time.Duration
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	IdleConnTimeout       time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxResponseBodyBytes  int64
	AllowPrivateNetworks  bool
	AllowedHosts          []string
}

type Adapter struct {
	client       *http.Client
	transport    *http.Transport
	resolver     *net.Resolver
	dialer       net.Dialer
	config       Config
	allowedHosts map[string]struct{}
}

func New(config Config) (*Adapter, error) {
	if config.Timeout <= 0 || config.DialTimeout <= 0 || config.TLSHandshakeTimeout <= 0 ||
		config.ResponseHeaderTimeout <= 0 || config.IdleConnTimeout <= 0 ||
		config.MaxIdleConns <= 0 || config.MaxIdleConnsPerHost <= 0 || config.MaxResponseBodyBytes < 0 {
		return nil, errors.New("invalid HTTP adapter configuration")
	}
	a := &Adapter{
		config:       config,
		resolver:     net.DefaultResolver,
		dialer:       net.Dialer{Timeout: config.DialTimeout, KeepAlive: 30 * time.Second},
		allowedHosts: make(map[string]struct{}, len(config.AllowedHosts)),
	}
	for _, host := range config.AllowedHosts {
		host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
		if host == "" {
			return nil, errors.New("HTTP allowed hosts contains an empty host")
		}
		a.allowedHosts[host] = struct{}{}
	}
	a.transport = &http.Transport{
		// A process-wide proxy could bypass destination DNS/IP validation.
		// Proxy support should be explicit and validate the proxy separately.
		Proxy:                 nil,
		DialContext:           a.dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		IdleConnTimeout:       config.IdleConnTimeout,
		TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
	}
	a.client = &http.Client{
		Transport: a.transport,
		Timeout:   config.Timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return a, nil
}

func (a *Adapter) Type() domain.DestinationType { return domain.DestinationHTTP }

func (a *Adapter) Push(ctx context.Context, req delivery.PushRequest) error {
	target := req.Destination.HTTP
	if target == nil {
		return delivery.NewPushError("invalid_destination", false, errors.New("HTTP destination is missing"))
	}
	parsed, err := url.Parse(target.URL)
	if err != nil || parsed.User != nil || parsed.Hostname() == "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return delivery.NewPushError("invalid_url", false, errors.New("invalid HTTP destination URL"))
	}
	if err := a.validateHost(ctx, parsed.Hostname()); err != nil {
		return delivery.NewPushError("ssrf_blocked", false, err)
	}

	method := strings.ToUpper(target.Method)
	if method == "" {
		method = http.MethodPost
	}
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
		return delivery.NewPushError("invalid_method", false, fmt.Errorf("unsupported HTTP method %q", method))
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, parsed.String(), bytes.NewReader(req.Payload))
	if err != nil {
		return delivery.NewPushError("request_build_failed", false, err)
	}
	for name, value := range target.Headers {
		httpReq.Header.Set(name, value)
	}
	for name, value := range req.Headers {
		httpReq.Header.Set(name, value)
	}
	httpReq.Header.Set("Content-Type", req.ContentType)
	setSystemHeaders(httpReq.Header, req)

	response, err := a.client.Do(httpReq)
	if err != nil {
		return delivery.NewPushError("network_error", true, err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, a.config.MaxResponseBodyBytes+1))
	if readErr != nil {
		return delivery.NewPushError("response_read_error", true, readErr)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}

	retryable := response.StatusCode == http.StatusRequestTimeout ||
		response.StatusCode == http.StatusTooEarly ||
		response.StatusCode == http.StatusTooManyRequests ||
		response.StatusCode >= 500
	code := "http_rejected"
	if retryable {
		code = "http_retryable_status"
	}
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 512 {
		snippet = snippet[:512]
	}
	pushErr := delivery.NewPushError(code, retryable, fmt.Errorf("HTTP status %d: %s", response.StatusCode, snippet))
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusServiceUnavailable {
		pushErr.RetryAfter = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return pushErr
}

func (a *Adapter) Ready(context.Context) error { return nil }

func (a *Adapter) Close(context.Context) error {
	a.transport.CloseIdleConnections()
	return nil
}

func (a *Adapter) validateHost(ctx context.Context, host string) error {
	normalized := strings.ToLower(strings.TrimSuffix(host, "."))
	if len(a.allowedHosts) > 0 {
		if _, ok := a.allowedHosts[normalized]; !ok {
			return fmt.Errorf("host %q is not allowlisted", normalized)
		}
	}
	addresses, err := a.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("resolve target: %w", err)
	}
	if len(addresses) == 0 {
		return errors.New("target has no IP addresses")
	}
	if !a.config.AllowPrivateNetworks {
		for _, address := range addresses {
			if blockedAddress(address) {
				return fmt.Errorf("target address %s is not public", address)
			}
		}
	}
	return nil
}

func (a *Adapter) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses, err := a.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	var last error
	for _, ip := range addresses {
		if !a.config.AllowPrivateNetworks && blockedAddress(ip) {
			last = fmt.Errorf("target address %s is not public", ip)
			continue
		}
		conn, dialErr := a.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		last = dialErr
	}
	if last == nil {
		last = errors.New("target has no usable IP addresses")
	}
	return nil, last
}

func blockedAddress(address netip.Addr) bool {
	return !address.IsValid() || address.IsLoopback() || address.IsPrivate() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsMulticast() || address.IsUnspecified()
}

func setSystemHeaders(headers http.Header, req delivery.PushRequest) {
	id := req.DeliveryID.String()
	headers.Set("Idempotency-Key", id)
	headers.Set("X-DeferMQ-Delivery-ID", id)
	headers.Set("X-DeferMQ-Schedule-Revision", strconv.FormatInt(req.ScheduleRevision, 10))
	headers.Set("X-DeferMQ-Attempt", strconv.Itoa(req.Attempt))
	headers.Set("X-DeferMQ-Scheduled-At", req.ScheduledAt.UTC().Format(time.RFC3339Nano))
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	date, err := http.ParseTime(value)
	if err != nil || !date.After(now) {
		return 0
	}
	return date.Sub(now)
}
