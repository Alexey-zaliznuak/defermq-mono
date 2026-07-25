package loadtest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// GatewayClient is a bounded-concurrency client for the DeferMQ HTTP API.
type GatewayClient struct {
	baseURL   string
	client    *http.Client
	createSem chan struct{}
	statusSem chan struct{}
}

type gatewayMessage struct {
	ID            string     `json:"id"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	LastError     *string    `json:"last_error"`
	DeliverAt     time.Time  `json:"deliver_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	DeliveredAt   *time.Time `json:"delivered_at"`
}

func NewGatewayClient(config GatewayConfig, createConcurrency, statusConcurrency int) (*GatewayClient, error) {
	if strings.TrimSpace(config.URL) == "" || createConcurrency < 1 || statusConcurrency < 1 {
		return nil, fmt.Errorf("gateway URL and positive concurrency limits are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	connectionBudget := createConcurrency + statusConcurrency
	transport.MaxIdleConns = connectionBudget
	transport.MaxIdleConnsPerHost = connectionBudget
	transport.MaxConnsPerHost = connectionBudget
	return &GatewayClient{
		baseURL:   strings.TrimRight(config.URL, "/"),
		client:    &http.Client{Timeout: config.Timeout.Value(), Transport: transport},
		createSem: make(chan struct{}, createConcurrency),
		statusSem: make(chan struct{}, statusConcurrency),
	}, nil
}

func (c *GatewayClient) Create(
	ctx context.Context,
	planned PlannedMessage,
	destinationURL string,
	idempotencyKey string,
) (string, error) {
	body := struct {
		DeliverAt   time.Time `json:"deliver_at"`
		MaxAttempts int       `json:"max_attempts"`
		Destination any       `json:"destination"`
		Payload     any       `json:"payload"`
	}{
		DeliverAt:   planned.DeliverAt.UTC(),
		MaxAttempts: planned.MaxAttempts,
		Destination: map[string]any{
			"type": "http",
			"http": map[string]any{"url": destinationURL, "method": http.MethodPost},
		},
		Payload: map[string]any{
			"content_type": "application/octet-stream",
			"body_base64":  base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, planned.PayloadBytes)),
		},
	}
	var response gatewayMessage
	if err := c.doJSON(ctx, c.createSem, http.MethodPost, "/v1/messages", idempotencyKey, body, http.StatusAccepted, &response); err != nil {
		return "", err
	}
	if response.ID == "" {
		return "", fmt.Errorf("Gateway create response has empty id")
	}
	return response.ID, nil
}

func (c *GatewayClient) Reschedule(ctx context.Context, id string, deliverAt time.Time) error {
	return c.doJSON(ctx, c.createSem, http.MethodPatch, "/v1/messages/"+id+"/schedule", "", map[string]any{
		"deliver_at": deliverAt.UTC(),
	}, http.StatusOK, nil)
}

func (c *GatewayClient) Cancel(ctx context.Context, id string) error {
	return c.doJSON(ctx, c.createSem, http.MethodDelete, "/v1/messages/"+id, "", nil, http.StatusOK, nil)
}

func (c *GatewayClient) Status(ctx context.Context, id string) (StatusObservation, error) {
	var response gatewayMessage
	if err := c.doJSON(ctx, c.statusSem, http.MethodGet, "/v1/messages/"+id, "", nil, http.StatusOK, &response); err != nil {
		return StatusObservation{}, err
	}
	lastError := ""
	if response.LastError != nil {
		lastError = *response.LastError
	}
	return StatusObservation{
		DeliveryID:    id,
		Status:        response.Status,
		Attempts:      response.Attempts,
		LastError:     lastError,
		DeliverAt:     response.DeliverAt,
		LastAttemptAt: response.LastAttemptAt,
		DeliveredAt:   response.DeliveredAt,
		ObservedAt:    time.Now().UTC(),
	}, nil
}

func (c *GatewayClient) doJSON(
	ctx context.Context,
	semaphore chan struct{},
	method, path, idempotencyKey string,
	body any,
	expectedStatus int,
	target any,
) error {
	select {
	case semaphore <- struct{}{}:
		defer func() { <-semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Gateway request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build Gateway request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("%s Gateway request: %w", method, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()
	if response.StatusCode != expectedStatus {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s %s returned %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(data)))
	}
	if target != nil {
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			return fmt.Errorf("decode Gateway response: %w", err)
		}
	}
	return nil
}
