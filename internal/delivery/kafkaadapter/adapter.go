package kafkaadapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Config struct {
	Brokers        []string
	ClientID       string
	AllowedTopics  []string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	TLSEnabled     bool
	TLSCAFile      string
	TLSCertFile    string
	TLSKeyFile     string
	TLSServerName  string
	SASLMechanism  string
	SASLUsername   string
	SASLPassword   string
}

type Adapter struct {
	client         *kgo.Client
	allowed        map[string]struct{}
	requestTimeout time.Duration
}

func New(config Config) (*Adapter, error) {
	if len(config.Brokers) == 0 || config.DialTimeout <= 0 || config.RequestTimeout <= 0 {
		return nil, errors.New("invalid Kafka adapter configuration")
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
		kgo.DialTimeout(config.DialTimeout),
		kgo.RequestTimeoutOverhead(config.RequestTimeout),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	}
	if config.TLSEnabled {
		tlsConfig, err := loadTLSConfig(config)
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.DialTLSConfig(tlsConfig))
	}
	switch strings.ToLower(strings.TrimSpace(config.SASLMechanism)) {
	case "":
	case "plain":
		if config.SASLUsername == "" {
			return nil, errors.New("Kafka SASL username is required")
		}
		options = append(options, kgo.SASL(plain.Auth{
			User: config.SASLUsername,
			Pass: config.SASLPassword,
		}.AsMechanism()))
	case "scram-sha-256":
		if config.SASLUsername == "" {
			return nil, errors.New("Kafka SASL username is required")
		}
		options = append(options, kgo.SASL(scram.Auth{
			User: config.SASLUsername,
			Pass: config.SASLPassword,
		}.AsSha256Mechanism()))
	case "scram-sha-512":
		if config.SASLUsername == "" {
			return nil, errors.New("Kafka SASL username is required")
		}
		options = append(options, kgo.SASL(scram.Auth{
			User: config.SASLUsername,
			Pass: config.SASLPassword,
		}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unsupported Kafka SASL mechanism %q", config.SASLMechanism)
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	a := &Adapter{
		client:         client,
		allowed:        make(map[string]struct{}, len(config.AllowedTopics)),
		requestTimeout: config.RequestTimeout,
	}
	for _, topic := range config.AllowedTopics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			client.Close()
			return nil, errors.New("Kafka topic allowlist contains an empty topic")
		}
		a.allowed[topic] = struct{}{}
	}
	return a, nil
}

func (a *Adapter) Type() domain.DestinationType { return domain.DestinationKafka }

func (a *Adapter) Push(ctx context.Context, req delivery.PushRequest) error {
	pushCtx, cancel := context.WithTimeout(ctx, a.requestTimeout)
	defer cancel()
	target := req.Destination.Kafka
	if target == nil || strings.TrimSpace(target.Topic) == "" {
		return delivery.NewPushError("invalid_topic", false, errors.New("Kafka topic is missing"))
	}
	if len(a.allowed) > 0 {
		if _, ok := a.allowed[target.Topic]; !ok {
			return delivery.NewPushError("topic_not_allowed", false, fmt.Errorf("Kafka topic %q is not allowlisted", target.Topic))
		}
	}
	key := target.Key
	if key == "" {
		key = req.DeliveryID.String()
	}
	record := &kgo.Record{
		Topic: target.Topic,
		Key:   []byte(key),
		Value: req.Payload,
	}
	for name, value := range target.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}
	for name, value := range req.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}
	record.Headers = append(record.Headers, systemHeaders(req)...)

	if err := a.client.ProduceSync(pushCtx, record).FirstErr(); err != nil {
		retryable := kerr.IsRetriable(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
		if errors.Is(err, kerr.InvalidTopicException) ||
			errors.Is(err, kerr.TopicAuthorizationFailed) ||
			errors.Is(err, kerr.ClusterAuthorizationFailed) {
			retryable = false
		}
		return delivery.NewPushError("kafka_publish_failed", retryable, err)
	}
	return nil
}

func (a *Adapter) Ready(ctx context.Context) error {
	return a.client.Ping(ctx)
}

func (a *Adapter) Close(context.Context) error {
	a.client.Close()
	return nil
}

func systemHeaders(req delivery.PushRequest) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "idempotency-key", Value: []byte(req.DeliveryID.String())},
		{Key: "x-defermq-delivery-id", Value: []byte(req.DeliveryID.String())},
		{Key: "x-defermq-schedule-revision", Value: []byte(strconv.FormatInt(req.ScheduleRevision, 10))},
		{Key: "x-defermq-attempt", Value: []byte(strconv.Itoa(req.Attempt))},
		{Key: "x-defermq-scheduled-at", Value: []byte(req.ScheduledAt.UTC().Format(time.RFC3339Nano))},
		{Key: "content-type", Value: []byte(req.ContentType)},
	}
}

func loadTLSConfig(config Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.TLSServerName}
	if config.TLSCAFile != "" {
		pem, err := os.ReadFile(config.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read Kafka CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka CA file contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if (config.TLSCertFile == "") != (config.TLSKeyFile == "") {
		return nil, errors.New("Kafka TLS certificate and key must be configured together")
	}
	if config.TLSCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}
