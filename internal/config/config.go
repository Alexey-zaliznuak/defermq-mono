package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Service string

const (
	ServiceGateway Service = "gateway"
	ServiceManager Service = "manager"
	ServicePusher  Service = "pusher"
)

type Config struct {
	Service        Service
	Common         Common
	Postgres       Postgres
	NATS           NATS
	Gateway        GatewayConfig
	Manager        Manager
	Pusher         Pusher
	HTTPAdapter    HTTPAdapter
	Kafka          Kafka
	Rabbit         Rabbit
	TargetPostgres TargetPostgres
	Admin          Admin
}

type Common struct {
	Environment         string
	ServiceName         string
	InstanceID          string
	Log                 Log
	ShutdownTimeout     time.Duration
	HotHorizon          time.Duration
	MaxPayloadBytes     int64
	EnabledDestinations []string
}

type Log struct {
	Level           string
	Format          string
	StacktraceLevel string
	SamplingEnabled bool
}

type Postgres struct {
	DSN                 string
	MaxConns            int32
	MinConns            int32
	MaxConnLifetime     time.Duration
	MaxConnIdleTime     time.Duration
	HealthCheckPeriod   time.Duration
	ConnectTimeout      time.Duration
	QueryTimeout        time.Duration
	MetricsQueryTimeout time.Duration
	AutoMigrate         bool
}

type NATS struct {
	URL                   string
	Name                  string
	ConnectTimeout        time.Duration
	ReconnectWait         time.Duration
	MaxReconnects         int
	PublishTimeout        time.Duration
	Stream                string
	StreamReplicas        int
	StreamMaxAge          time.Duration
	StreamMaxBytes        int64
	StreamMaxMessageSize  int32
	DuplicateWindow       time.Duration
	ScheduleSubjectPrefix string
	ReadySubjectPrefix    string
	User                  string
	Password              string
	CredentialsFile       string
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string
	TLSServerName         string
}

type GatewayConfig struct {
	HTTPAddr               string
	ReadTimeout            time.Duration
	ReadHeaderTimeout      time.Duration
	WriteTimeout           time.Duration
	IdleTimeout            time.Duration
	RequestTimeout         time.Duration
	MaxHeaderBytes         int
	DefaultMaxAttempts     int
	MaxIdempotencyKeyBytes int
}

type Manager struct {
	HTTPAddr                  string
	PromoterBatchSize         int
	PromoterPollInterval      time.Duration
	PromoterErrorBackoff      time.Duration
	OutboxWorkers             int
	OutboxBatchSize           int
	OutboxPollInterval        time.Duration
	OutboxLockTTL             time.Duration
	OutboxRetryInitial        time.Duration
	OutboxRetryMax            time.Duration
	OverdueInterval           time.Duration
	OverdueGrace              time.Duration
	OverdueBatchSize          int
	ReaperInterval            time.Duration
	RecoveryDelay             time.Duration
	RetentionInterval         time.Duration
	TerminalRetention         time.Duration
	OutboxRetention           time.Duration
	RetentionBatchSize        int
	MetricsCollectionInterval time.Duration
}

type Pusher struct {
	HTTPAddr                  string
	FetchBatchSize            int
	FetchMaxWait              time.Duration
	WorkersHTTP               int
	WorkersKafka              int
	WorkersRabbit             int
	WorkersPostgres           int
	AckWait                   time.Duration
	MaxAckPending             int
	MaxDeliver                int
	ProcessingLease           time.Duration
	LeaseHeartbeatInterval    time.Duration
	ClockSkewTolerance        time.Duration
	RetryInitialBackoff       time.Duration
	RetryMultiplier           float64
	RetryMaxBackoff           time.Duration
	RetryJitter               string
	MetricsCollectionInterval time.Duration
}

type HTTPAdapter struct {
	PushTimeout           time.Duration
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

type Kafka struct {
	Enabled        bool
	Brokers        []string
	ClientID       string
	AllowedTopics  []string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	TLSEnabled     bool
	TLSCAFile      string
	TLSCertFile    string
	TLSKeyFile     string
	SASLMechanism  string
	SASLUsername   string
	SASLPassword   string
}

type Rabbit struct {
	Enabled          bool
	URL              string
	AllowedExchanges []string
	ConnectTimeout   time.Duration
	PublishTimeout   time.Duration
	ReconnectInitial time.Duration
	ReconnectMax     time.Duration
	Mandatory        bool
}

type TargetPostgres struct {
	Enabled         bool
	DSN             string
	Table           string
	AutoCreateTable bool
	MaxConns        int32
	QueryTimeout    time.Duration
}

type Admin struct {
	MetricsEnabled bool
	PprofEnabled   bool
}

type LookupFunc func(string) (string, bool)

func Load(service Service) (Config, error) {
	return LoadFromLookup(service, os.LookupEnv)
}

func LoadFromLookup(service Service, lookup LookupFunc) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("config lookup function is nil")
	}
	r := reader{lookup: lookup}
	defaultServiceName := "defermq-" + string(service)
	cfg := Config{
		Service: service,
		Common: Common{
			Environment: r.text("DEFERMQ_ENV", "development"),
			ServiceName: r.text("DEFERMQ_SERVICE_NAME", defaultServiceName),
			InstanceID:  r.text("DEFERMQ_INSTANCE_ID", ""),
			Log: Log{
				Level:           r.text("DEFERMQ_LOG_LEVEL", "info"),
				Format:          r.text("DEFERMQ_LOG_FORMAT", "json"),
				StacktraceLevel: r.text("DEFERMQ_LOG_STACKTRACE_LEVEL", "error"),
				SamplingEnabled: r.boolean("DEFERMQ_LOG_SAMPLING_ENABLED", true),
			},
			ShutdownTimeout:     r.duration("DEFERMQ_SHUTDOWN_TIMEOUT", 20*time.Second),
			HotHorizon:          r.duration("DEFERMQ_HOT_HORIZON", 2*time.Minute),
			MaxPayloadBytes:     r.int64("DEFERMQ_MAX_PAYLOAD_BYTES", 1<<20),
			EnabledDestinations: r.list("DEFERMQ_ENABLED_DESTINATIONS", []string{"http", "kafka", "rabbit", "postgres"}),
		},
		Postgres: Postgres{
			DSN:                 r.text("DEFERMQ_POSTGRES_DSN", ""),
			MaxConns:            r.int32("DEFERMQ_POSTGRES_MAX_CONNS", 30),
			MinConns:            r.int32("DEFERMQ_POSTGRES_MIN_CONNS", 2),
			MaxConnLifetime:     r.duration("DEFERMQ_POSTGRES_MAX_CONN_LIFETIME", time.Hour),
			MaxConnIdleTime:     r.duration("DEFERMQ_POSTGRES_MAX_CONN_IDLE_TIME", 15*time.Minute),
			HealthCheckPeriod:   r.duration("DEFERMQ_POSTGRES_HEALTH_CHECK_PERIOD", 30*time.Second),
			ConnectTimeout:      r.duration("DEFERMQ_POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
			QueryTimeout:        r.duration("DEFERMQ_POSTGRES_QUERY_TIMEOUT", 5*time.Second),
			MetricsQueryTimeout: r.duration("DEFERMQ_POSTGRES_METRICS_QUERY_TIMEOUT", 3*time.Second),
			AutoMigrate:         r.boolean("DEFERMQ_POSTGRES_AUTO_MIGRATE", true),
		},
		NATS: NATS{
			URL:                   r.text("DEFERMQ_NATS_URL", "nats://localhost:4222"),
			Name:                  r.text("DEFERMQ_NATS_NAME", ""),
			ConnectTimeout:        r.duration("DEFERMQ_NATS_CONNECT_TIMEOUT", 5*time.Second),
			ReconnectWait:         r.duration("DEFERMQ_NATS_RECONNECT_WAIT", time.Second),
			MaxReconnects:         r.integer("DEFERMQ_NATS_MAX_RECONNECTS", -1),
			PublishTimeout:        r.duration("DEFERMQ_NATS_PUBLISH_TIMEOUT", 5*time.Second),
			Stream:                r.text("DEFERMQ_NATS_STREAM", "DEFERMQ"),
			StreamReplicas:        r.integer("DEFERMQ_NATS_STREAM_REPLICAS", 1),
			StreamMaxAge:          r.duration("DEFERMQ_NATS_STREAM_MAX_AGE", 24*time.Hour),
			StreamMaxBytes:        r.int64("DEFERMQ_NATS_STREAM_MAX_BYTES", 1<<30),
			StreamMaxMessageSize:  r.int32("DEFERMQ_NATS_STREAM_MAX_MSG_SIZE", 65536),
			DuplicateWindow:       r.duration("DEFERMQ_NATS_DUPLICATE_WINDOW", 10*time.Minute),
			ScheduleSubjectPrefix: r.text("DEFERMQ_NATS_SCHEDULE_SUBJECT_PREFIX", "defermq.schedule"),
			ReadySubjectPrefix:    r.text("DEFERMQ_NATS_READY_SUBJECT_PREFIX", "defermq.ready"),
			User:                  r.text("DEFERMQ_NATS_USER", ""),
			Password:              r.text("DEFERMQ_NATS_PASSWORD", ""),
			CredentialsFile:       r.text("DEFERMQ_NATS_CREDS_FILE", ""),
			TLSCAFile:             r.text("DEFERMQ_NATS_TLS_CA_FILE", ""),
			TLSCertFile:           r.text("DEFERMQ_NATS_TLS_CERT_FILE", ""),
			TLSKeyFile:            r.text("DEFERMQ_NATS_TLS_KEY_FILE", ""),
			TLSServerName:         r.text("DEFERMQ_NATS_TLS_SERVER_NAME", ""),
		},
		Gateway: GatewayConfig{
			HTTPAddr:               r.text("DEFERMQ_GATEWAY_HTTP_ADDR", ":8080"),
			ReadTimeout:            r.duration("DEFERMQ_GATEWAY_READ_TIMEOUT", 10*time.Second),
			ReadHeaderTimeout:      r.duration("DEFERMQ_GATEWAY_READ_HEADER_TIMEOUT", 5*time.Second),
			WriteTimeout:           r.duration("DEFERMQ_GATEWAY_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:            r.duration("DEFERMQ_GATEWAY_IDLE_TIMEOUT", 60*time.Second),
			RequestTimeout:         r.duration("DEFERMQ_GATEWAY_REQUEST_TIMEOUT", 10*time.Second),
			MaxHeaderBytes:         r.integer("DEFERMQ_GATEWAY_MAX_HEADER_BYTES", 1<<20),
			DefaultMaxAttempts:     r.integer("DEFERMQ_GATEWAY_DEFAULT_MAX_ATTEMPTS", 10),
			MaxIdempotencyKeyBytes: r.integer("DEFERMQ_GATEWAY_MAX_IDEMPOTENCY_KEY_BYTES", 200),
		},
		Manager: Manager{
			HTTPAddr:                  r.text("DEFERMQ_MANAGER_HTTP_ADDR", ":8081"),
			PromoterBatchSize:         r.integer("DEFERMQ_MANAGER_PROMOTER_BATCH_SIZE", 1000),
			PromoterPollInterval:      r.duration("DEFERMQ_MANAGER_PROMOTER_POLL_INTERVAL", 500*time.Millisecond),
			PromoterErrorBackoff:      r.duration("DEFERMQ_MANAGER_PROMOTER_ERROR_BACKOFF", time.Second),
			OutboxWorkers:             r.integer("DEFERMQ_MANAGER_OUTBOX_WORKERS", 4),
			OutboxBatchSize:           r.integer("DEFERMQ_MANAGER_OUTBOX_BATCH_SIZE", 200),
			OutboxPollInterval:        r.duration("DEFERMQ_MANAGER_OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
			OutboxLockTTL:             r.duration("DEFERMQ_MANAGER_OUTBOX_LOCK_TTL", 30*time.Second),
			OutboxRetryInitial:        r.duration("DEFERMQ_MANAGER_OUTBOX_RETRY_INITIAL", 250*time.Millisecond),
			OutboxRetryMax:            r.duration("DEFERMQ_MANAGER_OUTBOX_RETRY_MAX", 30*time.Second),
			OverdueInterval:           r.duration("DEFERMQ_MANAGER_OVERDUE_INTERVAL", time.Second),
			OverdueGrace:              r.duration("DEFERMQ_MANAGER_OVERDUE_GRACE", 2*time.Second),
			OverdueBatchSize:          r.integer("DEFERMQ_MANAGER_OVERDUE_BATCH_SIZE", 1000),
			ReaperInterval:            r.duration("DEFERMQ_MANAGER_REAPER_INTERVAL", 5*time.Second),
			RecoveryDelay:             r.duration("DEFERMQ_MANAGER_RECOVERY_DELAY", 0),
			RetentionInterval:         r.duration("DEFERMQ_MANAGER_RETENTION_INTERVAL", 10*time.Minute),
			TerminalRetention:         r.duration("DEFERMQ_MANAGER_TERMINAL_RETENTION", 168*time.Hour),
			OutboxRetention:           r.duration("DEFERMQ_MANAGER_OUTBOX_RETENTION", 24*time.Hour),
			RetentionBatchSize:        r.integer("DEFERMQ_MANAGER_RETENTION_BATCH_SIZE", 1000),
			MetricsCollectionInterval: r.duration("DEFERMQ_MANAGER_METRICS_COLLECTION_INTERVAL", 5*time.Second),
		},
		Pusher: Pusher{
			HTTPAddr:                  r.text("DEFERMQ_PUSHER_HTTP_ADDR", ":8082"),
			FetchBatchSize:            r.integer("DEFERMQ_PUSHER_FETCH_BATCH_SIZE", 100),
			FetchMaxWait:              r.duration("DEFERMQ_PUSHER_FETCH_MAX_WAIT", 2*time.Second),
			WorkersHTTP:               r.integer("DEFERMQ_PUSHER_WORKERS_HTTP", 32),
			WorkersKafka:              r.integer("DEFERMQ_PUSHER_WORKERS_KAFKA", 8),
			WorkersRabbit:             r.integer("DEFERMQ_PUSHER_WORKERS_RABBIT", 8),
			WorkersPostgres:           r.integer("DEFERMQ_PUSHER_WORKERS_POSTGRES", 16),
			AckWait:                   r.duration("DEFERMQ_PUSHER_ACK_WAIT", 2*time.Minute),
			MaxAckPending:             r.integer("DEFERMQ_PUSHER_MAX_ACK_PENDING", 5000),
			MaxDeliver:                r.integer("DEFERMQ_PUSHER_MAX_DELIVER", 20),
			ProcessingLease:           r.duration("DEFERMQ_PUSHER_PROCESSING_LEASE", 60*time.Second),
			LeaseHeartbeatInterval:    r.duration("DEFERMQ_PUSHER_LEASE_HEARTBEAT_INTERVAL", 20*time.Second),
			ClockSkewTolerance:        r.duration("DEFERMQ_PUSHER_CLOCK_SKEW_TOLERANCE", 10*time.Millisecond),
			RetryInitialBackoff:       r.duration("DEFERMQ_PUSHER_RETRY_INITIAL_BACKOFF", time.Second),
			RetryMultiplier:           r.float("DEFERMQ_PUSHER_RETRY_MULTIPLIER", 2),
			RetryMaxBackoff:           r.duration("DEFERMQ_PUSHER_RETRY_MAX_BACKOFF", 15*time.Minute),
			RetryJitter:               r.text("DEFERMQ_PUSHER_RETRY_JITTER", "full"),
			MetricsCollectionInterval: r.duration("DEFERMQ_PUSHER_METRICS_COLLECTION_INTERVAL", 5*time.Second),
		},
		HTTPAdapter: HTTPAdapter{
			PushTimeout:           r.duration("DEFERMQ_HTTP_PUSH_TIMEOUT", 15*time.Second),
			DialTimeout:           r.duration("DEFERMQ_HTTP_DIAL_TIMEOUT", 3*time.Second),
			TLSHandshakeTimeout:   r.duration("DEFERMQ_HTTP_TLS_HANDSHAKE_TIMEOUT", 5*time.Second),
			ResponseHeaderTimeout: r.duration("DEFERMQ_HTTP_RESPONSE_HEADER_TIMEOUT", 10*time.Second),
			IdleConnTimeout:       r.duration("DEFERMQ_HTTP_IDLE_CONN_TIMEOUT", 90*time.Second),
			MaxIdleConns:          r.integer("DEFERMQ_HTTP_MAX_IDLE_CONNS", 200),
			MaxIdleConnsPerHost:   r.integer("DEFERMQ_HTTP_MAX_IDLE_CONNS_PER_HOST", 20),
			MaxResponseBodyBytes:  r.int64("DEFERMQ_HTTP_MAX_RESPONSE_BODY_BYTES", 65536),
			AllowPrivateNetworks:  r.boolean("DEFERMQ_HTTP_ALLOW_PRIVATE_NETWORKS", true),
			AllowedHosts:          r.list("DEFERMQ_HTTP_ALLOWED_HOSTS", nil),
		},
		Kafka: Kafka{
			Enabled:        r.boolean("DEFERMQ_KAFKA_ENABLED", false),
			Brokers:        r.list("DEFERMQ_KAFKA_BROKERS", []string{"kafka:9092"}),
			ClientID:       r.text("DEFERMQ_KAFKA_CLIENT_ID", "defermq"),
			AllowedTopics:  r.list("DEFERMQ_KAFKA_ALLOWED_TOPICS", nil),
			DialTimeout:    r.duration("DEFERMQ_KAFKA_DIAL_TIMEOUT", 5*time.Second),
			RequestTimeout: r.duration("DEFERMQ_KAFKA_REQUEST_TIMEOUT", 15*time.Second),
			TLSEnabled:     r.boolean("DEFERMQ_KAFKA_TLS_ENABLED", false),
			TLSCAFile:      r.text("DEFERMQ_KAFKA_TLS_CA_FILE", ""),
			TLSCertFile:    r.text("DEFERMQ_KAFKA_TLS_CERT_FILE", ""),
			TLSKeyFile:     r.text("DEFERMQ_KAFKA_TLS_KEY_FILE", ""),
			SASLMechanism:  r.text("DEFERMQ_KAFKA_SASL_MECHANISM", ""),
			SASLUsername:   r.text("DEFERMQ_KAFKA_SASL_USERNAME", ""),
			SASLPassword:   r.text("DEFERMQ_KAFKA_SASL_PASSWORD", ""),
		},
		Rabbit: Rabbit{
			Enabled:          r.boolean("DEFERMQ_RABBIT_ENABLED", false),
			URL:              r.text("DEFERMQ_RABBIT_URL", ""),
			AllowedExchanges: r.list("DEFERMQ_RABBIT_ALLOWED_EXCHANGES", nil),
			ConnectTimeout:   r.duration("DEFERMQ_RABBIT_CONNECT_TIMEOUT", 5*time.Second),
			PublishTimeout:   r.duration("DEFERMQ_RABBIT_PUBLISH_TIMEOUT", 15*time.Second),
			ReconnectInitial: r.duration("DEFERMQ_RABBIT_RECONNECT_INITIAL", 500*time.Millisecond),
			ReconnectMax:     r.duration("DEFERMQ_RABBIT_RECONNECT_MAX", 30*time.Second),
			Mandatory:        r.boolean("DEFERMQ_RABBIT_MANDATORY", false),
		},
		TargetPostgres: TargetPostgres{
			Enabled:         r.boolean("DEFERMQ_TARGET_POSTGRES_ENABLED", false),
			DSN:             r.text("DEFERMQ_TARGET_POSTGRES_DSN", ""),
			Table:           r.text("DEFERMQ_TARGET_POSTGRES_TABLE", "defermq_messages"),
			AutoCreateTable: r.boolean("DEFERMQ_TARGET_POSTGRES_AUTO_CREATE_TABLE", true),
			MaxConns:        r.int32("DEFERMQ_TARGET_POSTGRES_MAX_CONNS", 10),
			QueryTimeout:    r.duration("DEFERMQ_TARGET_POSTGRES_QUERY_TIMEOUT", 10*time.Second),
		},
		Admin: Admin{
			MetricsEnabled: r.boolean("DEFERMQ_METRICS_ENABLED", true),
			PprofEnabled:   r.boolean("DEFERMQ_PPROF_ENABLED", false),
		},
	}
	if r.err != nil {
		return Config{}, r.err
	}
	if cfg.Common.InstanceID == "" {
		cfg.Common.InstanceID = newInstanceID()
	}
	if cfg.NATS.Name == "" {
		cfg.NATS.Name = cfg.Common.ServiceName + "-" + cfg.Common.InstanceID
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) DestinationEnabled(destination string) bool {
	for _, enabled := range c.Common.EnabledDestinations {
		if enabled == destination {
			return true
		}
	}
	return false
}

type reader struct {
	lookup LookupFunc
	err    error
}

func (r *reader) raw(key string) (string, bool) {
	value, ok := r.lookup(key)
	return strings.TrimSpace(value), ok
}

func (r *reader) text(key, fallback string) string {
	if value, ok := r.raw(key); ok {
		return value
	}
	return fallback
}

func (r *reader) parse(key, fallback, kind string, parse func(string) error) {
	if r.err != nil {
		return
	}
	if err := parse(fallback); err != nil {
		r.err = fmt.Errorf("%s must be %s: %w", key, kind, err)
	}
}

func (r *reader) duration(key string, fallback time.Duration) time.Duration {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "a Go duration", func(s string) error {
			parsed, err := time.ParseDuration(s)
			value = parsed
			return err
		})
	}
	return value
}

func (r *reader) boolean(key string, fallback bool) bool {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "a boolean", func(s string) error {
			parsed, err := strconv.ParseBool(s)
			value = parsed
			return err
		})
	}
	return value
}

func (r *reader) integer(key string, fallback int) int {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "an integer", func(s string) error {
			parsed, err := strconv.Atoi(s)
			value = parsed
			return err
		})
	}
	return value
}

func (r *reader) int32(key string, fallback int32) int32 {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "a 32-bit integer", func(s string) error {
			parsed, err := strconv.ParseInt(s, 10, 32)
			value = int32(parsed)
			return err
		})
	}
	return value
}

func (r *reader) int64(key string, fallback int64) int64 {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "an integer", func(s string) error {
			parsed, err := strconv.ParseInt(s, 10, 64)
			value = parsed
			return err
		})
	}
	return value
}

func (r *reader) float(key string, fallback float64) float64 {
	value := fallback
	if raw, ok := r.raw(key); ok {
		r.parse(key, raw, "a number", func(s string) error {
			parsed, err := strconv.ParseFloat(s, 64)
			value = parsed
			return err
		})
	}
	return value
}

func (r *reader) list(key string, fallback []string) []string {
	raw, ok := r.raw(key)
	if !ok {
		return append([]string(nil), fallback...)
	}
	if raw == "" {
		return nil
	}
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; !exists {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

func newInstanceID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
	}
	return host + "-" + hex.EncodeToString(suffix[:])
}
