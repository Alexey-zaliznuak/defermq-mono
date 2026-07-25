package pusher

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/delivery/httpadapter"
	"github.com/defermq/defermq/internal/delivery/kafkaadapter"
	"github.com/defermq/defermq/internal/delivery/postgresadapter"
	"github.com/defermq/defermq/internal/delivery/rabbitadapter"
	"github.com/defermq/defermq/internal/domain"
)

type RuntimeConfig struct {
	HTTPAddr           string
	InstanceID         string
	LogLevel           string
	ShutdownTimeout    time.Duration
	SourceDSN          string
	SourceMaxConns     int32
	SynchronousCommit  string
	QueryTimeout       time.Duration
	NATSURL            string
	NATSUser           string
	NATSPassword       string
	NATSCredsFile      string
	NATSTLSCAFile      string
	NATSTLSCertFile    string
	NATSTLSKeyFile     string
	NATSTLSServerName  string
	NATSName           string
	NATSStream         string
	SubjectsReady      string
	NATSConnectTimeout time.Duration
	NATSReconnectWait  time.Duration
	NATSMaxReconnects  int
	AckWait            time.Duration
	MaxAckPending      int
	MaxDeliver         int
	FetchBatch         int
	FetchMaxWait       time.Duration
	ClaimBatch         int
	ClaimFlushInterval time.Duration
	Lease              time.Duration
	Heartbeat          time.Duration
	ClockTolerance     time.Duration
	MaxPayload         int64
	HotHorizon         time.Duration
	Backoff            delivery.Backoff
	Enabled            []domain.DestinationType
	Workers            map[domain.DestinationType]int
	HTTP               httpadapter.Config
	Kafka              kafkaadapter.Config
	Rabbit             rabbitadapter.Config
	TargetPostgres     postgresadapter.Config
}

func LoadConfig() (RuntimeConfig, error) {
	var config RuntimeConfig
	var err error
	config.HTTPAddr = envString("DEFERMQ_PUSHER_HTTP_ADDR", ":8082")
	config.InstanceID = envString("DEFERMQ_INSTANCE_ID", defaultInstanceID())
	config.LogLevel = envString("DEFERMQ_LOG_LEVEL", "info")
	if config.ShutdownTimeout, err = envDuration("DEFERMQ_SHUTDOWN_TIMEOUT", 20*time.Second); err != nil {
		return config, err
	}
	config.SourceDSN = os.Getenv("DEFERMQ_POSTGRES_DSN")
	if config.SourceDSN == "" {
		return config, fmt.Errorf("DEFERMQ_POSTGRES_DSN is required")
	}
	if config.SourceMaxConns, err = envInt32("DEFERMQ_POSTGRES_MAX_CONNS", 30); err != nil {
		return config, err
	}
	config.SynchronousCommit = envString("DEFERMQ_PUSHER_POSTGRES_SYNCHRONOUS_COMMIT", "off")
	switch config.SynchronousCommit {
	case "on", "off", "local", "remote_write", "remote_apply":
	default:
		return config, fmt.Errorf("invalid DEFERMQ_PUSHER_POSTGRES_SYNCHRONOUS_COMMIT %q", config.SynchronousCommit)
	}
	if config.QueryTimeout, err = envDuration("DEFERMQ_POSTGRES_QUERY_TIMEOUT", 5*time.Second); err != nil {
		return config, err
	}
	config.NATSURL = envString("DEFERMQ_NATS_URL", "nats://127.0.0.1:4222")
	config.NATSUser = os.Getenv("DEFERMQ_NATS_USER")
	config.NATSPassword = os.Getenv("DEFERMQ_NATS_PASSWORD")
	config.NATSCredsFile = os.Getenv("DEFERMQ_NATS_CREDS_FILE")
	config.NATSTLSCAFile = os.Getenv("DEFERMQ_NATS_TLS_CA_FILE")
	config.NATSTLSCertFile = os.Getenv("DEFERMQ_NATS_TLS_CERT_FILE")
	config.NATSTLSKeyFile = os.Getenv("DEFERMQ_NATS_TLS_KEY_FILE")
	config.NATSTLSServerName = os.Getenv("DEFERMQ_NATS_TLS_SERVER_NAME")
	config.NATSName = envString("DEFERMQ_NATS_NAME", "defermq-pusher-"+config.InstanceID)
	config.NATSStream = envString("DEFERMQ_NATS_STREAM", "DEFERMQ")
	config.SubjectsReady = envString("DEFERMQ_NATS_READY_SUBJECT_PREFIX", "defermq.ready")
	if config.NATSConnectTimeout, err = envDuration("DEFERMQ_NATS_CONNECT_TIMEOUT", 5*time.Second); err != nil {
		return config, err
	}
	if config.NATSReconnectWait, err = envDuration("DEFERMQ_NATS_RECONNECT_WAIT", time.Second); err != nil {
		return config, err
	}
	if config.NATSMaxReconnects, err = envInt("DEFERMQ_NATS_MAX_RECONNECTS", -1); err != nil {
		return config, err
	}
	if config.AckWait, err = envDuration("DEFERMQ_PUSHER_ACK_WAIT", 2*time.Minute); err != nil {
		return config, err
	}
	if config.MaxAckPending, err = envInt("DEFERMQ_PUSHER_MAX_ACK_PENDING", 5000); err != nil {
		return config, err
	}
	if config.MaxDeliver, err = envInt("DEFERMQ_PUSHER_MAX_DELIVER", 20); err != nil {
		return config, err
	}
	if config.FetchBatch, err = envInt("DEFERMQ_PUSHER_FETCH_BATCH_SIZE", 100); err != nil {
		return config, err
	}
	if config.FetchMaxWait, err = envDuration("DEFERMQ_PUSHER_FETCH_MAX_WAIT", 2*time.Second); err != nil {
		return config, err
	}
	if config.ClaimBatch, err = envInt("DEFERMQ_PUSHER_CLAIM_BATCH_SIZE", 100); err != nil {
		return config, err
	}
	if config.ClaimFlushInterval, err = envDuration("DEFERMQ_PUSHER_CLAIM_FLUSH_INTERVAL", 10*time.Millisecond); err != nil {
		return config, err
	}
	if config.Lease, err = envDuration("DEFERMQ_PUSHER_PROCESSING_LEASE", time.Minute); err != nil {
		return config, err
	}
	if config.Heartbeat, err = envDuration("DEFERMQ_PUSHER_LEASE_HEARTBEAT_INTERVAL", 20*time.Second); err != nil {
		return config, err
	}
	if config.ClockTolerance, err = envDuration("DEFERMQ_PUSHER_CLOCK_SKEW_TOLERANCE", 10*time.Millisecond); err != nil {
		return config, err
	}
	if config.MaxPayload, err = envInt64("DEFERMQ_MAX_PAYLOAD_BYTES", 1<<20); err != nil {
		return config, err
	}
	if config.HotHorizon, err = envDuration("DEFERMQ_HOT_HORIZON", 2*time.Minute); err != nil {
		return config, err
	}
	initial, err := envDuration("DEFERMQ_PUSHER_RETRY_INITIAL_BACKOFF", time.Second)
	if err != nil {
		return config, err
	}
	maximum, err := envDuration("DEFERMQ_PUSHER_RETRY_MAX_BACKOFF", 15*time.Minute)
	if err != nil {
		return config, err
	}
	multiplier, err := envFloat("DEFERMQ_PUSHER_RETRY_MULTIPLIER", 2)
	if err != nil {
		return config, err
	}
	config.Backoff = delivery.Backoff{
		Initial: initial, Multiplier: multiplier, Max: maximum,
		Jitter: delivery.Jitter(envString("DEFERMQ_PUSHER_RETRY_JITTER", string(delivery.JitterFull))),
	}
	config.Enabled, err = parseDestinations(envString("DEFERMQ_ENABLED_DESTINATIONS", "http"))
	if err != nil {
		return config, err
	}
	config.Workers = make(map[domain.DestinationType]int, 4)
	for _, item := range []struct {
		typ domain.DestinationType
		env string
		def int
	}{
		{domain.DestinationHTTP, "DEFERMQ_PUSHER_WORKERS_HTTP", 32},
		{domain.DestinationKafka, "DEFERMQ_PUSHER_WORKERS_KAFKA", 8},
		{domain.DestinationRabbit, "DEFERMQ_PUSHER_WORKERS_RABBIT", 8},
		{domain.DestinationPostgres, "DEFERMQ_PUSHER_WORKERS_POSTGRES", 16},
	} {
		config.Workers[item.typ], err = envInt(item.env, item.def)
		if err != nil {
			return config, err
		}
	}
	if err := loadAdapterConfig(&config); err != nil {
		return config, err
	}
	if config.Heartbeat >= config.Lease {
		return config, fmt.Errorf("heartbeat interval must be shorter than processing lease")
	}
	if config.ClaimBatch <= 0 {
		return config, fmt.Errorf("DEFERMQ_PUSHER_CLAIM_BATCH_SIZE must be positive")
	}
	return config, nil
}

func loadAdapterConfig(config *RuntimeConfig) error {
	var err error
	config.HTTP.Timeout, err = envDuration("DEFERMQ_HTTP_PUSH_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}
	config.HTTP.DialTimeout, err = envDuration("DEFERMQ_HTTP_DIAL_TIMEOUT", 3*time.Second)
	if err != nil {
		return err
	}
	config.HTTP.TLSHandshakeTimeout, err = envDuration("DEFERMQ_HTTP_TLS_HANDSHAKE_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	config.HTTP.ResponseHeaderTimeout, err = envDuration("DEFERMQ_HTTP_RESPONSE_HEADER_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	config.HTTP.IdleConnTimeout, err = envDuration("DEFERMQ_HTTP_IDLE_CONN_TIMEOUT", 90*time.Second)
	if err != nil {
		return err
	}
	config.HTTP.MaxIdleConns, err = envInt("DEFERMQ_HTTP_MAX_IDLE_CONNS", 200)
	if err != nil {
		return err
	}
	config.HTTP.MaxIdleConnsPerHost, err = envInt("DEFERMQ_HTTP_MAX_IDLE_CONNS_PER_HOST", 20)
	if err != nil {
		return err
	}
	config.HTTP.MaxResponseBodyBytes, err = envInt64("DEFERMQ_HTTP_MAX_RESPONSE_BODY_BYTES", 65536)
	if err != nil {
		return err
	}
	config.HTTP.AllowPrivateNetworks, err = envBool("DEFERMQ_HTTP_ALLOW_PRIVATE_NETWORKS", false)
	if err != nil {
		return err
	}
	config.HTTP.AllowedHosts = envList("DEFERMQ_HTTP_ALLOWED_HOSTS")

	config.Kafka.Brokers = envListDefault("DEFERMQ_KAFKA_BROKERS", []string{"127.0.0.1:9092"})
	config.Kafka.ClientID = envString("DEFERMQ_KAFKA_CLIENT_ID", "defermq")
	config.Kafka.AllowedTopics = envList("DEFERMQ_KAFKA_ALLOWED_TOPICS")
	config.Kafka.DialTimeout, err = envDuration("DEFERMQ_KAFKA_DIAL_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	config.Kafka.RequestTimeout, err = envDuration("DEFERMQ_KAFKA_REQUEST_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}
	config.Kafka.TLSEnabled, err = envBool("DEFERMQ_KAFKA_TLS_ENABLED", false)
	if err != nil {
		return err
	}
	config.Kafka.TLSCAFile = os.Getenv("DEFERMQ_KAFKA_TLS_CA_FILE")
	config.Kafka.TLSCertFile = os.Getenv("DEFERMQ_KAFKA_TLS_CERT_FILE")
	config.Kafka.TLSKeyFile = os.Getenv("DEFERMQ_KAFKA_TLS_KEY_FILE")
	config.Kafka.SASLMechanism = os.Getenv("DEFERMQ_KAFKA_SASL_MECHANISM")
	config.Kafka.SASLUsername = os.Getenv("DEFERMQ_KAFKA_SASL_USERNAME")
	config.Kafka.SASLPassword = os.Getenv("DEFERMQ_KAFKA_SASL_PASSWORD")

	config.Rabbit.URL = os.Getenv("DEFERMQ_RABBIT_URL")
	config.Rabbit.AllowedExchanges = envList("DEFERMQ_RABBIT_ALLOWED_EXCHANGES")
	config.Rabbit.ConnectTimeout, err = envDuration("DEFERMQ_RABBIT_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	config.Rabbit.PublishTimeout, err = envDuration("DEFERMQ_RABBIT_PUBLISH_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}
	config.Rabbit.ReconnectInitial, err = envDuration("DEFERMQ_RABBIT_RECONNECT_INITIAL", 500*time.Millisecond)
	if err != nil {
		return err
	}
	config.Rabbit.ReconnectMax, err = envDuration("DEFERMQ_RABBIT_RECONNECT_MAX", 30*time.Second)
	if err != nil {
		return err
	}
	config.Rabbit.Mandatory, err = envBool("DEFERMQ_RABBIT_MANDATORY", false)
	if err != nil {
		return err
	}

	config.TargetPostgres.DSN = os.Getenv("DEFERMQ_TARGET_POSTGRES_DSN")
	config.TargetPostgres.Table = envString("DEFERMQ_TARGET_POSTGRES_TABLE", "defermq_messages")
	config.TargetPostgres.AutoCreateTable, err = envBool("DEFERMQ_TARGET_POSTGRES_AUTO_CREATE_TABLE", false)
	if err != nil {
		return err
	}
	config.TargetPostgres.MaxConns, err = envInt32("DEFERMQ_TARGET_POSTGRES_MAX_CONNS", 10)
	if err != nil {
		return err
	}
	config.TargetPostgres.QueryTimeout, err = envDuration("DEFERMQ_TARGET_POSTGRES_QUERY_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	for _, item := range []struct {
		typ domain.DestinationType
		env string
	}{
		{domain.DestinationKafka, "DEFERMQ_KAFKA_ENABLED"},
		{domain.DestinationRabbit, "DEFERMQ_RABBIT_ENABLED"},
		{domain.DestinationPostgres, "DEFERMQ_TARGET_POSTGRES_ENABLED"},
	} {
		enabled, parseErr := envBool(item.env, false)
		if parseErr != nil {
			return parseErr
		}
		if containsDestination(config.Enabled, item.typ) && !enabled {
			return fmt.Errorf("%s must be true when %q is enabled", item.env, item.typ)
		}
	}
	return nil
}

func containsDestination(values []domain.DestinationType, expected domain.DestinationType) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func parseDestinations(value string) ([]domain.DestinationType, error) {
	seen := map[domain.DestinationType]bool{}
	var result []domain.DestinationType
	for _, raw := range strings.Split(value, ",") {
		typ := domain.DestinationType(strings.TrimSpace(raw))
		switch typ {
		case domain.DestinationHTTP, domain.DestinationKafka, domain.DestinationRabbit, domain.DestinationPostgres:
		default:
			return nil, fmt.Errorf("unknown enabled destination %q", typ)
		}
		if !seen[typ] {
			seen[typ] = true
			result = append(result, typ)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one destination must be enabled")
	}
	return result, nil
}

func defaultInstanceID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return parsed, nil
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return parsed, nil
}

func envInt32(name string, fallback int32) (int32, error) {
	value, err := envInt64(name, int64(fallback))
	return int32(value), err
}

func envInt64(name string, fallback int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return parsed, nil
}

func envFloat(name string, fallback float64) (float64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number", name)
	}
	return parsed, nil
}

func envBool(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return parsed, nil
}

func envList(name string) []string {
	return envListDefault(name, nil)
}

func envListDefault(name string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}
