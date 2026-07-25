package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (c Config) Validate() error {
	var errs []error
	if c.Service != ServiceGateway && c.Service != ServiceManager && c.Service != ServicePusher {
		errs = append(errs, fmt.Errorf("unknown service %q", c.Service))
	}
	if strings.TrimSpace(c.Common.ServiceName) == "" {
		errs = append(errs, errors.New("DEFERMQ_SERVICE_NAME must not be empty"))
	}
	if strings.TrimSpace(c.Common.InstanceID) == "" {
		errs = append(errs, errors.New("DEFERMQ_INSTANCE_ID must not be empty"))
	}
	if !oneOf(c.Common.Log.Format, "json", "console") {
		errs = append(errs, errors.New("DEFERMQ_LOG_FORMAT must be json or console"))
	}
	if !oneOf(c.Common.Log.Level, "debug", "info", "warn", "error", "dpanic", "panic", "fatal") {
		errs = append(errs, errors.New("DEFERMQ_LOG_LEVEL is invalid"))
	}
	if !oneOf(c.Common.Log.StacktraceLevel, "debug", "info", "warn", "error", "dpanic", "panic", "fatal") {
		errs = append(errs, errors.New("DEFERMQ_LOG_STACKTRACE_LEVEL is invalid"))
	}
	positiveDuration(&errs, "DEFERMQ_SHUTDOWN_TIMEOUT", c.Common.ShutdownTimeout)
	positiveDuration(&errs, "DEFERMQ_HOT_HORIZON", c.Common.HotHorizon)
	if c.Common.MaxPayloadBytes <= 0 {
		errs = append(errs, errors.New("DEFERMQ_MAX_PAYLOAD_BYTES must be positive"))
	}
	if len(c.Common.EnabledDestinations) == 0 {
		errs = append(errs, errors.New("DEFERMQ_ENABLED_DESTINATIONS must not be empty"))
	}
	for _, destination := range c.Common.EnabledDestinations {
		if !oneOf(destination, "http", "kafka", "rabbit", "postgres") {
			errs = append(errs, fmt.Errorf("DEFERMQ_ENABLED_DESTINATIONS contains unknown value %q", destination))
		}
	}

	if strings.TrimSpace(c.Postgres.DSN) == "" {
		errs = append(errs, errors.New("DEFERMQ_POSTGRES_DSN is required"))
	} else if err := validateURL(c.Postgres.DSN, "postgres", "postgresql"); err != nil {
		errs = append(errs, fmt.Errorf("DEFERMQ_POSTGRES_DSN: %w", err))
	}
	if c.Postgres.MaxConns <= 0 || c.Postgres.MinConns < 0 || c.Postgres.MinConns > c.Postgres.MaxConns {
		errs = append(errs, errors.New("PostgreSQL connection bounds are invalid"))
	}
	positiveDuration(&errs, "DEFERMQ_POSTGRES_MAX_CONN_LIFETIME", c.Postgres.MaxConnLifetime)
	positiveDuration(&errs, "DEFERMQ_POSTGRES_MAX_CONN_IDLE_TIME", c.Postgres.MaxConnIdleTime)
	positiveDuration(&errs, "DEFERMQ_POSTGRES_HEALTH_CHECK_PERIOD", c.Postgres.HealthCheckPeriod)
	positiveDuration(&errs, "DEFERMQ_POSTGRES_CONNECT_TIMEOUT", c.Postgres.ConnectTimeout)
	positiveDuration(&errs, "DEFERMQ_POSTGRES_QUERY_TIMEOUT", c.Postgres.QueryTimeout)
	positiveDuration(&errs, "DEFERMQ_POSTGRES_METRICS_QUERY_TIMEOUT", c.Postgres.MetricsQueryTimeout)

	if c.Service == ServiceGateway || c.Service == ServiceManager || c.Service == ServicePusher {
		if err := validateURL(c.NATS.URL, "nats", "tls", "ws", "wss"); err != nil {
			errs = append(errs, fmt.Errorf("DEFERMQ_NATS_URL: %w", err))
		}
	}
	positiveDuration(&errs, "DEFERMQ_NATS_CONNECT_TIMEOUT", c.NATS.ConnectTimeout)
	positiveDuration(&errs, "DEFERMQ_NATS_RECONNECT_WAIT", c.NATS.ReconnectWait)
	positiveDuration(&errs, "DEFERMQ_NATS_PUBLISH_TIMEOUT", c.NATS.PublishTimeout)
	positiveDuration(&errs, "DEFERMQ_NATS_STREAM_MAX_AGE", c.NATS.StreamMaxAge)
	positiveDuration(&errs, "DEFERMQ_NATS_DUPLICATE_WINDOW", c.NATS.DuplicateWindow)
	if c.NATS.MaxReconnects < -1 || c.NATS.StreamReplicas <= 0 || c.NATS.StreamMaxBytes <= 0 || c.NATS.StreamMaxMessageSize <= 0 {
		errs = append(errs, errors.New("NATS numeric limits are invalid"))
	}
	if strings.TrimSpace(c.NATS.Stream) == "" || strings.TrimSpace(c.NATS.IngestStream) == "" ||
		strings.TrimSpace(c.NATS.IngestPendingBucket) == "" ||
		!validSubjectPrefix(c.NATS.ScheduleSubjectPrefix) || !validSubjectPrefix(c.NATS.ReadySubjectPrefix) ||
		!validSubjectPrefix(c.NATS.IngestSubject) {
		errs = append(errs, errors.New("NATS stream and subject prefixes must be non-empty and contain no wildcards"))
	}
	if c.NATS.CredentialsFile != "" && (c.NATS.User != "" || c.NATS.Password != "") {
		errs = append(errs, errors.New("DEFERMQ_NATS_CREDS_FILE cannot be combined with NATS user/password"))
	}
	if (c.NATS.User == "") != (c.NATS.Password == "") {
		errs = append(errs, errors.New("DEFERMQ_NATS_USER and DEFERMQ_NATS_PASSWORD must be set together"))
	}
	validatePair(&errs, "DEFERMQ_NATS_TLS_CERT_FILE", c.NATS.TLSCertFile, "DEFERMQ_NATS_TLS_KEY_FILE", c.NATS.TLSKeyFile)

	validateGateway(&errs, c.Gateway)
	validateManager(&errs, c.Manager)
	validatePusher(&errs, c.Pusher)
	validateHTTPAdapter(&errs, c.HTTPAdapter)
	validateKafka(&errs, c.Kafka)
	validateRabbit(&errs, c.Rabbit)
	validateTargetPostgres(&errs, c.TargetPostgres)

	return errors.Join(errs...)
}

func validateGateway(errs *[]error, c GatewayConfig) {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		*errs = append(*errs, errors.New("DEFERMQ_GATEWAY_HTTP_ADDR must not be empty"))
	}
	for name, value := range map[string]time.Duration{
		"DEFERMQ_GATEWAY_READ_TIMEOUT": c.ReadTimeout, "DEFERMQ_GATEWAY_READ_HEADER_TIMEOUT": c.ReadHeaderTimeout,
		"DEFERMQ_GATEWAY_WRITE_TIMEOUT": c.WriteTimeout, "DEFERMQ_GATEWAY_IDLE_TIMEOUT": c.IdleTimeout,
		"DEFERMQ_GATEWAY_REQUEST_TIMEOUT": c.RequestTimeout,
	} {
		positiveDuration(errs, name, value)
	}
	if c.MaxHeaderBytes <= 0 || c.DefaultMaxAttempts <= 0 || c.MaxIdempotencyKeyBytes <= 0 ||
		c.IngestBatchSize <= 0 || c.IngestQueueCapacity < c.IngestBatchSize || c.IngestShardCount <= 0 {
		*errs = append(*errs, errors.New("Gateway integer limits must be positive"))
	}
	positiveDuration(errs, "DEFERMQ_GATEWAY_INGEST_FLUSH_INTERVAL", c.IngestFlushInterval)
}

func validateManager(errs *[]error, c Manager) {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		*errs = append(*errs, errors.New("DEFERMQ_MANAGER_HTTP_ADDR must not be empty"))
	}
	for name, value := range map[string]int{
		"DEFERMQ_MANAGER_PROMOTER_BATCH_SIZE": c.PromoterBatchSize, "DEFERMQ_MANAGER_OUTBOX_WORKERS": c.OutboxWorkers,
		"DEFERMQ_MANAGER_OUTBOX_BATCH_SIZE": c.OutboxBatchSize, "DEFERMQ_MANAGER_OVERDUE_BATCH_SIZE": c.OverdueBatchSize,
		"DEFERMQ_MANAGER_RETENTION_BATCH_SIZE":   c.RetentionBatchSize,
		"DEFERMQ_MANAGER_INGEST_WORKERS":         c.IngestWorkers,
		"DEFERMQ_MANAGER_INGEST_SHARDS":          c.IngestShards,
		"DEFERMQ_MANAGER_INGEST_BATCH_SIZE":      c.IngestBatchSize,
		"DEFERMQ_MANAGER_INGEST_MAX_ACK_PENDING": c.IngestMaxAckPending,
		"DEFERMQ_MANAGER_INGEST_MAX_DELIVER":     c.IngestMaxDeliver,
	} {
		if value <= 0 {
			*errs = append(*errs, fmt.Errorf("%s must be positive", name))
		}
	}
	for name, value := range map[string]time.Duration{
		"DEFERMQ_MANAGER_PROMOTER_POLL_INTERVAL": c.PromoterPollInterval, "DEFERMQ_MANAGER_PROMOTER_ERROR_BACKOFF": c.PromoterErrorBackoff,
		"DEFERMQ_MANAGER_OUTBOX_POLL_INTERVAL": c.OutboxPollInterval, "DEFERMQ_MANAGER_OUTBOX_LOCK_TTL": c.OutboxLockTTL,
		"DEFERMQ_MANAGER_OUTBOX_RETRY_INITIAL": c.OutboxRetryInitial, "DEFERMQ_MANAGER_OUTBOX_RETRY_MAX": c.OutboxRetryMax,
		"DEFERMQ_MANAGER_OVERDUE_INTERVAL": c.OverdueInterval, "DEFERMQ_MANAGER_REAPER_INTERVAL": c.ReaperInterval,
		"DEFERMQ_MANAGER_RETENTION_INTERVAL": c.RetentionInterval, "DEFERMQ_MANAGER_TERMINAL_RETENTION": c.TerminalRetention,
		"DEFERMQ_MANAGER_OUTBOX_RETENTION": c.OutboxRetention, "DEFERMQ_MANAGER_METRICS_COLLECTION_INTERVAL": c.MetricsCollectionInterval,
		"DEFERMQ_MANAGER_INGEST_FLUSH_INTERVAL": c.IngestFlushInterval, "DEFERMQ_MANAGER_INGEST_ACK_WAIT": c.IngestAckWait,
	} {
		positiveDuration(errs, name, value)
	}
	nonNegativeDuration(errs, "DEFERMQ_MANAGER_OVERDUE_GRACE", c.OverdueGrace)
	nonNegativeDuration(errs, "DEFERMQ_MANAGER_RECOVERY_DELAY", c.RecoveryDelay)
	if c.OutboxRetryInitial > c.OutboxRetryMax {
		*errs = append(*errs, errors.New("Manager outbox retry initial must not exceed retry max"))
	}
	if c.IngestMaxAckPending < c.IngestBatchSize {
		*errs = append(*errs, errors.New("Manager ingest max ack pending must cover one batch"))
	}
	if c.IngestWorkers > c.IngestShards {
		*errs = append(*errs, errors.New("Manager ingest workers must not exceed ingest shards"))
	}
}

func validatePusher(errs *[]error, c Pusher) {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		*errs = append(*errs, errors.New("DEFERMQ_PUSHER_HTTP_ADDR must not be empty"))
	}
	for name, value := range map[string]int{
		"DEFERMQ_PUSHER_FETCH_BATCH_SIZE": c.FetchBatchSize, "DEFERMQ_PUSHER_CLAIM_BATCH_SIZE": c.ClaimBatchSize,
		"DEFERMQ_PUSHER_WORKERS_HTTP":  c.WorkersHTTP,
		"DEFERMQ_PUSHER_WORKERS_KAFKA": c.WorkersKafka, "DEFERMQ_PUSHER_WORKERS_RABBIT": c.WorkersRabbit,
		"DEFERMQ_PUSHER_WORKERS_POSTGRES": c.WorkersPostgres, "DEFERMQ_PUSHER_MAX_ACK_PENDING": c.MaxAckPending,
		"DEFERMQ_PUSHER_MAX_DELIVER": c.MaxDeliver,
	} {
		if value <= 0 {
			*errs = append(*errs, fmt.Errorf("%s must be positive", name))
		}
	}
	for name, value := range map[string]time.Duration{
		"DEFERMQ_PUSHER_FETCH_MAX_WAIT": c.FetchMaxWait, "DEFERMQ_PUSHER_ACK_WAIT": c.AckWait,
		"DEFERMQ_PUSHER_CLAIM_FLUSH_INTERVAL": c.ClaimFlushInterval,
		"DEFERMQ_PUSHER_PROCESSING_LEASE":     c.ProcessingLease, "DEFERMQ_PUSHER_LEASE_HEARTBEAT_INTERVAL": c.LeaseHeartbeatInterval,
		"DEFERMQ_PUSHER_RETRY_INITIAL_BACKOFF": c.RetryInitialBackoff, "DEFERMQ_PUSHER_RETRY_MAX_BACKOFF": c.RetryMaxBackoff,
		"DEFERMQ_PUSHER_METRICS_COLLECTION_INTERVAL": c.MetricsCollectionInterval,
	} {
		positiveDuration(errs, name, value)
	}
	nonNegativeDuration(errs, "DEFERMQ_PUSHER_CLOCK_SKEW_TOLERANCE", c.ClockSkewTolerance)
	if c.LeaseHeartbeatInterval >= c.ProcessingLease {
		*errs = append(*errs, errors.New("Pusher lease heartbeat interval must be shorter than processing lease"))
	}
	if c.RetryInitialBackoff > c.RetryMaxBackoff || c.RetryMultiplier < 1 {
		*errs = append(*errs, errors.New("Pusher retry settings are invalid"))
	}
	if !oneOf(c.RetryJitter, "full", "equal", "none") {
		*errs = append(*errs, errors.New("DEFERMQ_PUSHER_RETRY_JITTER must be full, equal, or none"))
	}
}

func validateHTTPAdapter(errs *[]error, c HTTPAdapter) {
	for name, value := range map[string]time.Duration{
		"DEFERMQ_HTTP_PUSH_TIMEOUT": c.PushTimeout, "DEFERMQ_HTTP_DIAL_TIMEOUT": c.DialTimeout,
		"DEFERMQ_HTTP_TLS_HANDSHAKE_TIMEOUT": c.TLSHandshakeTimeout, "DEFERMQ_HTTP_RESPONSE_HEADER_TIMEOUT": c.ResponseHeaderTimeout,
		"DEFERMQ_HTTP_IDLE_CONN_TIMEOUT": c.IdleConnTimeout,
	} {
		positiveDuration(errs, name, value)
	}
	if c.MaxIdleConns <= 0 || c.MaxIdleConnsPerHost <= 0 || c.MaxResponseBodyBytes <= 0 {
		*errs = append(*errs, errors.New("HTTP adapter integer limits must be positive"))
	}
}

func validateKafka(errs *[]error, c Kafka) {
	positiveDuration(errs, "DEFERMQ_KAFKA_DIAL_TIMEOUT", c.DialTimeout)
	positiveDuration(errs, "DEFERMQ_KAFKA_REQUEST_TIMEOUT", c.RequestTimeout)
	if c.Enabled && (len(c.Brokers) == 0 || strings.TrimSpace(c.ClientID) == "") {
		*errs = append(*errs, errors.New("Enabled Kafka adapter requires brokers and client ID"))
	}
	validatePair(errs, "DEFERMQ_KAFKA_TLS_CERT_FILE", c.TLSCertFile, "DEFERMQ_KAFKA_TLS_KEY_FILE", c.TLSKeyFile)
	if c.TLSEnabled && c.TLSCertFile == "" && c.TLSKeyFile != "" {
		*errs = append(*errs, errors.New("Kafka TLS key requires a certificate"))
	}
	if c.SASLMechanism == "" && (c.SASLUsername != "" || c.SASLPassword != "") {
		*errs = append(*errs, errors.New("Kafka SASL credentials require DEFERMQ_KAFKA_SASL_MECHANISM"))
	}
	if c.SASLMechanism != "" && (c.SASLUsername == "" || c.SASLPassword == "") {
		*errs = append(*errs, errors.New("Kafka SASL mechanism requires username and password"))
	}
}

func validateRabbit(errs *[]error, c Rabbit) {
	for name, value := range map[string]time.Duration{
		"DEFERMQ_RABBIT_CONNECT_TIMEOUT": c.ConnectTimeout, "DEFERMQ_RABBIT_PUBLISH_TIMEOUT": c.PublishTimeout,
		"DEFERMQ_RABBIT_RECONNECT_INITIAL": c.ReconnectInitial, "DEFERMQ_RABBIT_RECONNECT_MAX": c.ReconnectMax,
	} {
		positiveDuration(errs, name, value)
	}
	if c.ReconnectInitial > c.ReconnectMax {
		*errs = append(*errs, errors.New("Rabbit reconnect initial must not exceed reconnect max"))
	}
	if c.Enabled {
		if c.URL == "" {
			*errs = append(*errs, errors.New("DEFERMQ_RABBIT_URL is required when Rabbit is enabled"))
		} else if err := validateURL(c.URL, "amqp", "amqps"); err != nil {
			*errs = append(*errs, fmt.Errorf("DEFERMQ_RABBIT_URL: %w", err))
		}
	}
}

func validateTargetPostgres(errs *[]error, c TargetPostgres) {
	positiveDuration(errs, "DEFERMQ_TARGET_POSTGRES_QUERY_TIMEOUT", c.QueryTimeout)
	if c.MaxConns <= 0 {
		*errs = append(*errs, errors.New("DEFERMQ_TARGET_POSTGRES_MAX_CONNS must be positive"))
	}
	if !identifierPattern.MatchString(c.Table) {
		*errs = append(*errs, errors.New("DEFERMQ_TARGET_POSTGRES_TABLE must be a simple SQL identifier"))
	}
	if c.Enabled {
		if c.DSN == "" {
			*errs = append(*errs, errors.New("DEFERMQ_TARGET_POSTGRES_DSN is required when target PostgreSQL is enabled"))
		} else if err := validateURL(c.DSN, "postgres", "postgresql"); err != nil {
			*errs = append(*errs, fmt.Errorf("DEFERMQ_TARGET_POSTGRES_DSN: %w", err))
		}
	}
}

func validateURL(value string, schemes ...string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || !oneOf(parsed.Scheme, schemes...) {
		return errors.New("invalid URL or scheme")
	}
	return nil
}

func validatePair(errs *[]error, firstName, first, secondName, second string) {
	if (first == "") != (second == "") {
		*errs = append(*errs, fmt.Errorf("%s and %s must be set together", firstName, secondName))
	}
}

func positiveDuration(errs *[]error, name string, value time.Duration) {
	if value <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be positive", name))
	}
}

func nonNegativeDuration(errs *[]error, name string, value time.Duration) {
	if value < 0 {
		*errs = append(*errs, fmt.Errorf("%s must not be negative", name))
	}
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validSubjectPrefix(value string) bool {
	value = strings.Trim(value, ".")
	return value != "" && !strings.ContainsAny(value, "*> \t\r\n") && !strings.Contains(value, "..")
}
