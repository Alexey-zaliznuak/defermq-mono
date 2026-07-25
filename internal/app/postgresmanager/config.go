package postgresmanager

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/hotstorage/valkey"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/defermq/defermq/internal/manager"
)

type Config struct {
	InstanceID                string
	PostgresDSN               string
	PostgresMaxConn           int32
	PostgresConnectTimeout    time.Duration
	PostgresQueryTimeout      time.Duration
	HTTPAddr                  string
	ShutdownTimeout           time.Duration
	NATS                      natsjs.ConnectionConfig
	Valkey                    valkey.ConnectionConfig
	ValkeyIndex               valkey.Config
	Stream                    natsjs.StreamConfig
	IngestStream              ingest.StreamConfig
	IngestWriter              ingest.WriterConfig
	DeleteLegacyIngestDurable bool
	Promoter                  manager.PromoterConfig
	OutboxWorkers             int
	Outbox                    manager.OutboxWorkerConfig
	OutboxInitial             time.Duration
	OutboxMax                 time.Duration
	RegistrarWorkers          int
	Registrar                 manager.RegistrarConfig
	SchedulerWorkers          int
	Scheduler                 manager.SchedulerConfig
	Repairer                  manager.RepairerConfig
	Overdue                   manager.OverdueConfig
	Reaper                    manager.ProcessingReaperConfig
	Retention                 manager.RetentionConfig
	MetricsCollectionInterval time.Duration
	LoopHealthStartupGrace    time.Duration
	LoopHealthMaxStaleness    time.Duration
}

func LoadConfig() (cfg Config, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			cfg = Config{}
			err = fmt.Errorf("parse environment: %v", recovered)
		}
	}()
	subjects := natsjs.Subjects{
		SchedulePrefix: envString("DEFERMQ_NATS_SCHEDULE_SUBJECT_PREFIX", "defermq.schedule"),
		ReadyPrefix:    envString("DEFERMQ_NATS_READY_SUBJECT_PREFIX", "defermq.ready"),
	}
	cfg = Config{
		InstanceID:             envString("DEFERMQ_INSTANCE_ID", defaultInstanceID()),
		PostgresDSN:            os.Getenv("DEFERMQ_POSTGRES_DSN"),
		PostgresMaxConn:        int32(envInt("DEFERMQ_POSTGRES_MAX_CONNS", 30)),
		PostgresConnectTimeout: envDuration("DEFERMQ_POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
		PostgresQueryTimeout:   envDuration("DEFERMQ_POSTGRES_QUERY_TIMEOUT", 5*time.Second),
		HTTPAddr:               envString("DEFERMQ_MANAGER_HTTP_ADDR", ":8081"),
		ShutdownTimeout:        envDuration("DEFERMQ_SHUTDOWN_TIMEOUT", 20*time.Second),
		NATS: natsjs.ConnectionConfig{
			URL:            envString("DEFERMQ_NATS_URL", "nats://127.0.0.1:4222"),
			Name:           envString("DEFERMQ_NATS_NAME", "defermq-postgres-manager"),
			User:           os.Getenv("DEFERMQ_NATS_USER"),
			Password:       os.Getenv("DEFERMQ_NATS_PASSWORD"),
			CredsFile:      os.Getenv("DEFERMQ_NATS_CREDS_FILE"),
			TLSCAFile:      os.Getenv("DEFERMQ_NATS_TLS_CA_FILE"),
			TLSCertFile:    os.Getenv("DEFERMQ_NATS_TLS_CERT_FILE"),
			TLSKeyFile:     os.Getenv("DEFERMQ_NATS_TLS_KEY_FILE"),
			TLSServerName:  os.Getenv("DEFERMQ_NATS_TLS_SERVER_NAME"),
			ConnectTimeout: envDuration("DEFERMQ_NATS_CONNECT_TIMEOUT", 5*time.Second),
			ReconnectWait:  envDuration("DEFERMQ_NATS_RECONNECT_WAIT", time.Second),
			MaxReconnects:  envInt("DEFERMQ_NATS_MAX_RECONNECTS", -1),
		},
		Valkey: valkey.ConnectionConfig{
			URL:          envString("DEFERMQ_VALKEY_URL", "redis://127.0.0.1:6379/0"),
			ClientName:   envString("DEFERMQ_VALKEY_CLIENT_NAME", "defermq-postgres-manager"),
			PoolSize:     envInt("DEFERMQ_VALKEY_POOL_SIZE", 32),
			DialTimeout:  envDuration("DEFERMQ_VALKEY_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  envDuration("DEFERMQ_VALKEY_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: envDuration("DEFERMQ_VALKEY_WRITE_TIMEOUT", 3*time.Second),
		},
		ValkeyIndex: valkey.Config{
			Prefix:  envString("DEFERMQ_VALKEY_PREFIX", "defermq:hot"),
			Buckets: envInt("DEFERMQ_VALKEY_BUCKETS", 32),
		},
		Stream: natsjs.StreamConfig{
			Name:            envString("DEFERMQ_NATS_STREAM", "DEFERMQ"),
			Subjects:        subjects,
			Replicas:        envInt("DEFERMQ_NATS_STREAM_REPLICAS", 1),
			MaxAge:          envDuration("DEFERMQ_NATS_STREAM_MAX_AGE", 24*time.Hour),
			MaxBytes:        envInt64("DEFERMQ_NATS_STREAM_MAX_BYTES", 1<<30),
			MaxMsgSize:      int32(envInt("DEFERMQ_NATS_STREAM_MAX_MSG_SIZE", 64<<10)),
			DuplicateWindow: envDuration("DEFERMQ_NATS_DUPLICATE_WINDOW", 10*time.Minute),
		},
		IngestStream: ingest.StreamConfig{
			Name:       envString("DEFERMQ_NATS_INGEST_STREAM", "DEFERMQ_INGEST_V2"),
			Subject:    envString("DEFERMQ_NATS_INGEST_SUBJECT", "defermq.ingest.v2.commands"),
			Replicas:   envInt("DEFERMQ_NATS_STREAM_REPLICAS", 1),
			MaxAge:     envDuration("DEFERMQ_NATS_STREAM_MAX_AGE", 24*time.Hour),
			MaxBytes:   envInt64("DEFERMQ_NATS_STREAM_MAX_BYTES", 1<<30),
			MaxMsgSize: int32(envInt("DEFERMQ_NATS_INGEST_MAX_MSG_SIZE", 3<<20)),
			Duplicates: envDuration("DEFERMQ_NATS_DUPLICATE_WINDOW", 10*time.Minute),
		},
		IngestWriter: ingest.WriterConfig{
			Stream:        envString("DEFERMQ_NATS_INGEST_STREAM", "DEFERMQ_INGEST_V2"),
			Subject:       envString("DEFERMQ_NATS_INGEST_SUBJECT", "defermq.ingest.v2.commands"),
			Durable:       envString("DEFERMQ_MANAGER_INGEST_DURABLE", "defermq-ingest-writer"),
			ShardCount:    envInt("DEFERMQ_MANAGER_INGEST_SHARDS", ingest.DefaultShardCount),
			WorkerCount:   envInt("DEFERMQ_MANAGER_INGEST_WORKERS", 8),
			BatchSize:     envInt("DEFERMQ_MANAGER_INGEST_BATCH_SIZE", 500),
			FlushInterval: envDuration("DEFERMQ_MANAGER_INGEST_FLUSH_INTERVAL", 500*time.Millisecond),
			AckWait:       envDuration("DEFERMQ_MANAGER_INGEST_ACK_WAIT", 30*time.Second),
			MaxAckPending: envInt("DEFERMQ_MANAGER_INGEST_MAX_ACK_PENDING", 2000),
			MaxDeliver:    envInt("DEFERMQ_MANAGER_INGEST_MAX_DELIVER", 20),
		},
		DeleteLegacyIngestDurable: envBool("DEFERMQ_MANAGER_INGEST_DELETE_LEGACY_DURABLE", false),
		Promoter: manager.PromoterConfig{
			HotHorizon:   envDuration("DEFERMQ_HOT_HORIZON", 10*time.Second),
			BatchSize:    envInt("DEFERMQ_MANAGER_PROMOTER_BATCH_SIZE", 1000),
			PollInterval: envDuration("DEFERMQ_MANAGER_PROMOTER_POLL_INTERVAL", 500*time.Millisecond),
			ErrorBackoff: envDuration("DEFERMQ_MANAGER_PROMOTER_ERROR_BACKOFF", time.Second),
		},
		OutboxWorkers: envInt("DEFERMQ_MANAGER_OUTBOX_WORKERS", 4),
		Outbox: manager.OutboxWorkerConfig{
			BatchSize:    envInt("DEFERMQ_MANAGER_OUTBOX_BATCH_SIZE", 200),
			PollInterval: envDuration("DEFERMQ_MANAGER_OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
			LockTTL:      envDuration("DEFERMQ_MANAGER_OUTBOX_LOCK_TTL", 30*time.Second),
		},
		OutboxInitial:    envDuration("DEFERMQ_MANAGER_OUTBOX_RETRY_INITIAL", 250*time.Millisecond),
		OutboxMax:        envDuration("DEFERMQ_MANAGER_OUTBOX_RETRY_MAX", 30*time.Second),
		RegistrarWorkers: envInt("DEFERMQ_MANAGER_REGISTRAR_WORKERS", 6),
		Registrar: manager.RegistrarConfig{
			BatchSize:    envInt("DEFERMQ_MANAGER_REGISTRAR_BATCH_SIZE", 200),
			PollInterval: envDuration("DEFERMQ_MANAGER_REGISTRAR_POLL_INTERVAL", 100*time.Millisecond),
			LockTTL:      envDuration("DEFERMQ_MANAGER_REGISTRAR_LOCK_TTL", 30*time.Second),
		},
		SchedulerWorkers: envInt("DEFERMQ_MANAGER_SCHEDULER_WORKERS", 10),
		Scheduler: manager.SchedulerConfig{
			PollInterval:   envDuration("DEFERMQ_MANAGER_SCHEDULER_POLL_INTERVAL", 25*time.Millisecond),
			LeaseTTL:       envDuration("DEFERMQ_MANAGER_SCHEDULER_LEASE_TTL", 30*time.Second),
			InflightTTL:    envDuration("DEFERMQ_MANAGER_SCHEDULER_INFLIGHT_TTL", 10*time.Second),
			PublishTimeout: envDuration("DEFERMQ_MANAGER_SCHEDULER_PUBLISH_TIMEOUT", 5*time.Second),
			EarlyWindow:    envDuration("DEFERMQ_MANAGER_SCHEDULER_EARLY_WINDOW", 50*time.Millisecond),
			BatchSize:      envInt("DEFERMQ_MANAGER_SCHEDULER_BATCH_SIZE", 200),
			ReclaimBatch:   envInt("DEFERMQ_MANAGER_SCHEDULER_RECLAIM_BATCH_SIZE", 200),
			ErrorBackoff:   envDuration("DEFERMQ_MANAGER_SCHEDULER_ERROR_BACKOFF", 250*time.Millisecond),
		},
		Repairer: manager.RepairerConfig{
			HotHorizon: envDuration("DEFERMQ_HOT_HORIZON", 10*time.Second),
			Interval:   envDuration("DEFERMQ_MANAGER_REPAIR_INTERVAL", 5*time.Second),
			BatchSize:  envInt("DEFERMQ_MANAGER_REPAIR_BATCH_SIZE", 1000),
		},
		Overdue: manager.OverdueConfig{
			Interval:  envDuration("DEFERMQ_MANAGER_OVERDUE_INTERVAL", time.Second),
			Grace:     envDuration("DEFERMQ_MANAGER_OVERDUE_GRACE", 15*time.Second),
			BatchSize: envInt("DEFERMQ_MANAGER_OVERDUE_BATCH_SIZE", 1000),
		},
		Reaper: manager.ProcessingReaperConfig{
			Interval:      envDuration("DEFERMQ_MANAGER_REAPER_INTERVAL", 5*time.Second),
			RecoveryDelay: envDuration("DEFERMQ_MANAGER_RECOVERY_DELAY", 0),
			BatchSize:     envInt("DEFERMQ_MANAGER_OVERDUE_BATCH_SIZE", 1000),
		},
		Retention: manager.RetentionConfig{
			Interval:          envDuration("DEFERMQ_MANAGER_RETENTION_INTERVAL", 10*time.Minute),
			TerminalRetention: envDuration("DEFERMQ_MANAGER_TERMINAL_RETENTION", 168*time.Hour),
			OutboxRetention:   envDuration("DEFERMQ_MANAGER_OUTBOX_RETENTION", 24*time.Hour),
			BatchSize:         envInt("DEFERMQ_MANAGER_RETENTION_BATCH_SIZE", 1000),
		},
		MetricsCollectionInterval: envDuration("DEFERMQ_MANAGER_METRICS_COLLECTION_INTERVAL", 5*time.Second),
		LoopHealthStartupGrace:    envDuration("DEFERMQ_MANAGER_LOOP_HEALTH_STARTUP_GRACE", 30*time.Second),
		LoopHealthMaxStaleness:    envDuration("DEFERMQ_MANAGER_LOOP_HEALTH_MAX_STALENESS", 20*time.Second),
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.PostgresDSN == "" {
		return errors.New("DEFERMQ_POSTGRES_DSN is required")
	}
	if c.InstanceID == "" || c.PostgresMaxConn <= 0 || c.PostgresConnectTimeout <= 0 ||
		c.PostgresQueryTimeout <= 0 || c.HTTPAddr == "" || c.ShutdownTimeout <= 0 ||
		c.OutboxWorkers <= 0 || c.OutboxInitial <= 0 || c.OutboxMax < c.OutboxInitial ||
		c.RegistrarWorkers <= 0 || c.SchedulerWorkers <= 0 ||
		c.SchedulerWorkers > c.ValkeyIndex.Buckets ||
		c.Scheduler.PollInterval <= 0 || c.Scheduler.LeaseTTL <= 0 ||
		c.Scheduler.InflightTTL <= 0 || c.Scheduler.PublishTimeout <= 0 ||
		c.Scheduler.InflightTTL <= c.Scheduler.PublishTimeout ||
		c.Overdue.Grace <= c.Scheduler.InflightTTL || c.Scheduler.EarlyWindow < 0 ||
		c.Scheduler.BatchSize <= 0 || c.Scheduler.ReclaimBatch <= 0 ||
		c.Scheduler.ErrorBackoff <= 0 || c.Repairer.HotHorizon <= 0 ||
		c.Repairer.Interval <= 0 || c.Repairer.BatchSize <= 0 ||
		c.MetricsCollectionInterval <= 0 || c.LoopHealthStartupGrace < 0 ||
		c.LoopHealthMaxStaleness <= 0 {
		return errors.New("invalid manager configuration")
	}
	if err := c.NATS.Validate(); err != nil {
		return err
	}
	if err := c.Valkey.Validate(); err != nil {
		return err
	}
	if err := c.ValkeyIndex.Validate(); err != nil {
		return err
	}
	if c.Stream.Name == "" || c.Stream.Replicas <= 0 || c.Stream.MaxAge <= 0 || c.Stream.MaxBytes <= 0 ||
		c.Stream.MaxMsgSize <= 0 || c.Stream.DuplicateWindow <= 0 {
		return errors.New("invalid JetStream configuration")
	}
	if err := c.IngestStream.Validate(); err != nil {
		return err
	}
	if c.IngestWriter.Stream == "" || c.IngestWriter.Subject == "" || c.IngestWriter.Durable == "" ||
		c.IngestWriter.ShardCount <= 0 || c.IngestWriter.WorkerCount <= 0 ||
		c.IngestWriter.WorkerCount > c.IngestWriter.ShardCount ||
		c.IngestWriter.BatchSize <= 0 || c.IngestWriter.FlushInterval <= 0 ||
		c.IngestWriter.AckWait <= 0 || c.IngestWriter.MaxAckPending < c.IngestWriter.BatchSize ||
		c.IngestWriter.MaxDeliver <= 0 {
		return errors.New("invalid ingest writer configuration")
	}
	return nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		panic(fmt.Sprintf("%s must be an integer: %v", name, err))
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("%s must be an integer: %v", name, err))
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		panic(fmt.Sprintf("%s must be a boolean: %v", name, err))
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Sprintf("%s must be a Go duration: %v", name, err))
	}
	return parsed
}

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}
