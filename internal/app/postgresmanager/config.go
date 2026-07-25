package postgresmanager

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/manager"
)

type Config struct {
	InstanceID             string
	PostgresDSN            string
	PostgresMaxConn        int32
	PostgresConnectTimeout time.Duration
	PostgresQueryTimeout   time.Duration
	HTTPAddr               string
	ShutdownTimeout        time.Duration
	NATS                   natsjs.ConnectionConfig
	Stream                 natsjs.StreamConfig
	Promoter               manager.PromoterConfig
	OutboxWorkers          int
	Outbox                 manager.OutboxWorkerConfig
	OutboxInitial          time.Duration
	OutboxMax              time.Duration
	Overdue                manager.OverdueConfig
	Reaper                 manager.ProcessingReaperConfig
	Retention              manager.RetentionConfig
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
		Stream: natsjs.StreamConfig{
			Name:            envString("DEFERMQ_NATS_STREAM", "DEFERMQ"),
			Subjects:        subjects,
			Replicas:        envInt("DEFERMQ_NATS_STREAM_REPLICAS", 1),
			MaxAge:          envDuration("DEFERMQ_NATS_STREAM_MAX_AGE", 24*time.Hour),
			MaxBytes:        envInt64("DEFERMQ_NATS_STREAM_MAX_BYTES", 1<<30),
			MaxMsgSize:      int32(envInt("DEFERMQ_NATS_STREAM_MAX_MSG_SIZE", 64<<10)),
			DuplicateWindow: envDuration("DEFERMQ_NATS_DUPLICATE_WINDOW", 10*time.Minute),
		},
		Promoter: manager.PromoterConfig{
			HotHorizon:   envDuration("DEFERMQ_HOT_HORIZON", 2*time.Minute),
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
		OutboxInitial: envDuration("DEFERMQ_MANAGER_OUTBOX_RETRY_INITIAL", 250*time.Millisecond),
		OutboxMax:     envDuration("DEFERMQ_MANAGER_OUTBOX_RETRY_MAX", 30*time.Second),
		Overdue: manager.OverdueConfig{
			Interval:  envDuration("DEFERMQ_MANAGER_OVERDUE_INTERVAL", time.Second),
			Grace:     envDuration("DEFERMQ_MANAGER_OVERDUE_GRACE", 2*time.Second),
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
		c.OutboxWorkers <= 0 || c.OutboxInitial <= 0 || c.OutboxMax < c.OutboxInitial {
		return errors.New("invalid manager configuration")
	}
	if err := c.NATS.Validate(); err != nil {
		return err
	}
	if c.Stream.Name == "" || c.Stream.Replicas <= 0 || c.Stream.MaxAge <= 0 || c.Stream.MaxBytes <= 0 ||
		c.Stream.MaxMsgSize <= 0 || c.Stream.DuplicateWindow <= 0 {
		return errors.New("invalid JetStream configuration")
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
