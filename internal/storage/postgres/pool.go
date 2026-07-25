package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultQueryTimeout = 5 * time.Second

type PoolConfig struct {
	DSN               string
	ApplicationName   string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
	QueryTimeout      time.Duration
	RuntimeParams     map[string]string
}

func (c PoolConfig) Validate() error {
	if c.DSN == "" {
		return errors.New("postgres DSN is required")
	}
	if c.MaxConns < 0 || c.MinConns < 0 {
		return errors.New("postgres connection limits cannot be negative")
	}
	if c.MaxConns > 0 && c.MinConns > c.MaxConns {
		return errors.New("postgres min connections exceed max connections")
	}
	for name, value := range map[string]time.Duration{
		"connect timeout":     c.ConnectTimeout,
		"query timeout":       c.QueryTimeout,
		"max connection life": c.MaxConnLifetime,
		"max connection idle": c.MaxConnIdleTime,
		"health check period": c.HealthCheckPeriod,
	} {
		if value < 0 {
			return fmt.Errorf("postgres %s cannot be negative", name)
		}
	}
	return nil
}

// Open creates and verifies a pgx v5 connection pool. Every new connection is
// pinned to UTC so timestamp interpretation is consistent across services.
func Open(ctx context.Context, cfg PoolConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	if cfg.ApplicationName != "" {
		poolCfg.ConnConfig.RuntimeParams["application_name"] = cfg.ApplicationName
	}
	for name, value := range cfg.RuntimeParams {
		poolCfg.ConnConfig.RuntimeParams[name] = value
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	poolCfg.MinConns = cfg.MinConns
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	}
	if cfg.ConnectTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	previousAfterConnect := poolCfg.AfterConnect
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if previousAfterConnect != nil {
			if err := previousAfterConnect(ctx, conn); err != nil {
				return err
			}
		}
		_, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'")
		return err
	}

	connectCtx := ctx
	cancel := func() {}
	if cfg.ConnectTimeout > 0 {
		connectCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
	}
	defer cancel()
	pool, err := pgxpool.NewWithConfig(connectCtx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	queryTimeout := cfg.QueryTimeout
	if queryTimeout == 0 {
		queryTimeout = defaultQueryTimeout
	}
	return New(pool, queryTimeout), nil
}

type Store struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func New(pool *pgxpool.Pool, queryTimeout time.Duration) *Store {
	if queryTimeout <= 0 {
		queryTimeout = defaultQueryTimeout
	}
	return &Store{pool: pool, queryTimeout: queryTimeout}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.pool.Ping(ctx)
}

func (s *Store) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

func durationMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}
