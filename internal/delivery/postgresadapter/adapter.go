package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Config struct {
	DSN             string
	Table           string
	AutoCreateTable bool
	MaxConns        int32
	QueryTimeout    time.Duration
}

type Adapter struct {
	pool         *pgxpool.Pool
	insertSQL    string
	queryTimeout time.Duration
}

func New(ctx context.Context, config Config) (*Adapter, error) {
	if config.DSN == "" || !identifierPattern.MatchString(config.Table) ||
		config.MaxConns <= 0 || config.QueryTimeout <= 0 {
		return nil, errors.New("invalid target PostgreSQL adapter configuration")
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse target PostgreSQL configuration: %w", err)
	}
	poolConfig.MaxConns = config.MaxConns
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create target PostgreSQL pool: %w", err)
	}
	quotedTable := pgx.Identifier{config.Table}.Sanitize()
	a := &Adapter{
		pool:         pool,
		queryTimeout: config.QueryTimeout,
		insertSQL: `INSERT INTO ` + quotedTable + ` (
			delivery_id, channel, payload, headers, metadata, scheduled_at, delivered_at
		) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
		ON CONFLICT (delivery_id) DO NOTHING`,
	}
	if config.AutoCreateTable {
		queryCtx, cancel := context.WithTimeout(ctx, config.QueryTimeout)
		defer cancel()
		_, err = pool.Exec(queryCtx, `CREATE TABLE IF NOT EXISTS `+quotedTable+` (
			delivery_id UUID PRIMARY KEY,
			channel TEXT NOT NULL DEFAULT '',
			payload BYTEA NOT NULL,
			headers JSONB NOT NULL DEFAULT '{}'::jsonb,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			scheduled_at TIMESTAMPTZ NOT NULL,
			delivered_at TIMESTAMPTZ NOT NULL
		)`)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("create target PostgreSQL table: %w", err)
		}
	}
	return a, nil
}

func (a *Adapter) Type() domain.DestinationType { return domain.DestinationPostgres }

func (a *Adapter) Push(ctx context.Context, req delivery.PushRequest) error {
	target := req.Destination.Postgres
	if target == nil {
		return delivery.NewPushError("invalid_destination", false, errors.New("PostgreSQL destination is missing"))
	}
	headers, err := json.Marshal(req.Headers)
	if err != nil {
		return delivery.NewPushError("invalid_headers", false, err)
	}
	metadata, err := json.Marshal(target.Metadata)
	if err != nil {
		return delivery.NewPushError("invalid_metadata", false, err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, a.queryTimeout)
	defer cancel()
	if _, err = a.pool.Exec(
		queryCtx,
		a.insertSQL,
		req.DeliveryID,
		target.Channel,
		req.Payload,
		headers,
		metadata,
		req.ScheduledAt.UTC(),
	); err != nil {
		return delivery.NewPushError("postgres_insert_failed", retryable(err), err)
	}
	return nil
}

func (a *Adapter) Ready(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, a.queryTimeout)
	defer cancel()
	return a.pool.Ping(queryCtx)
}

func (a *Adapter) Close(context.Context) error {
	a.pool.Close()
	return nil
}

func retryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return true
	}
	switch postgresError.Code {
	case "40001", "40P01", "55P03", "53300", "53400", "57P01", "57P02", "57P03":
		return true
	}
	return len(postgresError.Code) >= 2 && postgresError.Code[:2] == "08"
}
