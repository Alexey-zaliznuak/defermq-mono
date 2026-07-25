package valkey

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConnectionConfig struct {
	URL          string
	ClientName   string
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c ConnectionConfig) Validate() error {
	if c.URL == "" {
		return errors.New("Valkey URL is required")
	}
	if c.PoolSize <= 0 || c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		return errors.New("invalid Valkey pool or timeout configuration")
	}
	return nil
}

type Connection struct {
	client *redis.Client
}

func Connect(ctx context.Context, cfg ConnectionConfig) (*Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse Valkey URL: %w", err)
	}
	options.ClientName = cfg.ClientName
	options.PoolSize = cfg.PoolSize
	options.DialTimeout = cfg.DialTimeout
	options.ReadTimeout = cfg.ReadTimeout
	options.WriteTimeout = cfg.WriteTimeout
	client := redis.NewClient(options)
	connection := &Connection{client: client}
	if err := connection.Ready(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect Valkey: %w", err)
	}
	return connection, nil
}

func (c *Connection) Client() *redis.Client {
	if c == nil {
		return nil
	}
	return c.client
}

func (c *Connection) Ready(ctx context.Context) error {
	if c == nil || c.client == nil {
		return errors.New("Valkey client is not initialized")
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Valkey: %w", err)
	}
	return nil
}

func (c *Connection) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}
