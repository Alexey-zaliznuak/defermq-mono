package natsjs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ConnectionConfig struct {
	URL            string
	Name           string
	User           string
	Password       string
	CredsFile      string
	TLSCAFile      string
	TLSCertFile    string
	TLSKeyFile     string
	TLSServerName  string
	ConnectTimeout time.Duration
	ReconnectWait  time.Duration
	MaxReconnects  int
}

func (c ConnectionConfig) Validate() error {
	if c.URL == "" {
		return errors.New("NATS URL is required")
	}
	if c.User != "" && c.CredsFile != "" {
		return errors.New("NATS user/password and credentials file are mutually exclusive")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("both NATS TLS certificate and key are required")
	}
	if c.ConnectTimeout <= 0 || c.ReconnectWait <= 0 || c.MaxReconnects < -1 {
		return errors.New("invalid NATS timeout or reconnect configuration")
	}
	return nil
}

type Connection struct {
	Conn *nats.Conn
	JS   jetstream.JetStream
}

func Connect(ctx context.Context, cfg ConnectionConfig) (*Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts := []nats.Option{
		nats.Name(cfg.Name),
		nats.Timeout(cfg.ConnectTimeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnects),
	}
	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	} else if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	tlsConfig, err := loadTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		opts = append(opts, nats.Secure(tlsConfig))
	}

	type result struct {
		conn *nats.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, connectErr := nats.Connect(cfg.URL, opts...)
		ch <- result{conn: conn, err: connectErr}
	}()
	select {
	case <-ctx.Done():
		go func() {
			result := <-ch
			if result.conn != nil {
				result.conn.Close()
			}
		}()
		return nil, ctx.Err()
	case result := <-ch:
		if result.err != nil {
			return nil, fmt.Errorf("connect NATS: %w", result.err)
		}
		js, err := jetstream.New(result.conn)
		if err != nil {
			result.conn.Close()
			return nil, fmt.Errorf("create JetStream client: %w", err)
		}
		return &Connection{Conn: result.conn, JS: js}, nil
	}
}

func (c *Connection) Ready(context.Context) error {
	if c == nil || c.Conn == nil || !c.Conn.IsConnected() {
		return errors.New("NATS is not connected")
	}
	return nil
}

func (c *Connection) Close(ctx context.Context) error {
	if c == nil || c.Conn == nil {
		return nil
	}
	drained := make(chan error, 1)
	go func() { drained <- c.Conn.Drain() }()
	select {
	case err := <-drained:
		c.Conn.Close()
		return err
	case <-ctx.Done():
		c.Conn.Close()
		return ctx.Err()
	}
}

func loadTLSConfig(cfg ConnectionConfig) (*tls.Config, error) {
	if cfg.TLSCAFile == "" && cfg.TLSCertFile == "" && cfg.TLSServerName == "" {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}
	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read NATS TLS CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("NATS TLS CA contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if cfg.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load NATS TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
