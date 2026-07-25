package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/defermq/defermq/internal/api/httpapi"
	"github.com/defermq/defermq/internal/app/gateway"
	"github.com/defermq/defermq/internal/buildinfo"
	"github.com/defermq/defermq/internal/config"
	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/defermq/defermq/internal/observability"
	"github.com/defermq/defermq/internal/storage/postgres"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load(config.ServiceGateway)
	if err != nil {
		logger, _ := zap.NewProduction()
		logger.Error("invalid gateway configuration", zap.Error(err))
		_ = logger.Sync()
		os.Exit(1)
	}
	logger, err := observability.NewLoggerWithOptions(observability.LoggerOptions{
		Service: cfg.Common.ServiceName, InstanceID: cfg.Common.InstanceID,
		Level: cfg.Common.Log.Level, Format: cfg.Common.Log.Format,
		StacktraceLevel: cfg.Common.Log.StacktraceLevel,
		SamplingEnabled: cfg.Common.Log.SamplingEnabled, Build: buildinfo.Current(),
	})
	if err != nil {
		fallback, _ := zap.NewProduction()
		fallback.Error("initialize logger", zap.Error(err))
		_ = fallback.Sync()
		os.Exit(1)
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), cfg.Postgres.ConnectTimeout)
	store, err := postgres.Open(connectCtx, postgres.PoolConfig{
		DSN: cfg.Postgres.DSN, ApplicationName: cfg.Common.ServiceName + "-" + cfg.Common.InstanceID,
		MaxConns: cfg.Postgres.MaxConns, MinConns: cfg.Postgres.MinConns,
		MaxConnLifetime: cfg.Postgres.MaxConnLifetime, MaxConnIdleTime: cfg.Postgres.MaxConnIdleTime,
		HealthCheckPeriod: cfg.Postgres.HealthCheckPeriod, ConnectTimeout: cfg.Postgres.ConnectTimeout,
		QueryTimeout: cfg.Postgres.QueryTimeout,
	})
	cancel()
	if err == nil {
		defer store.Close()
		if cfg.Postgres.AutoMigrate {
			err = store.Migrate(context.Background())
		}
	}
	if err == nil {
		err = run(context.Background(), cfg, logger, store)
	}
	if err != nil {
		logger.Error("gateway stopped", zap.Error(err))
	}
	_ = observability.Sync(logger)
	if err != nil {
		os.Exit(1)
	}
}

func run(parent context.Context, cfg config.Config, logger *zap.Logger, store *postgres.Store) error {
	registry, commonMetrics := observability.NewRegistry(cfg.Common.ServiceName, buildinfo.Current(), time.Now())
	gatewayMetrics := observability.NewGatewayMetrics(registry)
	connectCtx, connectCancel := context.WithTimeout(parent, cfg.NATS.ConnectTimeout)
	natsConnection, err := natsjs.Connect(connectCtx, natsjs.ConnectionConfig{
		URL: cfg.NATS.URL, Name: cfg.NATS.Name, User: cfg.NATS.User, Password: cfg.NATS.Password,
		CredsFile: cfg.NATS.CredentialsFile, TLSCAFile: cfg.NATS.TLSCAFile,
		TLSCertFile: cfg.NATS.TLSCertFile, TLSKeyFile: cfg.NATS.TLSKeyFile,
		TLSServerName: cfg.NATS.TLSServerName, ConnectTimeout: cfg.NATS.ConnectTimeout,
		ReconnectWait: cfg.NATS.ReconnectWait, MaxReconnects: cfg.NATS.MaxReconnects,
	})
	connectCancel()
	if err != nil {
		return err
	}
	defer natsConnection.Close(context.Background()) //nolint:errcheck
	ingestStream := ingest.StreamConfig{
		Name: cfg.NATS.IngestStream, Subject: cfg.NATS.IngestSubject,
		Replicas: cfg.NATS.StreamReplicas, MaxAge: cfg.NATS.StreamMaxAge,
		MaxBytes: cfg.NATS.StreamMaxBytes, MaxMsgSize: ingestMaxMessageSize(cfg),
		Duplicates: cfg.NATS.DuplicateWindow,
	}
	if _, err := ingest.EnsureStream(parent, natsConnection.JS, ingestStream); err != nil {
		return err
	}
	publisher, err := ingest.NewBatchPublisher(
		natsConnection.JS, cfg.NATS.IngestSubject, cfg.Gateway.IngestBatchSize,
		cfg.Gateway.IngestFlushInterval, cfg.Gateway.IngestQueueCapacity, cfg.Gateway.IngestShardCount,
	)
	if err != nil {
		return err
	}
	pendingKV, err := natsConnection.JS.CreateOrUpdateKeyValue(parent, jetstream.KeyValueConfig{
		Bucket: cfg.NATS.IngestPendingBucket, Description: "Gateway pending ingestion state",
		History: 1, TTL: cfg.NATS.StreamMaxAge, Storage: jetstream.FileStorage,
		Replicas: cfg.NATS.StreamReplicas,
	})
	if err != nil {
		return err
	}
	pendingStore, err := gateway.NewNATSPendingStore(pendingKV)
	if err != nil {
		return err
	}
	repository, err := gateway.NewIngestRepository(store, publisher, natsConnection, pendingStore)
	if err != nil {
		return err
	}
	service, err := gateway.New(repository, gateway.Options{
		HotHorizon: cfg.Common.HotHorizon, MaxPayloadBytes: cfg.Common.MaxPayloadBytes,
		DefaultMaxAttempts:     cfg.Gateway.DefaultMaxAttempts,
		MaxIdempotencyKeyBytes: cfg.Gateway.MaxIdempotencyKeyBytes,
		EnabledDestinations:    enabledDestinations(cfg.Common.EnabledDestinations),
	})
	if err != nil {
		return err
	}

	commonMetrics.SetDependencyReady("postgres", true)
	commonMetrics.SetDependencyReady("nats", true)
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	publisherErrors := make(chan error, 1)
	go func() { publisherErrors <- publisher.Run(ctx) }()
	var shuttingDown atomic.Bool
	router, err := httpapi.NewRouter(httpapi.Options{
		Service: service, Logger: logger, Registerer: registry, Gatherer: registry,
		Metrics: gatewayMetrics, CommonMetrics: commonMetrics,
		RequestTimeout: cfg.Gateway.RequestTimeout, MaxBodyBytes: maxRequestBodyBytes(cfg.Common.MaxPayloadBytes),
		ServiceName: cfg.Common.ServiceName, Version: buildinfo.Version, ShuttingDown: shuttingDown.Load,
	})
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: cfg.Gateway.HTTPAddr, Handler: router, ReadTimeout: cfg.Gateway.ReadTimeout,
		ReadHeaderTimeout: cfg.Gateway.ReadHeaderTimeout, WriteTimeout: cfg.Gateway.WriteTimeout,
		IdleTimeout: cfg.Gateway.IdleTimeout, MaxHeaderBytes: cfg.Gateway.MaxHeaderBytes,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("gateway HTTP server started", zap.String("address", cfg.Gateway.HTTPAddr))
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case publishErr := <-publisherErrors:
		if publishErr != nil {
			return publishErr
		}
		return errors.New("ingest publisher stopped unexpectedly")
	case serveErr := <-serverErrors:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
		return nil
	}

	shuttingDown.Store(true)
	commonMetrics.SetDependencyReady("postgres", false)
	commonMetrics.SetDependencyReady("nats", false)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Common.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return err
	}
	if err := <-serverErrors; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Info("gateway HTTP server stopped")
	return nil
}

func ingestMaxMessageSize(cfg config.Config) int32 {
	required := cfg.Common.MaxPayloadBytes*2 + (1 << 20)
	if required > int64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	if int64(cfg.NATS.StreamMaxMessageSize) > required {
		return cfg.NATS.StreamMaxMessageSize
	}
	return int32(required)
}

func enabledDestinations(values []string) map[domain.DestinationType]bool {
	result := make(map[domain.DestinationType]bool, len(values))
	for _, value := range values {
		result[domain.DestinationType(value)] = true
	}
	return result
}

func maxRequestBodyBytes(maxPayloadBytes int64) int64 {
	return maxPayloadBytes*2 + (1 << 20)
}
