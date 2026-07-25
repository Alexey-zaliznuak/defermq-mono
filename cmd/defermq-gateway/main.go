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
	"github.com/defermq/defermq/internal/observability"
	"github.com/defermq/defermq/internal/storage/postgres"
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
	repository, err := gateway.NewPostgresRepository(store, gatewayMetrics)
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
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
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
	case serveErr := <-serverErrors:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
		return nil
	}

	shuttingDown.Store(true)
	commonMetrics.SetDependencyReady("postgres", false)
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
