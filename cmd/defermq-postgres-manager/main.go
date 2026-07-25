package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/defermq/defermq/internal/app/postgresmanager"
	"github.com/defermq/defermq/internal/buildinfo"
	"github.com/defermq/defermq/internal/observability"
	"go.uber.org/zap"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := postgresmanager.LoadConfig()
	if err != nil {
		logger, _ := zap.NewProduction()
		logger.Error("invalid configuration", zap.Error(err))
		_ = logger.Sync()
		return 1
	}
	logger, err := observability.NewLoggerWithOptions(observability.LoggerOptions{
		Service: "defermq-postgres-manager", InstanceID: cfg.InstanceID,
		Level: envDefault("DEFERMQ_LOG_LEVEL", "info"), Format: envDefault("DEFERMQ_LOG_FORMAT", "json"),
		StacktraceLevel: "error", SamplingEnabled: os.Getenv("DEFERMQ_LOG_SAMPLING_ENABLED") != "false",
		Build: buildinfo.Current(),
	})
	if err != nil {
		_, _ = os.Stderr.WriteString("create logger: " + err.Error() + "\n")
		return 1
	}
	defer func() { _ = observability.Sync(logger) }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := postgresmanager.New(ctx, cfg, logger.With(
		zap.String("service", "defermq-postgres-manager"),
		zap.String("instance_id", cfg.InstanceID),
	))
	if err != nil {
		logger.Error("initialize postgres manager", zap.Error(err))
		return 1
	}
	if err := app.Run(ctx); err != nil {
		logger.Error("postgres manager stopped with error", zap.Error(err))
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = app.Close(closeCtx)
		return 1
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := app.Close(closeCtx); err != nil {
		logger.Warn("close postgres manager", zap.Error(err))
	}
	return 0
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
