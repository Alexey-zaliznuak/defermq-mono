package observability

import (
	"errors"
	"os"
	"strings"
	"syscall"

	"github.com/defermq/defermq/internal/buildinfo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerOptions struct {
	Service          string
	InstanceID       string
	Level            string
	Format           string
	StacktraceLevel  string
	SamplingEnabled  bool
	OutputPaths      []string
	ErrorOutputPaths []string
	Build            buildinfo.Info
}

// NewLogger is the compact constructor intended for simple main packages.
func NewLogger(service, instanceID, format, level string) (*zap.Logger, error) {
	return NewLoggerWithOptions(LoggerOptions{
		Service:         service,
		InstanceID:      instanceID,
		Format:          format,
		Level:           level,
		StacktraceLevel: "error",
		SamplingEnabled: true,
		Build:           buildinfo.Current(),
	})
}

// NewLoggerWithOptions exposes build metadata, sampling, stacktrace, and sink controls.
func NewLoggerWithOptions(options LoggerOptions) (*zap.Logger, error) {
	if strings.TrimSpace(options.Service) == "" || strings.TrimSpace(options.InstanceID) == "" {
		return nil, errors.New("logger service and instance ID are required")
	}
	level, err := zapcore.ParseLevel(options.Level)
	if err != nil {
		return nil, err
	}
	stacktraceLevel, err := zapcore.ParseLevel(options.StacktraceLevel)
	if err != nil {
		return nil, err
	}
	if options.Format != "json" && options.Format != "console" {
		return nil, errors.New("logger format must be json or console")
	}
	if len(options.OutputPaths) == 0 {
		options.OutputPaths = []string{"stdout"}
	}
	if len(options.ErrorOutputPaths) == 0 {
		options.ErrorOutputPaths = []string{"stderr"}
	}

	encoder := zap.NewProductionEncoderConfig()
	encoder.TimeKey = "timestamp"
	encoder.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder.EncodeDuration = zapcore.StringDurationEncoder
	encoder.EncodeLevel = zapcore.LowercaseLevelEncoder
	if options.Format == "console" {
		encoder.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	cfg := zap.Config{
		Level:             zap.NewAtomicLevelAt(level),
		Development:       options.Format == "console",
		Encoding:          options.Format,
		EncoderConfig:     encoder,
		OutputPaths:       options.OutputPaths,
		ErrorOutputPaths:  options.ErrorOutputPaths,
		DisableStacktrace: true,
	}
	if options.SamplingEnabled {
		cfg.Sampling = &zap.SamplingConfig{Initial: 100, Thereafter: 100}
	}
	logger, err := cfg.Build(
		zap.AddCaller(),
		zap.AddStacktrace(stacktraceLevel),
		zap.Fields(
			zap.String("service", options.Service),
			zap.String("instance_id", options.InstanceID),
			zap.String("version", options.Build.Version),
			zap.String("commit", options.Build.Commit),
		),
	)
	if err != nil {
		return nil, err
	}
	return logger, nil
}

// Sync flushes a logger and suppresses only the well-known stdout/stderr sync
// errors returned by terminals and pipes.
func Sync(logger *zap.Logger) error {
	if logger == nil {
		return nil
	}
	err := logger.Sync()
	if err == nil || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, os.ErrInvalid) || strings.Contains(strings.ToLower(err.Error()), "inappropriate ioctl") {
		return nil
	}
	return err
}
