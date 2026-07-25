package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/defermq/defermq/internal/app/pusher"
	"github.com/defermq/defermq/internal/buildinfo"
	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/delivery/httpadapter"
	"github.com/defermq/defermq/internal/delivery/kafkaadapter"
	"github.com/defermq/defermq/internal/delivery/postgresadapter"
	"github.com/defermq/defermq/internal/delivery/rabbitadapter"
	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/observability"
	storagepostgres "github.com/defermq/defermq/internal/storage/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := newLogger(os.Getenv("DEFERMQ_LOG_LEVEL"))
	defer logger.Sync()
	if err := run(logger); err != nil {
		logger.Error("pusher stopped", zap.Error(err))
		os.Exit(1)
	}
}

func run(logger *zap.Logger) error {
	config, err := pusher.LoadConfig()
	if err != nil {
		return err
	}
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceStore, err := storagepostgres.Open(root, storagepostgres.PoolConfig{
		DSN:             config.SourceDSN,
		ApplicationName: "defermq-pusher",
		MaxConns:        config.SourceMaxConns,
		ConnectTimeout:  config.QueryTimeout,
		QueryTimeout:    config.QueryTimeout,
		RuntimeParams: map[string]string{
			"synchronous_commit": config.SynchronousCommit,
		},
	})
	if err != nil {
		return err
	}
	defer sourceStore.Close()
	repository, err := pusher.NewPostgresRepository(sourceStore)
	if err != nil {
		return err
	}

	natsConnection, err := natsjs.Connect(root, natsjs.ConnectionConfig{
		URL: config.NATSURL, Name: config.NATSName,
		User: config.NATSUser, Password: config.NATSPassword, CredsFile: config.NATSCredsFile,
		TLSCAFile: config.NATSTLSCAFile, TLSCertFile: config.NATSTLSCertFile,
		TLSKeyFile: config.NATSTLSKeyFile, TLSServerName: config.NATSTLSServerName,
		ConnectTimeout: config.NATSConnectTimeout, ReconnectWait: config.NATSReconnectWait,
		MaxReconnects: config.NATSMaxReconnects,
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		_ = natsConnection.Close(closeCtx)
	}()
	js := natsConnection.JS
	registry, commonMetrics := observability.NewRegistry("defermq-pusher", buildinfo.Current(), time.Now())
	pusherMetrics := observability.NewPusherMetrics(registry)
	registry.MustRegister(observability.NewPGXPoolCollector(sourceStore.Pool(), "pusher", "source"))
	commonMetrics.SetDependencyReady("postgres", true)
	commonMetrics.SetDependencyReady("nats", true)

	adapters := make([]delivery.Adapter, 0, len(config.Enabled))
	for _, typ := range config.Enabled {
		var adapter delivery.Adapter
		switch typ {
		case domain.DestinationHTTP:
			adapter, err = httpadapter.New(config.HTTP)
		case domain.DestinationKafka:
			adapter, err = kafkaadapter.New(config.Kafka)
		case domain.DestinationRabbit:
			adapter, err = rabbitadapter.New(config.Rabbit)
		case domain.DestinationPostgres:
			adapter, err = postgresadapter.New(root, config.TargetPostgres)
		}
		if err != nil {
			return err
		}
		adapters = append(adapters, adapter)
	}
	dispatcher, err := delivery.NewDispatcher(adapters...)
	if err != nil {
		return err
	}

	random := &lockedRandom{source: rand.New(rand.NewSource(time.Now().UnixNano()))}
	config.Backoff.Random = random
	consumers := make([]pusher.Consumer, 0, len(config.Enabled))
	pools := make([]*pusher.Pool, 0, len(config.Enabled))
	subjects := natsjs.Subjects{ReadyPrefix: config.SubjectsReady}
	for _, typ := range config.Enabled {
		consumer, consumerErr := pusher.NewNATSConsumer(root, js, pusher.NATSConsumerConfig{
			Stream:        config.NATSStream,
			Durable:       "defermq-pusher-" + string(typ),
			Subjects:      subjects,
			Type:          typ,
			AckWait:       config.AckWait,
			MaxAckPending: config.MaxAckPending,
			MaxDeliver:    config.MaxDeliver,
			MaxBatch:      config.FetchBatch,
			MaxWait:       config.FetchMaxWait,
		})
		if consumerErr != nil {
			return consumerErr
		}
		workerCount := config.Workers[typ]
		workerPool, poolErr := pusher.NewPool(pusher.PoolConfig{
			Workers:            workerCount,
			QueueSize:          max(workerCount*2, config.FetchBatch, config.ClaimBatch),
			FetchBatchSize:     config.FetchBatch,
			FetchMaxWait:       config.FetchMaxWait,
			ClaimBatchSize:     config.ClaimBatch,
			ClaimFlushInterval: config.ClaimFlushInterval,
			ProcessingLease:    config.Lease,
			HeartbeatInterval:  config.Heartbeat,
			ClockSkewTolerance: config.ClockTolerance,
			MaxPayloadBytes:    config.MaxPayload,
			HotHorizon:         config.HotHorizon,
			TransitionRetry:    time.Second,
			ShutdownTimeout:    config.ShutdownTimeout,
		}, config.InstanceID, consumer, repository, dispatcher, config.Backoff, logger)
		if poolErr != nil {
			return poolErr
		}
		workerPool.SetMetrics(pusherMetrics)
		consumers = append(consumers, consumer)
		pools = append(pools, workerPool)
	}
	app, err := pusher.NewApp(pools, consumers, repository, dispatcher)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if closeErr := app.Close(closeCtx); closeErr != nil {
			logger.Warn("pusher close failed", zap.Error(closeErr))
		}
	}()

	router := adminRouter(root, app, registry)
	server := &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       time.Minute,
	}
	group, groupCtx := errgroup.WithContext(root)
	group.Go(func() error { return app.Run(groupCtx) })
	group.Go(func() error {
		return observability.RunPusherGaugeCollector(
			groupCtx, 5*time.Second, config.QueryTimeout, logger, pusherMetrics,
			func(ctx context.Context) (observability.PusherGaugeSnapshot, error) {
				db, err := sourceStore.CollectPusherDBMetrics(ctx, config.InstanceID)
				if err != nil {
					return observability.PusherGaugeSnapshot{}, err
				}
				snapshot := observability.PusherGaugeSnapshot{
					ProcessingOwned: float64(db.ProcessingOwned), ProcessingOldestAge: db.ProcessingOldestAge.Seconds(),
					ConsumerPending: make(map[string]float64), ConsumerAckPending: make(map[string]float64),
				}
				for _, consumer := range consumers {
					if concrete, ok := consumer.(*pusher.NATSConsumer); ok {
						pending, ackPending, infoErr := concrete.Pending(ctx)
						if infoErr != nil {
							return observability.PusherGaugeSnapshot{}, infoErr
						}
						key := string(consumer.Type())
						snapshot.ConsumerPending[key] = float64(pending)
						snapshot.ConsumerAckPending[key] = float64(ackPending)
					}
				}
				return snapshot, nil
			},
		)
	})
	group.Go(func() error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})
	logger.Info("pusher started",
		zap.String("instance_id", config.InstanceID),
		zap.String("http_addr", config.HTTPAddr),
		zap.Strings("destinations", destinationNames(config.Enabled)),
	)
	return group.Wait()
}

func adminRouter(root context.Context, app *pusher.App, registry *prometheus.Registry) http.Handler {
	router := chi.NewRouter()
	router.Get("/livez", func(writer http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		if root.Err() != nil {
			status = http.StatusServiceUnavailable
		}
		writeJSON(writer, status, map[string]any{"service": "defermq-pusher", "live": status == http.StatusOK})
	})
	router.Get("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
		defer cancel()
		if err := app.Ready(ctx); err != nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ready": false, "error": err.Error()})
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"ready": true})
	})
	router.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return router
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func newLogger(levelText string) *zap.Logger {
	if levelText == "" {
		levelText = "info"
	}
	format := os.Getenv("DEFERMQ_LOG_FORMAT")
	if format == "" {
		format = "json"
	}
	logger, err := observability.NewLoggerWithOptions(observability.LoggerOptions{
		Service: "defermq-pusher", InstanceID: os.Getenv("DEFERMQ_INSTANCE_ID"),
		Level: levelText, Format: format, StacktraceLevel: "error",
		SamplingEnabled: os.Getenv("DEFERMQ_LOG_SAMPLING_ENABLED") != "false",
		Build:           buildinfo.Current(),
	})
	if err != nil {
		return zap.NewNop()
	}
	return logger
}

func destinationNames(types []domain.DestinationType) []string {
	result := make([]string, len(types))
	for index, typ := range types {
		result[index] = string(typ)
	}
	return result
}

type lockedRandom struct {
	mu     sync.Mutex
	source *rand.Rand
}

func (r *lockedRandom) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.source.Float64()
}
