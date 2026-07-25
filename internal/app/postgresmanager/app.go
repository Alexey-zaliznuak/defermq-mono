package postgresmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/defermq/defermq/internal/buildinfo"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/hotstorage/valkey"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/defermq/defermq/internal/manager"
	"github.com/defermq/defermq/internal/observability"
	storagepostgres "github.com/defermq/defermq/internal/storage/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type App struct {
	config        Config
	logger        *zap.Logger
	pool          *pgxpool.Pool
	repository    *Repository
	nats          *natsjs.Connection
	valkey        *valkey.Connection
	hotIndex      *valkey.Store
	publisher     *natsjs.Publisher
	registry      *prometheus.Registry
	common        *observability.CommonMetrics
	metrics       *observability.ManagerMetrics
	metricsDB     *storagepostgres.Store
	ingestWriters []*ingest.Writer
	loopHealth    *observability.LoopHealth
	depthBucket   atomic.Uint32
	shutting      atomic.Bool
}

func New(ctx context.Context, cfg Config, logger *zap.Logger) (*App, error) {
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	poolConfig.MaxConns = cfg.PostgresMaxConn
	poolConfig.ConnConfig.ConnectTimeout = cfg.PostgresConnectTimeout
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(
		cfg.PostgresQueryTimeout.Milliseconds(),
		10,
	)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	repository := NewRepository(pool)
	if err := validateRepository(pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := repository.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	natsConnection, err := natsjs.Connect(ctx, cfg.NATS)
	if err != nil {
		pool.Close()
		return nil, err
	}
	valkeyConnection, err := valkey.Connect(ctx, cfg.Valkey)
	if err != nil {
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	hotIndex, err := valkey.New(valkeyConnection.Client(), cfg.ValkeyIndex)
	if err != nil {
		_ = valkeyConnection.Close()
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	if _, err := natsjs.EnsureStream(ctx, natsConnection.JS, cfg.Stream); err != nil {
		_ = valkeyConnection.Close()
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	if _, err := ingest.EnsureStream(ctx, natsConnection.JS, cfg.IngestStream); err != nil {
		_ = valkeyConnection.Close()
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	registry, common := observability.NewRegistry("defermq-postgres-manager", buildinfo.Current(), time.Now())
	metrics := observability.NewManagerMetrics(registry)
	loopHealth := observability.NewLoopHealth(
		[]string{"ingest", "promoter", "registrar", "scheduler", "repairer", "overdue_reconciler"},
		cfg.LoopHealthStartupGrace,
		cfg.LoopHealthMaxStaleness,
	)
	repository.SetMetrics(metrics)
	registry.MustRegister(observability.NewPGXPoolCollector(pool, "manager", "source"))
	common.SetDependencyReady("postgres", true)
	common.SetDependencyReady("nats", true)
	common.SetDependencyReady("valkey", true)
	metricsDB := storagepostgres.New(pool, cfg.PostgresQueryTimeout)
	startSequence, err := ingest.PrepareShardedConsumers(
		ctx,
		natsConnection.JS,
		cfg.IngestWriter.Stream,
		cfg.IngestWriter.Durable,
		cfg.DeleteLegacyIngestDurable,
	)
	if err != nil {
		_ = valkeyConnection.Close()
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	cfg.IngestWriter.StartSequence = startSequence
	writerConfigs, err := ingest.ShardWriterConfigs(cfg.IngestWriter)
	if err != nil {
		_ = valkeyConnection.Close()
		_ = natsConnection.Close(context.Background())
		pool.Close()
		return nil, err
	}
	ingestWriters := make([]*ingest.Writer, 0, len(writerConfigs))
	for _, writerConfig := range writerConfigs {
		writer, writerErr := ingest.NewWriter(ctx, natsConnection.JS, metricsDB, writerConfig, func(err error) {
			logger.Warn("ingest writer operation failed", zap.Error(err))
		})
		if writerErr != nil {
			_ = valkeyConnection.Close()
			_ = natsConnection.Close(context.Background())
			pool.Close()
			return nil, writerErr
		}
		writer.SetObserver(metrics, func(succeeded bool) {
			loopHealth.Observe("ingest", succeeded)
		})
		ingestWriters = append(ingestWriters, writer)
	}
	return &App{
		config:        cfg,
		logger:        logger,
		pool:          pool,
		repository:    repository,
		nats:          natsConnection,
		valkey:        valkeyConnection,
		hotIndex:      hotIndex,
		publisher:     natsjs.NewPublisher(natsConnection.JS, cfg.Stream.Subjects),
		registry:      registry,
		common:        common,
		metrics:       metrics,
		metricsDB:     metricsDB,
		ingestWriters: ingestWriters,
		loopHealth:    loopHealth,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)
	onError := func(component string, err error) {
		a.metrics.RecordLoopError(component)
		a.logger.Warn("manager loop operation failed", zap.String("component", component), zap.Error(err))
	}
	onLoop := func(component string, duration time.Duration, succeeded, fullBatch bool) {
		a.metrics.ObserveLoop(component, duration, succeeded, fullBatch)
		a.loopHealth.Observe(component, succeeded)
	}
	promoter := &manager.Promoter{
		Repository: a.repository,
		Config:     a.config.Promoter,
		OnError:    onError,
		Observe:    onLoop,
	}
	group.Go(func() error { return promoter.Run(groupCtx) })

	for i := 0; i < a.config.OutboxWorkers; i++ {
		worker := &manager.OutboxWorker{
			Repository: a.repository,
			Publisher:  a.publisher,
			Backoff: manager.NewBackoff(
				a.config.OutboxInitial,
				a.config.OutboxMax,
				time.Now().UnixNano()+int64(i),
			),
			Config:  a.config.Outbox,
			OnError: onError,
			Observe: onLoop,
			OnPublish: func(kind natsjs.OutboxKind, duration time.Duration) {
				a.metrics.ObserveOutboxPublish(string(kind), duration)
			},
		}
		worker.Config.Kind = natsjs.OutboxReady
		worker.Config.WorkerID = a.config.InstanceID + "-outbox-" + strconv.Itoa(i+1)
		group.Go(func() error { return worker.Run(groupCtx) })
	}

	for i := 0; i < a.config.RegistrarWorkers; i++ {
		registrar := &manager.Registrar{
			Repository: a.repository,
			Index:      a.hotIndex,
			Backoff: manager.NewBackoff(
				a.config.OutboxInitial, a.config.OutboxMax, time.Now().UnixNano()+int64(100+i),
			),
			Config:  a.config.Registrar,
			OnError: onError,
			Observe: onLoop,
			OnBatch: func(size int) {
				a.metrics.RegistrarBatchSize.Observe(float64(size))
			},
			OnZADD: func(result string) {
				a.metrics.RegistrarZADD.WithLabelValues(result).Inc()
			},
			OnValkeyError: func(operation string) {
				a.metrics.ValkeyOperationErrors.WithLabelValues(operation).Inc()
			},
		}
		registrar.Config.WorkerID = a.config.InstanceID + "-registrar-" + strconv.Itoa(i+1)
		group.Go(func() error { return registrar.Run(groupCtx) })
	}

	for i := 0; i < a.config.SchedulerWorkers; i++ {
		scheduler := &manager.Scheduler{
			Index: a.hotIndex, Repository: a.repository, Publisher: a.publisher,
			Config: a.config.Scheduler, OnError: onError, Observe: onLoop,
			OnClaimed: func(count int) {
				a.metrics.SchedulerClaimed.Add(float64(count))
			},
			OnPublished: func(result string) {
				a.metrics.SchedulerPublished.WithLabelValues(result).Inc()
			},
			OnReclaimed: func(count int) {
				a.metrics.SchedulerReclaimed.Add(float64(count))
			},
			OnWakeLag: func(lag time.Duration) {
				a.metrics.SchedulerWakeLag.Observe(lag.Seconds())
			},
			OnValkeyError: func(operation string) {
				a.metrics.ValkeyOperationErrors.WithLabelValues(operation).Inc()
			},
		}
		scheduler.Config.Owner = a.config.InstanceID
		scheduler.Config.Worker = i
		scheduler.Config.Workers = a.config.SchedulerWorkers
		group.Go(func() error { return scheduler.Run(groupCtx) })
	}
	repairer := &manager.Repairer{
		Repository: a.repository, Index: a.hotIndex, Config: a.config.Repairer,
		OnError: onError, Observe: onLoop,
		OnRegister: func(result string) {
			a.metrics.RepairRegistrations.WithLabelValues(result).Inc()
		},
		OnValkeyError: func(operation string) {
			a.metrics.ValkeyOperationErrors.WithLabelValues(operation).Inc()
		},
	}
	group.Go(func() error { return repairer.Run(groupCtx) })

	overdue := &manager.OverdueReconciler{
		Repository: a.repository, Config: a.config.Overdue, OnError: onError, Observe: onLoop,
	}
	reaper := &manager.ProcessingReaper{
		Repository: a.repository, Config: a.config.Reaper, OnError: onError, Observe: onLoop,
	}
	retention := &manager.RetentionCleaner{
		Repository: a.repository, Config: a.config.Retention, OnError: onError, Observe: onLoop,
	}
	group.Go(func() error { return overdue.Run(groupCtx) })
	group.Go(func() error { return reaper.Run(groupCtx) })
	group.Go(func() error { return retention.Run(groupCtx) })
	for _, writer := range a.ingestWriters {
		group.Go(func() error { return writer.Run(groupCtx) })
	}
	group.Go(func() error {
		return observability.RunManagerGaugeCollector(
			groupCtx, a.config.MetricsCollectionInterval, a.config.PostgresQueryTimeout,
			a.logger, a.metrics, a.collectManagerMetrics,
		)
	})

	server := &http.Server{
		Addr:              a.config.HTTPAddr,
		Handler:           a.adminRouter(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	group.Go(func() error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		<-groupCtx.Done()
		a.shutting.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("shutdown admin HTTP: %w", err)
		}
		return nil
	})
	return group.Wait()
}

func (a *App) Close(ctx context.Context) error {
	a.shutting.Store(true)
	a.common.SetDependencyReady("postgres", false)
	a.common.SetDependencyReady("nats", false)
	a.common.SetDependencyReady("valkey", false)
	err := a.nats.Close(ctx)
	err = errors.Join(err, a.valkey.Close())
	a.pool.Close()
	return err
}

func (a *App) adminRouter() http.Handler {
	router := chi.NewRouter()
	router.Get("/livez", func(writer http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		if a.shutting.Load() {
			status = http.StatusServiceUnavailable
		}
		writeJSON(writer, status, map[string]any{"service": "defermq-postgres-manager", "live": status == http.StatusOK})
	})
	router.Get("/readyz", a.readiness)
	router.Handle("/metrics", promhttp.HandlerFor(a.registry, promhttp.HandlerOpts{}))
	return router
}

func (a *App) collectManagerMetrics(ctx context.Context) (observability.ManagerGaugeSnapshot, error) {
	value, err := a.metricsDB.CollectManagerDBMetrics(ctx)
	if err != nil {
		return observability.ManagerGaugeSnapshot{}, err
	}
	snapshot := observability.ManagerGaugeSnapshot{
		UnpromotedHeadroom: float64(value.UnpromotedHeadroom) / float64(time.Second),
		ScheduledDue:       float64(value.ScheduledDue),
		Processing:         float64(value.Processing),
		ProcessingExpired:  float64(value.ProcessingExpired),
		Outbox:             make(map[string]observability.OutboxGaugeSnapshot, 2),
	}
	if value.OldestUnpromotedDeliverAt != nil {
		snapshot.UnpromotedExists = true
		snapshot.OldestUnpromotedDeliverAt = float64(value.OldestUnpromotedDeliverAt.Unix())
	} else {
		snapshot.UnpromotedHeadroom = a.config.Promoter.HotHorizon.Seconds()
	}
	for kind, item := range value.Outbox {
		snapshot.Outbox[string(kind)] = observability.OutboxGaugeSnapshot{
			Pending: float64(item.Pending), Locked: float64(item.Locked), OldestAge: item.OldestAge.Seconds(),
		}
	}
	bucket := int((a.depthBucket.Add(1) - 1) % uint32(a.hotIndex.BucketCount()))
	schedule, inflight, depthErr := a.hotIndex.BucketDepth(ctx, bucket)
	if depthErr != nil {
		a.metrics.ValkeyOperationErrors.WithLabelValues("bucket_depth").Inc()
	} else {
		label := strconv.Itoa(bucket)
		a.metrics.BucketScheduleDepth.WithLabelValues(label).Set(float64(schedule))
		a.metrics.BucketInflightDepth.WithLabelValues(label).Set(float64(inflight))
	}
	return snapshot, nil
}

func (a *App) readiness(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{
		"postgres": "ok", "nats": "ok", "valkey": "ok", "stream": "ok", "loops": "ok",
	}
	ready := !a.shutting.Load()
	if !ready {
		checks["loops"] = "shutting_down"
	} else if stale := a.loopHealth.Stale(); len(stale) != 0 {
		checks["loops"] = "stale:" + strings.Join(stale, ",")
		ready = false
	}
	if err := a.repository.Ping(ctx); err != nil {
		checks["postgres"] = "unavailable"
		ready = false
	}
	if err := a.nats.Ready(ctx); err != nil {
		checks["nats"] = "unavailable"
		checks["stream"] = "unavailable"
		ready = false
	} else if err := natsjs.CheckStream(ctx, a.nats.JS, a.config.Stream); err != nil {
		checks["stream"] = "incompatible"
		ready = false
	}
	if err := a.valkey.Ready(ctx); err != nil {
		checks["valkey"] = "unavailable"
		ready = false
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, map[string]any{"ready": ready, "checks": checks})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
