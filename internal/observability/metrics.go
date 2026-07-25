package observability

import (
	"strconv"
	"time"

	"github.com/defermq/defermq/internal/buildinfo"
	"github.com/prometheus/client_golang/prometheus"
)

type CommonMetrics struct {
	BuildInfo       *prometheus.GaugeVec
	ProcessStart    prometheus.Gauge
	DependencyReady *prometheus.GaugeVec
}

func NewRegistry(service string, build buildinfo.Info, startedAt time.Time) (*prometheus.Registry, *CommonMetrics) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return registry, NewCommonMetrics(registry, service, build, startedAt)
}

func NewCommonMetrics(registerer prometheus.Registerer, service string, build buildinfo.Info, startedAt time.Time) *CommonMetrics {
	metrics := &CommonMetrics{
		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "defermq", Name: "build_info", Help: "Build information for the running DeferMQ process.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"version", "commit", "go_version"}),
		ProcessStart: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "defermq", Name: "process_start_time_seconds", Help: "Start time of the DeferMQ process.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		DependencyReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "defermq", Name: "dependency_ready", Help: "Whether a required external dependency is ready.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"dependency"}),
	}
	registerer.MustRegister(metrics.BuildInfo, metrics.ProcessStart, metrics.DependencyReady)
	metrics.BuildInfo.WithLabelValues(build.Version, build.Commit, build.GoVersion).Set(1)
	metrics.ProcessStart.Set(float64(startedAt.Unix()))
	return metrics
}

func (m *CommonMetrics) SetDependencyReady(dependency string, ready bool) {
	value := 0.0
	if ready {
		value = 1
	}
	m.DependencyReady.WithLabelValues(dependency).Set(value)
}

type GatewayMetrics struct {
	HTTPRequests        *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec
	HTTPRequestSize     *prometheus.HistogramVec
	HTTPResponseSize    *prometheus.HistogramVec
	MessagesCreated     *prometheus.CounterVec
	IdempotencyReplays  prometheus.Counter
	MessagesCancelled   *prometheus.CounterVec
	MessagesRescheduled *prometheus.CounterVec
	PayloadSize         prometheus.Histogram
	DBOperationDuration *prometheus.HistogramVec
	DBErrors            *prometheus.CounterVec
}

func NewGatewayMetrics(registerer prometheus.Registerer) *GatewayMetrics {
	m := &GatewayMetrics{
		HTTPRequests:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "http_requests_total", Help: "HTTP requests by route and outcome."}, []string{"method", "route", "status_class"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "gateway", Name: "http_request_duration_seconds", Help: "HTTP request duration.", Buckets: prometheus.DefBuckets}, []string{"method", "route"}),
		HTTPRequestSize:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "gateway", Name: "http_request_size_bytes", Help: "HTTP request size.", Buckets: prometheus.ExponentialBuckets(256, 4, 8)}, []string{"route"}),
		HTTPResponseSize:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "gateway", Name: "http_response_size_bytes", Help: "HTTP response size.", Buckets: prometheus.ExponentialBuckets(256, 4, 8)}, []string{"route"}),
		MessagesCreated:     prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "messages_created_total", Help: "Message creation attempts."}, []string{"destination_type", "result"}),
		IdempotencyReplays:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "idempotency_replays_total", Help: "Idempotent create replays."}),
		MessagesCancelled:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "messages_cancelled_total", Help: "Message cancellation attempts."}, []string{"result"}),
		MessagesRescheduled: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "messages_rescheduled_total", Help: "Message rescheduling attempts."}, []string{"result"}),
		PayloadSize:         prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "gateway", Name: "payload_size_bytes", Help: "Accepted payload sizes.", Buckets: prometheus.ExponentialBuckets(256, 4, 8)}),
		DBOperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "gateway", Name: "db_operation_duration_seconds", Help: "Database operation duration.", Buckets: prometheus.DefBuckets}, []string{"operation"}),
		DBErrors:            prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "gateway", Name: "db_errors_total", Help: "Database errors."}, []string{"operation"}),
	}
	registerer.MustRegister(m.HTTPRequests, m.HTTPRequestDuration, m.HTTPRequestSize, m.HTTPResponseSize, m.MessagesCreated,
		m.IdempotencyReplays, m.MessagesCancelled, m.MessagesRescheduled, m.PayloadSize, m.DBOperationDuration, m.DBErrors)
	return m
}

type ManagerMetrics struct {
	PromoterCycles            *prometheus.CounterVec
	PromoterBatches           *prometheus.CounterVec
	PromoterBatchSize         prometheus.Histogram
	PromoterBatchDuration     prometheus.Histogram
	PromotedMessages          prometheus.Counter
	OutboxClaimed             *prometheus.CounterVec
	OutboxPublished           *prometheus.CounterVec
	OutboxPublishDuration     *prometheus.HistogramVec
	OutboxPublishRetries      *prometheus.CounterVec
	OverdueReconciled         prometheus.Counter
	ProcessingLeasesReaped    prometheus.Counter
	RetentionDeleted          *prometheus.CounterVec
	CollectorSuccess          prometheus.Gauge
	CollectorLastSuccess      prometheus.Gauge
	CollectorDuration         prometheus.Histogram
	CollectorErrors           prometheus.Counter
	UnpromotedHeadroom        prometheus.Gauge
	UnpromotedExists          prometheus.Gauge
	OldestUnpromotedDeliverAt prometheus.Gauge
	ScheduledDue              prometheus.Gauge
	Processing                prometheus.Gauge
	ProcessingExpired         prometheus.Gauge
	OutboxPending             *prometheus.GaugeVec
	OutboxOldestAge           *prometheus.GaugeVec
	OutboxLocked              *prometheus.GaugeVec
}

func NewManagerMetrics(registerer prometheus.Registerer) *ManagerMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "manager", Name: name, Help: help}, labels)
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "manager", Name: name, Help: help})
	}
	m := &ManagerMetrics{
		PromoterCycles:            counter("promoter_cycles_total", "Promoter cycles.", "result"),
		PromoterBatches:           counter("promoter_batches_total", "Promoter batches.", "full"),
		PromoterBatchSize:         prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "manager", Name: "promoter_batch_size", Help: "Promoter candidate batch sizes.", Buckets: prometheus.ExponentialBuckets(1, 2, 12)}),
		PromoterBatchDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "manager", Name: "promoter_batch_duration_seconds", Help: "Promoter batch duration.", Buckets: prometheus.DefBuckets}),
		PromotedMessages:          prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "manager", Name: "promoted_messages_total", Help: "Promoted messages."}),
		OutboxClaimed:             counter("outbox_claimed_total", "Claimed outbox rows.", "kind"),
		OutboxPublished:           counter("outbox_published_total", "Outbox publish outcomes.", "kind", "result"),
		OutboxPublishDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "manager", Name: "outbox_publish_duration_seconds", Help: "Outbox publish duration.", Buckets: prometheus.DefBuckets}, []string{"kind"}),
		OutboxPublishRetries:      counter("outbox_publish_retries_total", "Outbox publish retries.", "kind"),
		OverdueReconciled:         prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "manager", Name: "overdue_reconciled_total", Help: "Overdue deliveries reconciled."}),
		ProcessingLeasesReaped:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "manager", Name: "processing_leases_reaped_total", Help: "Expired processing leases reaped."}),
		RetentionDeleted:          counter("retention_deleted_total", "Rows deleted by retention.", "entity"),
		CollectorSuccess:          gauge("metrics_collector_success", "Whether the last background metric collection succeeded."),
		CollectorLastSuccess:      gauge("metrics_collector_last_success_timestamp_seconds", "Last successful metric collection time."),
		CollectorDuration:         prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "manager", Name: "metrics_collector_duration_seconds", Help: "Background metric collection duration.", Buckets: prometheus.DefBuckets}),
		CollectorErrors:           prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "manager", Name: "metrics_collector_errors_total", Help: "Background metric collection errors."}),
		UnpromotedHeadroom:        gauge("unpromoted_headroom_seconds", "Seconds until the earliest unpromoted delivery; hot horizon when none exist."),
		UnpromotedExists:          gauge("unpromoted_exists", "Whether an unpromoted scheduled delivery exists."),
		OldestUnpromotedDeliverAt: gauge("oldest_unpromoted_deliver_at_timestamp_seconds", "Deliver-at time of the oldest unpromoted delivery."),
		ScheduledDue:              gauge("scheduled_due_total", "Current scheduled deliveries that are due."),
		Processing:                gauge("processing_total", "Current processing deliveries."),
		ProcessingExpired:         gauge("processing_expired_total", "Current processing deliveries with expired leases."),
		OutboxPending:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "manager", Name: "outbox_pending_total", Help: "Pending outbox rows."}, []string{"kind"}),
		OutboxOldestAge:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "manager", Name: "outbox_oldest_age_seconds", Help: "Age of the oldest pending outbox row."}, []string{"kind"}),
		OutboxLocked:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "manager", Name: "outbox_locked_total", Help: "Currently locked outbox rows."}, []string{"kind"}),
	}
	registerer.MustRegister(m.PromoterCycles, m.PromoterBatches, m.PromoterBatchSize, m.PromoterBatchDuration, m.PromotedMessages,
		m.OutboxClaimed, m.OutboxPublished, m.OutboxPublishDuration, m.OutboxPublishRetries, m.OverdueReconciled,
		m.ProcessingLeasesReaped, m.RetentionDeleted, m.CollectorSuccess, m.CollectorLastSuccess, m.CollectorDuration,
		m.CollectorErrors, m.UnpromotedHeadroom, m.UnpromotedExists, m.OldestUnpromotedDeliverAt, m.ScheduledDue,
		m.Processing, m.ProcessingExpired, m.OutboxPending, m.OutboxOldestAge, m.OutboxLocked)
	return m
}

type PusherMetrics struct {
	MessagesReceived       *prometheus.CounterVec
	EventsInvalid          *prometheus.CounterVec
	Claims                 *prometheus.CounterVec
	Inflight               *prometheus.GaugeVec
	Attempts               *prometheus.CounterVec
	AttemptDuration        *prometheus.HistogramVec
	DeliveryStartLag       *prometheus.HistogramVec
	DeliveryCompletionLag  *prometheus.HistogramVec
	RetriesScheduled       *prometheus.CounterVec
	Dead                   *prometheus.CounterVec
	Acks                   *prometheus.CounterVec
	ProcessingHeartbeat    *prometheus.CounterVec
	EarlyEvents            *prometheus.CounterVec
	CollectorSuccess       prometheus.Gauge
	CollectorLastSuccess   prometheus.Gauge
	CollectorDuration      prometheus.Histogram
	CollectorErrors        prometheus.Counter
	ProcessingOwned        prometheus.Gauge
	ProcessingOldestAge    prometheus.Gauge
	NATSConsumerPending    *prometheus.GaugeVec
	NATSConsumerAckPending *prometheus.GaugeVec
}

func NewPusherMetrics(registerer prometheus.Registerer) *PusherMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "pusher", Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "pusher", Name: name, Help: help, Buckets: prometheus.DefBuckets}, labels)
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "pusher", Name: name, Help: help})
	}
	m := &PusherMetrics{
		MessagesReceived:       counter("messages_received_total", "Ready events received.", "destination_type"),
		EventsInvalid:          counter("events_invalid_total", "Invalid ready events.", "reason"),
		Claims:                 counter("claims_total", "Delivery claim outcomes.", "destination_type", "result"),
		Inflight:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "pusher", Name: "inflight", Help: "Active delivery attempts."}, []string{"destination_type"}),
		Attempts:               counter("attempts_total", "External delivery attempts.", "destination_type", "result", "retryable"),
		AttemptDuration:        histogram("attempt_duration_seconds", "External delivery attempt duration.", "destination_type"),
		DeliveryStartLag:       histogram("delivery_start_lag_seconds", "Delay from scheduled time to attempt start.", "destination_type"),
		DeliveryCompletionLag:  histogram("delivery_completion_lag_seconds", "Delay from scheduled time to successful completion.", "destination_type"),
		RetriesScheduled:       counter("retries_scheduled_total", "Delivery retries scheduled.", "destination_type"),
		Dead:                   counter("dead_total", "Deliveries transitioned to dead.", "destination_type", "reason_class"),
		Acks:                   counter("acks_total", "JetStream acknowledgement outcomes.", "destination_type", "result"),
		ProcessingHeartbeat:    counter("processing_heartbeat_total", "Processing lease heartbeat outcomes.", "result"),
		EarlyEvents:            counter("early_events_total", "Ready events observed before deliver-at.", "destination_type"),
		CollectorSuccess:       gauge("metrics_collector_success", "Whether the last background metric collection succeeded."),
		CollectorLastSuccess:   gauge("metrics_collector_last_success_timestamp_seconds", "Last successful metric collection time."),
		CollectorDuration:      prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "defermq", Subsystem: "pusher", Name: "metrics_collector_duration_seconds", Help: "Background metric collection duration.", Buckets: prometheus.DefBuckets}),
		CollectorErrors:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "defermq", Subsystem: "pusher", Name: "metrics_collector_errors_total", Help: "Background metric collection errors."}),
		ProcessingOwned:        gauge("processing_owned_total", "Processing deliveries owned by this pusher instance."),
		ProcessingOldestAge:    gauge("processing_oldest_age_seconds", "Age of the oldest processing delivery owned by this instance."),
		NATSConsumerPending:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "pusher", Name: "nats_consumer_pending", Help: "Pending messages by destination consumer."}, []string{"destination_type"}),
		NATSConsumerAckPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "defermq", Subsystem: "pusher", Name: "nats_consumer_ack_pending", Help: "Unacknowledged messages by destination consumer."}, []string{"destination_type"}),
	}
	registerer.MustRegister(m.MessagesReceived, m.EventsInvalid, m.Claims, m.Inflight, m.Attempts, m.AttemptDuration,
		m.DeliveryStartLag, m.DeliveryCompletionLag, m.RetriesScheduled, m.Dead, m.Acks, m.ProcessingHeartbeat,
		m.EarlyEvents, m.CollectorSuccess, m.CollectorLastSuccess, m.CollectorDuration, m.CollectorErrors,
		m.ProcessingOwned, m.ProcessingOldestAge, m.NATSConsumerPending, m.NATSConsumerAckPending)
	return m
}

func StatusClass(status int) string {
	if status < 100 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}
