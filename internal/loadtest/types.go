package loadtest

import "time"

type Action string

const (
	ActionDeliver    Action = "deliver"
	ActionCancel     Action = "cancel"
	ActionReschedule Action = "reschedule"
)

type PlannedMessage struct {
	Sequence          int
	Group             string
	Action            Action
	AdmitAt           time.Time
	DeliverAt         time.Time
	RescheduledAt     time.Time
	PayloadBytes      int
	MaxAttempts       int
	FailFirstAttempts int
	FailureStatus     int
}

type AcceptedMessage struct {
	PlannedMessage
	ID             string
	AcceptedAt     time.Time
	CreateLatency  time.Duration
	IdempotencyKey string
	Error          string
	ActionError    string
}

type DeliveryObservation struct {
	DeliveryID       string
	Group            string
	ScheduleRevision int64
	Attempt          int
	ScheduledAt      time.Time
	ReceivedAt       time.Time
	Lag              time.Duration
	StatusCode       int
	Duplicate        bool
	Early            bool
}

type StatusObservation struct {
	DeliveryID    string
	Status        string
	Attempts      int
	LastError     string
	DeliverAt     time.Time
	LastAttemptAt *time.Time
	DeliveredAt   *time.Time
	ObservedAt    time.Time
}

type ResourceSample struct {
	At         time.Time                    `json:"at"`
	Containers map[string]ContainerResource `json:"containers,omitempty"`
	Groups     map[string]ResourcePoint     `json:"groups"`
}

type ContainerResource struct {
	Service     string  `json:"service"`
	CPUPercent  float64 `json:"cpu_percent"`
	MemoryBytes float64 `json:"memory_bytes"`
	PIDs        float64 `json:"pids"`
	NetRXBytes  float64 `json:"net_rx_bytes"`
	NetTXBytes  float64 `json:"net_tx_bytes"`
	BlockRead   float64 `json:"block_read_bytes"`
	BlockWrite  float64 `json:"block_write_bytes"`
}

type ResourcePoint struct {
	CPUPercent  float64 `json:"cpu_percent"`
	MemoryBytes float64 `json:"memory_bytes"`
	PIDs        float64 `json:"pids"`
	NetRXBytes  float64 `json:"net_rx_bytes"`
	NetTXBytes  float64 `json:"net_tx_bytes"`
	BlockRead   float64 `json:"block_read_bytes"`
	BlockWrite  float64 `json:"block_write_bytes"`
}

type DistributionSummary struct {
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

type ResourceSummary struct {
	Samples      int                 `json:"samples"`
	CPUPercent   DistributionSummary `json:"cpu_percent"`
	MemoryMB     DistributionSummary `json:"memory_mb"`
	PIDs         DistributionSummary `json:"pids"`
	NetRXMB      DistributionSummary `json:"net_rx_mb"`
	NetTXMB      DistributionSummary `json:"net_tx_mb"`
	BlockReadMB  DistributionSummary `json:"block_read_mb"`
	BlockWriteMB DistributionSummary `json:"block_write_mb"`
}

type GroupResult struct {
	Name               string              `json:"name"`
	Planned            int                 `json:"planned"`
	Accepted           int                 `json:"accepted"`
	CreateErrors       int                 `json:"create_errors"`
	ActionErrors       int                 `json:"action_errors"`
	ExpectedDeliveries int                 `json:"expected_deliveries"`
	DeliveredUnique    int                 `json:"delivered_unique"`
	DeliveryAttempts   int                 `json:"delivery_attempts"`
	Duplicates         int                 `json:"duplicates"`
	EarlyDeliveries    int                 `json:"early_deliveries"`
	ReceiverEarly      int                 `json:"receiver_observed_early"`
	Missing            int                 `json:"missing"`
	Cancelled          int                 `json:"cancelled"`
	Dead               int                 `json:"dead"`
	OtherFinalStatuses map[string]int      `json:"other_final_statuses,omitempty"`
	CreateLatencyMS    DistributionSummary `json:"create_latency_ms"`
	DeliveryLagMS      DistributionSummary `json:"delivery_lag_ms"`
	ReceiverLagMS      DistributionSummary `json:"receiver_observed_lag_ms"`
}

type Report struct {
	SchemaVersion      int                        `json:"schema_version"`
	Name               string                     `json:"name"`
	Seed               int64                      `json:"seed"`
	StartedAt          time.Time                  `json:"started_at"`
	FinishedAt         time.Time                  `json:"finished_at"`
	DurationSeconds    float64                    `json:"duration_seconds"`
	AdmissionSeconds   float64                    `json:"admission_duration_seconds"`
	AdmissionRPS       float64                    `json:"admission_rps"`
	Planned            int                        `json:"planned"`
	Accepted           int                        `json:"accepted"`
	CreateErrors       int                        `json:"create_errors"`
	ActionErrors       int                        `json:"action_errors"`
	ExpectedDeliveries int                        `json:"expected_deliveries"`
	DeliveredUnique    int                        `json:"delivered_unique"`
	DeliveryAttempts   int                        `json:"delivery_attempts"`
	Duplicates         int                        `json:"duplicates"`
	EarlyDeliveries    int                        `json:"early_deliveries"`
	ReceiverEarly      int                        `json:"receiver_observed_early"`
	Missing            int                        `json:"missing"`
	Cancelled          int                        `json:"cancelled"`
	Dead               int                        `json:"dead"`
	DeliveryThroughput float64                    `json:"delivery_throughput_per_second"`
	CreateLatencyMS    DistributionSummary        `json:"create_latency_ms"`
	DeliveryLagMS      DistributionSummary        `json:"delivery_lag_ms"`
	ReceiverLagMS      DistributionSummary        `json:"receiver_observed_lag_ms"`
	Groups             []GroupResult              `json:"groups"`
	Resources          map[string]ResourceSummary `json:"resources"`
	ResourceSamples    []ResourceSample           `json:"resource_samples,omitempty"`
	Warnings           []string                   `json:"warnings,omitempty"`
}
