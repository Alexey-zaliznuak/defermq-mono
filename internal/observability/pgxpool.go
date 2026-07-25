package observability

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type PGXPoolCollector struct {
	pool            *pgxpool.Pool
	total           *prometheus.Desc
	idle            *prometheus.Desc
	acquired        *prometheus.Desc
	max             *prometheus.Desc
	acquireCount    *prometheus.Desc
	acquireDuration *prometheus.Desc
}

func NewPGXPoolCollector(pool *pgxpool.Pool, service, poolName string) *PGXPoolCollector {
	labels := prometheus.Labels{"service": service, "pool": poolName}
	return &PGXPoolCollector{
		pool:            pool,
		total:           prometheus.NewDesc("defermq_postgres_pool_total_connections", "Current total PostgreSQL pool connections.", nil, labels),
		idle:            prometheus.NewDesc("defermq_postgres_pool_idle_connections", "Current idle PostgreSQL pool connections.", nil, labels),
		acquired:        prometheus.NewDesc("defermq_postgres_pool_acquired_connections", "Current acquired PostgreSQL pool connections.", nil, labels),
		max:             prometheus.NewDesc("defermq_postgres_pool_max_connections", "Configured maximum PostgreSQL pool connections.", nil, labels),
		acquireCount:    prometheus.NewDesc("defermq_postgres_pool_acquire_count_total", "Total successful PostgreSQL pool acquisitions.", nil, labels),
		acquireDuration: prometheus.NewDesc("defermq_postgres_pool_acquire_duration_seconds_total", "Total time spent acquiring PostgreSQL connections.", nil, labels),
	}
}

func (c *PGXPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.total
	ch <- c.idle
	ch <- c.acquired
	ch <- c.max
	ch <- c.acquireCount
	ch <- c.acquireDuration
}

func (c *PGXPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}
	stat := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(stat.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(stat.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, stat.AcquireDuration().Seconds())
}
