package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/defermq/defermq/internal/app/gateway"
	"github.com/defermq/defermq/internal/observability"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Options struct {
	Service        *gateway.Service
	Logger         *zap.Logger
	Registerer     prometheus.Registerer
	Gatherer       prometheus.Gatherer
	Metrics        *observability.GatewayMetrics
	CommonMetrics  *observability.CommonMetrics
	RequestTimeout time.Duration
	MaxBodyBytes   int64
	ServiceName    string
	Version        string
	ShuttingDown   func() bool
}

func NewRouter(options Options) (http.Handler, error) {
	if options.Service == nil || options.Logger == nil || options.Registerer == nil ||
		options.Gatherer == nil || options.RequestTimeout <= 0 || options.MaxBodyBytes <= 0 {
		return nil, errors.New("invalid HTTP API options")
	}
	if options.ShuttingDown == nil {
		options.ShuttingDown = func() bool { return false }
	}
	metrics := options.Metrics
	if metrics == nil {
		metrics = observability.NewGatewayMetrics(options.Registerer)
	}
	handler := &handlers{
		service: options.Service, logger: options.Logger, metrics: metrics,
		commonMetrics: options.CommonMetrics, serviceName: options.ServiceName,
		version: options.Version, shuttingDown: options.ShuttingDown,
	}

	router := chi.NewRouter()
	router.Use(chimiddleware.RequestID)
	router.Use(requestIDHeader)
	router.Use(accessLogAndMetrics(options.Logger, metrics))
	router.Use(recoverer(options.Logger))
	router.Use(requestTimeout(options.RequestTimeout))
	router.Use(bodyLimit(options.MaxBodyBytes))

	router.Post("/v1/messages", handler.create)
	router.Get("/v1/messages/{id}", handler.get)
	router.Patch("/v1/messages/{id}/schedule", handler.reschedule)
	router.Delete("/v1/messages/{id}", handler.cancel)
	router.Get("/livez", handler.live)
	router.Get("/readyz", handler.ready)
	router.Handle("/metrics", promhttp.HandlerFor(options.Gatherer, promhttp.HandlerOpts{}))

	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "route_not_found", "route not found", nil)
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
	})
	return router, nil
}
