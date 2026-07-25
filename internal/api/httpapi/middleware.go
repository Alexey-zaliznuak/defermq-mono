package httpapi

import (
	"context"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/defermq/defermq/internal/observability"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func requestTimeout(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func requestIDHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", chimiddleware.GetReqID(r.Context()))
		next.ServeHTTP(w, r)
	})
}

func bodyLimit(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

func recoverer(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.Error("HTTP handler panic",
						zap.Any("panic", recovered),
						zap.ByteString("stack", debug.Stack()),
						zap.String("request_id", chimiddleware.GetReqID(r.Context())),
					)
					writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func accessLogAndMetrics(logger *zap.Logger, metrics *observability.GatewayMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			writer := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(writer, r)

			route := "unmatched"
			if context := chi.RouteContext(r.Context()); context != nil && context.RoutePattern() != "" {
				route = context.RoutePattern()
			}
			status := writer.Status()
			if status == 0 {
				status = http.StatusOK
			}
			duration := time.Since(started)
			metrics.HTTPRequests.WithLabelValues(r.Method, route, observability.StatusClass(status)).Inc()
			metrics.HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(duration.Seconds())
			if r.ContentLength > 0 {
				metrics.HTTPRequestSize.WithLabelValues(route).Observe(float64(r.ContentLength))
			}
			metrics.HTTPResponseSize.WithLabelValues(route).Observe(float64(writer.BytesWritten()))
			logger.Info("HTTP request",
				zap.String("request_id", chimiddleware.GetReqID(r.Context())),
				zap.String("method", r.Method),
				zap.String("route", route),
				zap.Int("status", status),
				zap.Int("response_bytes", writer.BytesWritten()),
				zap.Duration("duration", duration),
			)
		})
	}
}
