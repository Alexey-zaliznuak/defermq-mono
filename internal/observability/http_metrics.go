package observability

import (
	"bufio"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

func (m *GatewayMetrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, request)

		route := chi.RouteContext(request.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		m.HTTPRequests.WithLabelValues(request.Method, route, StatusClass(recorder.status)).Inc()
		m.HTTPRequestDuration.WithLabelValues(request.Method, route).Observe(time.Since(started).Seconds())
		if request.ContentLength >= 0 {
			m.HTTPRequestSize.WithLabelValues(route).Observe(float64(request.ContentLength))
		}
		m.HTTPResponseSize.WithLabelValues(route).Observe(float64(recorder.bytes))
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
	wrote  bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.status = status
	r.wrote = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	count, err := r.ResponseWriter.Write(body)
	r.bytes += count
	return count, err
}

func (r *responseRecorder) Flush() {
	http.NewResponseController(r.ResponseWriter).Flush()
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(r.ResponseWriter).Hijack()
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
