package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/defermq/defermq/internal/app/gateway"
	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/observability"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type handlers struct {
	service       *gateway.Service
	logger        *zap.Logger
	metrics       *observability.GatewayMetrics
	commonMetrics *observability.CommonMetrics
	serviceName   string
	version       string
	shuttingDown  func() bool
}

func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	if !requireJSON(w, r) {
		return
	}
	var request createRequest
	if err := decodeJSON(r, &request); err != nil {
		h.respondError(w, r, err)
		return
	}
	input, err := request.input(r.Header.Get("Idempotency-Key"))
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	delivery, replay, err := h.service.Create(r.Context(), input)
	if err != nil {
		h.metrics.MessagesCreated.WithLabelValues(string(request.Destination.Type), "error").Inc()
		h.respondError(w, r, err)
		return
	}
	response, err := responseFromDelivery(delivery, false)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
		h.metrics.IdempotencyReplays.Inc()
		h.metrics.MessagesCreated.WithLabelValues(string(delivery.DestinationType), "replay").Inc()
	} else {
		h.metrics.MessagesCreated.WithLabelValues(string(delivery.DestinationType), "created").Inc()
		h.metrics.PayloadSize.Observe(float64(len(input.Payload.Body)))
	}
	writeJSON(w, http.StatusAccepted, response)
}

func (h *handlers) get(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	message, err := h.service.Get(r.Context(), id)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	response, err := responseFromMessage(message)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *handlers) reschedule(w http.ResponseWriter, r *http.Request) {
	if !requireJSON(w, r) {
		h.metrics.MessagesRescheduled.WithLabelValues("error").Inc()
		return
	}
	id, err := pathID(r)
	if err != nil {
		h.metrics.MessagesRescheduled.WithLabelValues("error").Inc()
		h.respondError(w, r, err)
		return
	}
	var request scheduleRequest
	if err := decodeJSON(r, &request); err != nil {
		h.metrics.MessagesRescheduled.WithLabelValues("error").Inc()
		h.respondError(w, r, err)
		return
	}
	delivery, err := h.service.Reschedule(r.Context(), id, request.DeliverAt)
	if err != nil {
		h.metrics.MessagesRescheduled.WithLabelValues("error").Inc()
		h.respondError(w, r, err)
		return
	}
	response, err := responseFromDelivery(delivery, false)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	h.metrics.MessagesRescheduled.WithLabelValues("success").Inc()
	writeJSON(w, http.StatusOK, response)
}

func (h *handlers) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.metrics.MessagesCancelled.WithLabelValues("error").Inc()
		h.respondError(w, r, err)
		return
	}
	delivery, err := h.service.Cancel(r.Context(), id)
	if err != nil {
		h.metrics.MessagesCancelled.WithLabelValues("error").Inc()
		h.respondError(w, r, err)
		return
	}
	response, err := responseFromDelivery(delivery, false)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	h.metrics.MessagesCancelled.WithLabelValues("success").Inc()
	writeJSON(w, http.StatusOK, response)
}

func (h *handlers) live(w http.ResponseWriter, _ *http.Request) {
	if h.shuttingDown() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "shutting_down", "service": h.serviceName, "version": h.version,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "service": h.serviceName, "version": h.version,
	})
}

func (h *handlers) ready(w http.ResponseWriter, r *http.Request) {
	results := h.service.Ready(r.Context())
	if h.commonMetrics != nil {
		h.commonMetrics.SetDependencyReady("postgres", results["postgres"] == nil)
	}
	checks := make(map[string]string, len(results))
	ready := true
	for name, err := range results {
		if err != nil {
			checks[name] = "failed"
			ready = false
		} else {
			checks[name] = "ok"
		}
	}
	status := http.StatusOK
	state := "ready"
	if !ready || h.shuttingDown() {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": checks})
}

func (h *handlers) respondError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := classifyError(err)
	if status >= 500 {
		h.logger.Error("HTTP request failed",
			zap.String("request_id", chimiddleware.GetReqID(r.Context())),
			zap.Error(err),
		)
	}
	writeError(w, r, status, code, message, nil)
}

func classifyError(err error) (int, string, string) {
	var maxBytes *http.MaxBytesError
	switch {
	case errors.As(err, &maxBytes), errors.Is(err, domain.ErrPayloadTooLarge):
		return http.StatusRequestEntityTooLarge, "payload_too_large", "request or payload exceeds the configured limit"
	case errors.Is(err, gateway.ErrValidation), errors.Is(err, domain.ErrInvalidDestination):
		return http.StatusBadRequest, "validation_failed", validationMessage(err)
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, "not_found", "message not found"
	case errors.Is(err, domain.ErrInvalidState), errors.Is(err, domain.ErrConflict):
		return http.StatusConflict, "invalid_state", "message cannot be changed in its current state"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "request_timeout", "request timed out"
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}

func validationMessage(err error) string {
	message := err.Error()
	if index := strings.LastIndexByte(message, '\n'); index >= 0 {
		message = message[index+1:]
	}
	message = strings.TrimPrefix(message, gateway.ErrValidation.Error()+": ")
	message = strings.TrimPrefix(message, domain.ErrInvalidDestination.Error()+": ")
	return message
}

func decodeJSON(r *http.Request, destination any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var syntax *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			return err
		}
		if errors.As(err, &syntax) || errors.As(err, &typeError) || errors.Is(err, io.EOF) {
			return errors.Join(gateway.ErrValidation, errors.New("invalid JSON request"))
		}
		return errors.Join(gateway.ErrValidation, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.Join(gateway.ErrValidation, errors.New("request must contain one JSON value"))
	}
	return nil
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		return uuid.Nil, errors.Join(gateway.ErrValidation, errors.New("id must be a UUID"))
	}
	return id, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	writeJSON(w, status, errorEnvelope{Error: apiError{
		Code: code, Message: message, RequestID: chimiddleware.GetReqID(r.Context()), Details: details,
	}})
}

func requireJSON(w http.ResponseWriter, r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", nil)
		return false
	}
	return true
}
