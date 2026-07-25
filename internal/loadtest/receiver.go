package loadtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Receiver validates and records HTTP delivery attempts made by DeferMQ.
type Receiver struct {
	config         ReceiverConfig
	earlyTolerance time.Duration
	server         *http.Server

	mu           sync.Mutex
	messages     map[string]PlannedMessage
	succeeded    map[string]struct{}
	observations []DeliveryObservation
}

func NewReceiver(config ReceiverConfig, earlyTolerance time.Duration) *Receiver {
	receiver := &Receiver{
		config:         config,
		earlyTolerance: earlyTolerance,
		messages:       make(map[string]PlannedMessage),
		succeeded:      make(map[string]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(config.Path, receiver.handle)
	receiver.server = &http.Server{Addr: config.ListenAddress, Handler: mux}
	return receiver
}

// Handler exposes the receiver for httptest and embedding.
func (r *Receiver) Handler() http.Handler { return r.server.Handler }

// Register associates the Gateway delivery ID with its load-test plan.
func (r *Receiver) Register(deliveryID string, planned PlannedMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages[deliveryID] = planned
}

// Observations returns a stable snapshot of all valid delivery attempts.
func (r *Receiver) Observations() []DeliveryObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DeliveryObservation(nil), r.observations...)
}

// Start binds the configured address and serves until ctx is cancelled.
func (r *Receiver) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", r.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for load-test receiver: %w", err)
	}
	return r.serve(ctx, listener)
}

func (r *Receiver) serve(ctx context.Context, listener net.Listener) error {
	errs := make(chan error, 1)
	go func() {
		errs <- r.server.Serve(listener)
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.server.Shutdown(shutdownCtx)
	}()
	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve load-test receiver: %w", err)
	case <-ctx.Done():
		err := <-errs
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (r *Receiver) handle(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	deliveryID := request.Header.Get("X-DeferMQ-Delivery-ID")
	if _, err := uuid.Parse(deliveryID); err != nil || request.Header.Get("Idempotency-Key") != deliveryID {
		http.Error(w, "invalid delivery identity headers", http.StatusBadRequest)
		return
	}
	revision, err := strconv.ParseInt(request.Header.Get("X-DeferMQ-Schedule-Revision"), 10, 64)
	if err != nil || revision < 1 {
		http.Error(w, "invalid schedule revision header", http.StatusBadRequest)
		return
	}
	attempt, err := strconv.Atoi(request.Header.Get("X-DeferMQ-Attempt"))
	if err != nil || attempt < 1 {
		http.Error(w, "invalid attempt header", http.StatusBadRequest)
		return
	}
	scheduledAt, err := time.Parse(time.RFC3339Nano, request.Header.Get("X-DeferMQ-Scheduled-At"))
	if err != nil {
		http.Error(w, "invalid scheduled-at header", http.StatusBadRequest)
		return
	}

	receivedAt := time.Now().UTC()
	r.mu.Lock()
	planned, exists := r.messages[deliveryID]
	if !exists {
		r.mu.Unlock()
		http.Error(w, "unknown delivery", http.StatusNotFound)
		return
	}
	status := http.StatusNoContent
	if attempt <= planned.FailFirstAttempts {
		status = planned.FailureStatus
	}
	_, duplicate := r.succeeded[deliveryID]
	if status >= 200 && status < 300 {
		r.succeeded[deliveryID] = struct{}{}
	}
	r.observations = append(r.observations, DeliveryObservation{
		DeliveryID:       deliveryID,
		Group:            planned.Group,
		ScheduleRevision: revision,
		Attempt:          attempt,
		ScheduledAt:      scheduledAt.UTC(),
		ReceivedAt:       receivedAt,
		Lag:              receivedAt.Sub(scheduledAt),
		StatusCode:       status,
		Duplicate:        duplicate,
		Early:            receivedAt.Before(scheduledAt.Add(-r.earlyTolerance)),
	})
	r.mu.Unlock()
	w.WriteHeader(status)
}
