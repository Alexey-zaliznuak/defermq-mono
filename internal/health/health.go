package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/defermq/defermq/internal/buildinfo"
)

type Check func(context.Context) error

type Result struct {
	Name     string        `json:"name"`
	Ready    bool          `json:"ready"`
	Duration time.Duration `json:"-"`
	Error    error         `json:"-"`
}

type Registry struct {
	mu      sync.RWMutex
	checks  map[string]Check
	timeout time.Duration
}

func NewRegistry(timeout time.Duration) *Registry {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Registry{checks: make(map[string]Check), timeout: timeout}
}

func (r *Registry) Register(name string, check Check) error {
	if name == "" || check == nil {
		return errors.New("health check name and function are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.checks[name]; exists {
		return errors.New("health check is already registered")
	}
	r.checks[name] = check
	return nil
}

func (r *Registry) MustRegister(name string, check Check) {
	if err := r.Register(name, check); err != nil {
		panic(err)
	}
}

func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	delete(r.checks, name)
	r.mu.Unlock()
}

func (r *Registry) Check(ctx context.Context) (bool, []Result) {
	r.mu.RLock()
	checks := make(map[string]Check, len(r.checks))
	for name, check := range r.checks {
		checks[name] = check
	}
	r.mu.RUnlock()

	names := make([]string, 0, len(checks))
	for name := range checks {
		names = append(names, name)
	}
	sort.Strings(names)

	results := make([]Result, len(names))
	var wg sync.WaitGroup
	for index, name := range names {
		wg.Add(1)
		go func(index int, name string, check Check) {
			defer wg.Done()
			started := time.Now()
			checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()
			err := check(checkCtx)
			results[index] = Result{Name: name, Ready: err == nil, Duration: time.Since(started), Error: err}
		}(index, name, checks[name])
	}
	wg.Wait()

	ready := true
	for _, result := range results {
		ready = ready && result.Ready
	}
	return ready, results
}

type Liveness struct {
	shuttingDown atomic.Bool
}

func (l *Liveness) MarkShuttingDown() {
	l.shuttingDown.Store(true)
}

func (l *Liveness) Alive() bool {
	return !l.shuttingDown.Load()
}

type State struct {
	mu    sync.RWMutex
	ready bool
	err   error
}

func (s *State) MarkReady() {
	s.mu.Lock()
	s.ready = true
	s.err = nil
	s.mu.Unlock()
}

func (s *State) MarkFailed(err error) {
	if err == nil {
		err = errors.New("component is not ready")
	}
	s.mu.Lock()
	s.ready = false
	s.err = err
	s.mu.Unlock()
}

func (s *State) Check(context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ready {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New("component has not initialized")
}

type Heartbeat struct {
	mu     sync.RWMutex
	last   time.Time
	maxAge time.Duration
	clock  func() time.Time
}

func NewHeartbeat(maxAge time.Duration) *Heartbeat {
	return &Heartbeat{maxAge: maxAge, clock: time.Now}
}

func (h *Heartbeat) Beat() {
	h.mu.Lock()
	h.last = h.clock()
	h.mu.Unlock()
}

func (h *Heartbeat) Check(context.Context) error {
	h.mu.RLock()
	last := h.last
	h.mu.RUnlock()
	if last.IsZero() {
		return errors.New("heartbeat has not started")
	}
	if h.maxAge > 0 && h.clock().Sub(last) > h.maxAge {
		return errors.New("heartbeat is stale")
	}
	return nil
}

type Handler struct {
	service  string
	build    buildinfo.Info
	liveness *Liveness
	registry *Registry
}

func NewHandler(service string, build buildinfo.Info, liveness *Liveness, registry *Registry) *Handler {
	if liveness == nil {
		liveness = &Liveness{}
	}
	if registry == nil {
		registry = NewRegistry(3 * time.Second)
	}
	return &Handler{service: service, build: build, liveness: liveness, registry: registry}
}

func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	status := http.StatusOK
	state := "live"
	if !h.liveness.Alive() {
		status = http.StatusServiceUnavailable
		state = "shutting_down"
	}
	writeJSON(w, status, map[string]any{
		"status":  state,
		"service": h.service,
		"version": h.build.Version,
		"commit":  h.build.Commit,
	})
}

func (h *Handler) Ready(w http.ResponseWriter, request *http.Request) {
	ready, results := h.registry.Check(request.Context())
	status := http.StatusOK
	state := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	checks := make([]map[string]any, 0, len(results))
	for _, result := range results {
		check := map[string]any{"name": result.Name, "ready": result.Ready}
		if !result.Ready {
			check["message"] = "check failed"
		}
		checks = append(checks, check)
	}
	writeJSON(w, status, map[string]any{
		"status":  state,
		"service": h.service,
		"checks":  checks,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
