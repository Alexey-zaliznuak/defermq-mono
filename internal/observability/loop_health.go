package observability

import (
	"sort"
	"sync"
	"time"
)

// LoopHealth tracks successful cycles only. A busy loop and an idle loop are
// equally healthy; backlog size is deliberately not part of readiness.
type LoopHealth struct {
	mu           sync.RWMutex
	startedAt    time.Time
	startupGrace time.Duration
	maxStaleness time.Duration
	critical     map[string]struct{}
	lastSuccess  map[string]time.Time
	now          func() time.Time
}

func NewLoopHealth(components []string, startupGrace, maxStaleness time.Duration) *LoopHealth {
	critical := make(map[string]struct{}, len(components))
	for _, component := range components {
		critical[component] = struct{}{}
	}
	now := time.Now
	return &LoopHealth{
		startedAt: now(), startupGrace: startupGrace, maxStaleness: maxStaleness,
		critical: critical, lastSuccess: make(map[string]time.Time, len(critical)), now: now,
	}
}

func (h *LoopHealth) Observe(component string, succeeded bool) {
	if h == nil || !succeeded {
		return
	}
	h.mu.Lock()
	if _, critical := h.critical[component]; critical {
		h.lastSuccess[component] = h.now()
	}
	h.mu.Unlock()
}

// Stale returns critical loops that never succeeded after startup grace or
// whose latest successful cycle is older than maxStaleness.
func (h *LoopHealth) Stale() []string {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := h.now()
	if now.Sub(h.startedAt) <= h.startupGrace {
		return nil
	}
	stale := make([]string, 0)
	for component := range h.critical {
		last := h.lastSuccess[component]
		if last.IsZero() || now.Sub(last) > h.maxStaleness {
			stale = append(stale, component)
		}
	}
	sort.Strings(stale)
	return stale
}
