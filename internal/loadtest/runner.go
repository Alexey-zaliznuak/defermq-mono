package loadtest

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ResourceSampler supplies optional host/container measurements.
type ResourceSampler interface {
	Sample(context.Context) (ResourceSample, error)
}

type Logger interface {
	Printf(format string, arguments ...any)
}

type RunResult struct {
	StartedAt       time.Time
	FinishedAt      time.Time
	Planned         []PlannedMessage
	Accepted        []AcceptedMessage
	Deliveries      []DeliveryObservation
	Statuses        []StatusObservation
	ResourceSamples []ResourceSample
	Warnings        []string
}

type Option func(*Runner)

func WithResourceSampler(sampler ResourceSampler) Option {
	return func(runner *Runner) { runner.sampler = sampler }
}

func WithLogger(logger Logger) Option {
	return func(runner *Runner) {
		if logger != nil {
			runner.logger = logger
		}
	}
}

type Runner struct {
	config   Config
	client   *GatewayClient
	receiver *Receiver
	sampler  ResourceSampler
	logger   Logger
	runID    string
}

func NewRunner(config Config, options ...Option) (*Runner, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid load-test config: %w", err)
	}
	client, err := NewGatewayClient(config.Gateway, config.Load.CreateConcurrency, config.Load.StatusConcurrency)
	if err != nil {
		return nil, err
	}
	runner := &Runner{
		config:   config,
		client:   client,
		receiver: NewReceiver(config.Receiver, config.Load.EarlyTolerance.Value()),
		logger:   log.Default(),
	}
	for _, option := range options {
		option(runner)
	}
	return runner, nil
}

func (r *Runner) Run(ctx context.Context) (RunResult, error) {
	result := RunResult{StartedAt: time.Now().UTC()}
	r.runID = strconv.FormatInt(result.StartedAt.UnixNano(), 36)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	listener, err := net.Listen("tcp", r.config.Receiver.ListenAddress)
	if err != nil {
		return result, fmt.Errorf("start receiver: %w", err)
	}
	receiverErr := make(chan error, 1)
	go func() { receiverErr <- r.receiver.serve(runCtx, listener) }()

	var sampleWG sync.WaitGroup
	var resultMu sync.Mutex
	if r.sampler != nil && r.config.Resources.Enabled {
		sampleWG.Add(1)
		go func() {
			defer sampleWG.Done()
			r.sampleResources(runCtx, &result, &resultMu)
		}()
	}

	if err := waitContext(ctx, r.config.Load.Warmup.Value()); err != nil {
		cancel()
		sampleWG.Wait()
		result.FinishedAt = time.Now().UTC()
		return result, err
	}
	origin := time.Now().UTC()
	result.Planned = Plan(r.config, origin)
	result.Accepted = r.admit(ctx, result.Planned)
	result.Statuses = r.poll(ctx, result.Accepted, &result.Warnings, &resultMu)

	if err := waitContext(ctx, r.config.Load.Cooldown.Value()); err != nil {
		cancel()
		sampleWG.Wait()
		result.Deliveries = r.receiver.Observations()
		result.FinishedAt = time.Now().UTC()
		return result, err
	}
	cancel()
	sampleWG.Wait()
	if err := <-receiverErr; err != nil {
		result.Warnings = append(result.Warnings, err.Error())
	}
	result.Deliveries = r.receiver.Observations()
	result.FinishedAt = time.Now().UTC()
	return result, ctx.Err()
}

func (r *Runner) admit(ctx context.Context, planned []PlannedMessage) []AcceptedMessage {
	schedule := append([]PlannedMessage(nil), planned...)
	sort.SliceStable(schedule, func(i, j int) bool {
		if schedule[i].AdmitAt.Equal(schedule[j].AdmitAt) {
			return schedule[i].Sequence < schedule[j].Sequence
		}
		return schedule[i].AdmitAt.Before(schedule[j].AdmitAt)
	})

	jobs := make(chan PlannedMessage)
	results := make(chan AcceptedMessage, len(schedule))
	var workers sync.WaitGroup
	for range r.config.Load.CreateConcurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for message := range jobs {
				results <- r.admitOne(ctx, message)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, message := range schedule {
			if err := waitUntil(ctx, message.AdmitAt); err != nil {
				return
			}
			select {
			case jobs <- message:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	accepted := make([]AcceptedMessage, 0, len(schedule))
	for message := range results {
		accepted = append(accepted, message)
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].Sequence < accepted[j].Sequence })
	return accepted
}

func (r *Runner) admitOne(ctx context.Context, planned PlannedMessage) AcceptedMessage {
	accepted := AcceptedMessage{
		PlannedMessage: planned,
		IdempotencyKey: fmt.Sprintf("loadtest-%d-%s-%d", r.config.Seed, r.runID, planned.Sequence),
	}
	started := time.Now()
	id, err := r.client.Create(ctx, planned, r.destinationURL(), accepted.IdempotencyKey)
	accepted.AcceptedAt = time.Now().UTC()
	accepted.CreateLatency = time.Since(started)
	if err != nil {
		accepted.Error = err.Error()
		return accepted
	}
	accepted.ID = id
	r.receiver.Register(id, planned)
	switch planned.Action {
	case ActionCancel:
		err = r.client.Cancel(ctx, id)
	case ActionReschedule:
		err = r.client.Reschedule(ctx, id, planned.RescheduledAt)
	}
	if err != nil {
		accepted.ActionError = err.Error()
	}
	return accepted
}

func (r *Runner) poll(
	ctx context.Context,
	accepted []AcceptedMessage,
	warnings *[]string,
	resultMu *sync.Mutex,
) []StatusObservation {
	pending := make(map[string]struct{}, len(accepted))
	for _, message := range accepted {
		if message.ID != "" {
			pending[message.ID] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	pollCtx, cancel := context.WithTimeout(ctx, r.config.Load.AwaitTimeout.Value())
	defer cancel()
	var observations []StatusObservation
	for len(pending) > 0 {
		ids := make([]string, 0, len(pending))
		for id := range pending {
			ids = append(ids, id)
		}
		type statusResult struct {
			observation StatusObservation
			err         error
		}
		jobs := make(chan string)
		results := make(chan statusResult, len(ids))
		var workers sync.WaitGroup
		workerCount := min(r.config.Load.StatusConcurrency, len(ids))
		for range workerCount {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for id := range jobs {
					observation, err := r.client.Status(pollCtx, id)
					results <- statusResult{observation: observation, err: err}
				}
			}()
		}
		go func() {
			for _, id := range ids {
				jobs <- id
			}
			close(jobs)
			workers.Wait()
			close(results)
		}()
		for result := range results {
			if result.err != nil {
				if pollCtx.Err() == nil {
					resultMu.Lock()
					*warnings = append(*warnings, result.err.Error())
					resultMu.Unlock()
				}
				continue
			}
			observations = append(observations, result.observation)
			if terminalStatus(result.observation.Status) {
				delete(pending, result.observation.DeliveryID)
			}
		}
		if len(pending) == 0 {
			break
		}
		if err := waitContext(pollCtx, r.config.Load.PollInterval.Value()); err != nil {
			break
		}
	}
	if len(pending) > 0 {
		resultMu.Lock()
		*warnings = append(*warnings, fmt.Sprintf("%d messages did not reach a terminal status", len(pending)))
		resultMu.Unlock()
	}
	sort.SliceStable(observations, func(i, j int) bool {
		return observations[i].ObservedAt.Before(observations[j].ObservedAt)
	})
	return observations
}

func (r *Runner) sampleResources(ctx context.Context, result *RunResult, mu *sync.Mutex) {
	ticker := time.NewTicker(r.config.Resources.SampleInterval.Value())
	defer ticker.Stop()
	for {
		sample, err := r.sampler.Sample(ctx)
		mu.Lock()
		if err != nil {
			if ctx.Err() == nil {
				result.Warnings = append(result.Warnings, "resource sample: "+err.Error())
			}
		} else {
			result.ResourceSamples = append(result.ResourceSamples, sample)
		}
		mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) destinationURL() string {
	return strings.TrimRight(r.config.Receiver.PublicURL, "/") + "/" + strings.TrimLeft(r.config.Receiver.Path, "/")
}

func terminalStatus(status string) bool {
	switch strings.ToLower(status) {
	case "delivered", "cancelled", "dead":
		return true
	default:
		return false
	}
}

func waitUntil(ctx context.Context, at time.Time) error {
	return waitContext(ctx, time.Until(at))
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
