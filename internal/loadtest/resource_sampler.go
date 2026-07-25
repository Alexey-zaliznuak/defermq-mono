package loadtest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	resourceGroupGo    = "go"
	resourceGroupNonGo = "non_go"
	resourceGroupAll   = "all"
)

// CommandExecutor permits Docker command execution to be replaced in tests.
type CommandExecutor interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execCommandExecutor struct{}

func (execCommandExecutor) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

// DockerResourceSampler periodically collects resource usage for configured
// services in one Docker Compose project.
type DockerResourceSampler struct {
	config   ResourceConfig
	executor CommandExecutor

	mu      sync.RWMutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
	samples []ResourceSample
	errs    []error
}

func NewDockerResourceSampler(config ResourceConfig, executor CommandExecutor) *DockerResourceSampler {
	if executor == nil {
		executor = execCommandExecutor{}
	}
	return &DockerResourceSampler{config: config, executor: executor}
}

// Start begins sampling. Cancellation of ctx stops the sampler.
func (s *DockerResourceSampler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("Docker resource sampler is already started")
	}
	s.started = true
	s.done = make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	if !s.config.Enabled {
		close(s.done)
		return nil
	}
	go s.run(runCtx, s.done)
	return nil
}

// Run samples until ctx is cancelled and returns the collected result.
func (s *DockerResourceSampler) Run(ctx context.Context) ([]ResourceSample, []error) {
	if err := s.Start(ctx); err != nil {
		return s.Samples(), []error{err}
	}
	<-ctx.Done()
	return s.Stop()
}

// Stop stops sampling and waits for an in-flight Docker command to finish.
func (s *DockerResourceSampler) Stop() ([]ResourceSample, []error) {
	s.mu.RLock()
	cancel, done, started := s.cancel, s.done, s.started
	s.mu.RUnlock()
	if !started {
		return s.Samples(), s.Errors()
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return s.Samples(), s.Errors()
}

func (s *DockerResourceSampler) Samples() []ResourceSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]ResourceSample(nil), s.samples...)
}

func (s *DockerResourceSampler) Errors() []error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]error(nil), s.errs...)
}

func (s *DockerResourceSampler) Warnings() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	warnings := make([]string, len(s.errs))
	for index, err := range s.errs {
		warnings[index] = err.Error()
	}
	return warnings
}

// Sample performs one immediate measurement. It lets Runner own the sampling
// cadence while the Start/Run API remains available for standalone use.
func (s *DockerResourceSampler) Sample(ctx context.Context) (ResourceSample, error) {
	return s.sample(ctx, time.Now().UTC())
}

func (s *DockerResourceSampler) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.config.SampleInterval.Value())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-ticker.C:
			sample, err := s.sample(ctx, at)
			if err != nil {
				if ctx.Err() == nil {
					s.addError(err)
				}
				continue
			}
			s.mu.Lock()
			s.samples = append(s.samples, sample)
			s.mu.Unlock()
		}
	}
}

func (s *DockerResourceSampler) sample(ctx context.Context, at time.Time) (ResourceSample, error) {
	services, err := s.listContainers(ctx)
	if err != nil {
		return ResourceSample{}, err
	}
	if len(services) == 0 {
		return emptyResourceSample(at), nil
	}

	ids := make([]string, 0, len(services))
	for id := range services {
		ids = append(ids, id)
	}
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, ids...)
	output, err := s.runCommand(ctx, args...)
	if err != nil {
		return ResourceSample{}, fmt.Errorf("collect Docker stats: %w", err)
	}
	return buildResourceSample(at, output, services, s.config)
}

func (s *DockerResourceSampler) listContainers(ctx context.Context) (map[string]string, error) {
	label := "label=com.docker.compose.project=" + s.config.ComposeProject
	output, err := s.runCommand(ctx, "ps", "--filter", label, "--format",
		`{{.ID}}\t{{.Label "com.docker.compose.service"}}`)
	if err != nil {
		return nil, fmt.Errorf("list Compose containers: %w", err)
	}
	containers := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("parse docker ps row %q", scanner.Text())
		}
		service := strings.TrimSpace(parts[1])
		if classifyService(service, s.config) == "" {
			continue
		}
		containers[strings.TrimSpace(parts[0])] = service
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read docker ps output: %w", err)
	}
	return containers, nil
}

func (s *DockerResourceSampler) runCommand(ctx context.Context, args ...string) ([]byte, error) {
	commandCtx, cancel := context.WithTimeout(ctx, s.config.CommandTimeout.Value())
	defer cancel()
	return s.executor.Run(commandCtx, "docker", args...)
}

func (s *DockerResourceSampler) addError(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

func buildResourceSample(
	at time.Time,
	output []byte,
	services map[string]string,
	config ResourceConfig,
) (ResourceSample, error) {
	sample := emptyResourceSample(at)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		row, point, err := parseDockerStatsLine(line)
		if err != nil {
			return ResourceSample{}, err
		}
		service := serviceForContainer(row.ID, services)
		group := classifyService(service, config)
		if group == "" {
			continue
		}
		key := row.Name
		if key == "" {
			key = row.ID
		}
		sample.Containers[key] = containerResource(service, point)
		sample.Groups[group] = addResourcePoints(sample.Groups[group], point)
		sample.Groups[resourceGroupAll] = addResourcePoints(sample.Groups[resourceGroupAll], point)
	}
	if err := scanner.Err(); err != nil {
		return ResourceSample{}, fmt.Errorf("read docker stats output: %w", err)
	}
	return sample, nil
}

func emptyResourceSample(at time.Time) ResourceSample {
	return ResourceSample{
		At:         at,
		Containers: make(map[string]ContainerResource),
		Groups: map[string]ResourcePoint{
			resourceGroupGo:    {},
			resourceGroupNonGo: {},
			resourceGroupAll:   {},
		},
	}
}

func classifyService(service string, config ResourceConfig) string {
	if containsString(config.GoServices, service) {
		return resourceGroupGo
	}
	if containsString(config.NonGoServices, service) {
		return resourceGroupNonGo
	}
	return ""
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func serviceForContainer(id string, services map[string]string) string {
	if service, ok := services[id]; ok {
		return service
	}
	for listedID, service := range services {
		if strings.HasPrefix(id, listedID) || strings.HasPrefix(listedID, id) {
			return service
		}
	}
	return ""
}

func containerResource(service string, point ResourcePoint) ContainerResource {
	return ContainerResource{
		Service: service, CPUPercent: point.CPUPercent, MemoryBytes: point.MemoryBytes,
		PIDs: point.PIDs, NetRXBytes: point.NetRXBytes, NetTXBytes: point.NetTXBytes,
		BlockRead: point.BlockRead, BlockWrite: point.BlockWrite,
	}
}
