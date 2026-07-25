package loadtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeCommandResult struct {
	output string
	err    error
}

type fakeCommandExecutor struct {
	mu      sync.Mutex
	results []fakeCommandResult
	calls   [][]string
}

func (f *fakeCommandExecutor) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]string{name}, args...))
	if len(f.results) == 0 {
		return nil, errors.New("unexpected command")
	}
	result := f.results[0]
	f.results = f.results[1:]
	return []byte(result.output), result.err
}

func TestSamplerUsesComposeFilterAndAggregatesConfiguredServices(t *testing.T) {
	executor := &fakeCommandExecutor{results: []fakeCommandResult{
		{output: "abc123\tgateway\ndef456\tpostgres\nzzz999\tunconfigured\n"},
		{output: strings.Join([]string{
			`{"ID":"abc123","Name":"gateway-1","CPUPerc":"2.5%","MemUsage":"10MiB / 1GiB","NetIO":"1kB / 2kB","BlockIO":"3kB / 4kB","PIDs":"5"}`,
			`{"ID":"def456","Name":"postgres-1","CPUPerc":"7.5%","MemUsage":"20MB / 1GB","NetIO":"5kB / 6kB","BlockIO":"7kB / 8kB","PIDs":"9"}`,
		}, "\n")},
	}}
	config := ResourceConfig{
		Enabled: true, SampleInterval: Duration(time.Second), CommandTimeout: Duration(time.Second),
		ComposeProject: "sample-project", GoServices: []string{"gateway"}, NonGoServices: []string{"postgres"},
	}
	sampler := NewDockerResourceSampler(config, executor)
	sample, err := sampler.sample(context.Background(), time.Unix(123, 0))
	if err != nil {
		t.Fatal(err)
	}

	if len(sample.Containers) != 2 || sample.Groups[resourceGroupGo].CPUPercent != 2.5 ||
		sample.Groups[resourceGroupNonGo].CPUPercent != 7.5 ||
		sample.Groups[resourceGroupAll].CPUPercent != 10 {
		t.Fatalf("unexpected aggregation: %+v", sample)
	}
	if sample.Groups[resourceGroupAll].PIDs != 14 ||
		sample.Groups[resourceGroupAll].MemoryBytes != 10*1024*1024+20e6 {
		t.Fatalf("unexpected all group: %+v", sample.Groups[resourceGroupAll])
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()
	if got := strings.Join(executor.calls[0], " "); !strings.Contains(got,
		"docker ps --filter label=com.docker.compose.project=sample-project") {
		t.Fatalf("unexpected docker ps command: %s", got)
	}
	stats := executor.calls[1]
	if strings.Join(stats[:5], " ") != "docker stats --no-stream --format {{json .}}" {
		t.Fatalf("unexpected docker stats command: %v", stats)
	}
	if strings.Contains(strings.Join(stats, " "), "zzz999") {
		t.Fatalf("unconfigured service was passed to stats: %v", stats)
	}
}

func TestSamplerKeepsRunningAfterSamplingError(t *testing.T) {
	executor := &fakeCommandExecutor{results: []fakeCommandResult{
		{err: errors.New("Docker temporarily unavailable")},
		{output: "abc123\tgateway\n"},
		{output: `{"ID":"abc123","Name":"gateway-1","CPUPerc":"1%","MemUsage":"1MiB / 1GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B","PIDs":"1"}`},
	}}
	config := ResourceConfig{
		Enabled: true, SampleInterval: Duration(5 * time.Millisecond), CommandTimeout: Duration(time.Second),
		ComposeProject: "project", GoServices: []string{"gateway"},
	}
	sampler := NewDockerResourceSampler(config, executor)
	ctx, cancel := context.WithCancel(context.Background())
	if err := sampler.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for len(sampler.Samples()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	samples, errs := sampler.Stop()
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "temporarily unavailable") {
		t.Fatalf("unexpected sampling errors: %v", errs)
	}
	if len(sampler.Warnings()) != 1 {
		t.Fatalf("warnings = %v", sampler.Warnings())
	}
}

type timeoutExecutor struct {
	sawDeadline bool
}

func (e *timeoutExecutor) Run(ctx context.Context, _ string, _ ...string) ([]byte, error) {
	_, e.sawDeadline = ctx.Deadline()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSamplerAppliesCommandTimeout(t *testing.T) {
	executor := &timeoutExecutor{}
	config := ResourceConfig{
		Enabled: true, CommandTimeout: Duration(5 * time.Millisecond), ComposeProject: "project",
	}
	sampler := NewDockerResourceSampler(config, executor)
	started := time.Now()
	_, err := sampler.sample(context.Background(), time.Now())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if !executor.sawDeadline || time.Since(started) > time.Second {
		t.Fatalf("command timeout was not applied")
	}
}
