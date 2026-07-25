package observability

import (
	"bytes"
	"encoding/json"
	"net/url"
	"sync"
	"testing"

	"github.com/defermq/defermq/internal/buildinfo"
	"go.uber.org/zap"
)

func TestNewLoggerWritesStructuredContext(t *testing.T) {
	sink := &memorySink{}
	if err := zap.RegisterSink("defermqtest", func(*url.URL) (zap.Sink, error) { return sink, nil }); err != nil {
		t.Fatalf("RegisterSink() error = %v", err)
	}
	logger, err := NewLoggerWithOptions(LoggerOptions{
		Service:          "defermq-gateway",
		InstanceID:       "gateway-test",
		Level:            "info",
		Format:           "json",
		StacktraceLevel:  "error",
		SamplingEnabled:  false,
		OutputPaths:      []string{"defermqtest://logger"},
		ErrorOutputPaths: []string{"defermqtest://logger"},
		Build:            buildinfo.Info{Version: "v1.0.0", Commit: "abcdef"},
	})
	if err != nil {
		t.Fatalf("NewLoggerWithOptions() error = %v", err)
	}
	logger.Info("started", zap.String("operation", "startup"))
	if err := Sync(logger); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	body := sink.Bytes()
	var entry map[string]any
	if err := json.Unmarshal(body, &entry); err != nil {
		t.Fatalf("log is not JSON: %v; body=%s", err, body)
	}
	for key, want := range map[string]string{
		"service": "defermq-gateway", "instance_id": "gateway-test", "version": "v1.0.0",
		"commit": "abcdef", "operation": "startup", "msg": "started",
	} {
		if got := entry[key]; got != want {
			t.Fatalf("%s = %v, want %q", key, got, want)
		}
	}
	if _, ok := entry["timestamp"]; !ok {
		t.Fatal("timestamp field is missing")
	}
}

type memorySink struct {
	mu sync.Mutex
	bytes.Buffer
}

func (s *memorySink) Write(body []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Buffer.Write(body)
}

func (s *memorySink) Sync() error  { return nil }
func (s *memorySink) Close() error { return nil }

func (s *memorySink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.Buffer.Bytes()...)
}

func TestNewLoggerRejectsInvalidOptions(t *testing.T) {
	_, err := NewLoggerWithOptions(LoggerOptions{
		Service: "service", InstanceID: "instance", Level: "verbose",
		Format: "json", StacktraceLevel: "error",
	})
	if err == nil {
		t.Fatal("invalid log level should fail")
	}

	_, err = NewLoggerWithOptions(LoggerOptions{
		Service: "service", InstanceID: "instance", Level: "info",
		Format: "text", StacktraceLevel: "error",
	})
	if err == nil {
		t.Fatal("invalid log format should fail")
	}
}
