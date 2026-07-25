package loadtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigFile(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "loadtest.example.yml")
	config, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile() error = %v", err)
	}
	if len(config.Groups) != 3 {
		t.Fatalf("message groups = %d, want 3", len(config.Groups))
	}
	if got := config.Groups[0].DeliveryDelay.Mean.Value(); got != 30*time.Second {
		t.Fatalf("delivery mean = %s, want 30s", got)
	}
	if got := config.Groups[2].RescheduleDelay.Mean.Value(); got != -90*time.Second {
		t.Fatalf("negative reschedule mean = %s, want -90s", got)
	}
}

func TestLoad1000RPSConfig(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "loadtest.1000rps.yml")
	config, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile() error = %v", err)
	}
	total := 0
	for _, group := range config.Groups {
		total += group.Count
		if group.AdmissionOffset.Kind != "uniform" {
			t.Fatalf("group %q admission kind = %q, want uniform", group.Name, group.AdmissionOffset.Kind)
		}
	}
	if total != 120000 {
		t.Fatalf("message count = %d, want 120000", total)
	}
}

func TestLoadConfigRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	data := `
gateway:
  url: http://localhost:8080
receiver:
  public_url: http://localhost:18080
load:
  unknown_setting: true
message_groups:
  - name: one
    count: 1
    payload_bytes: 1
    max_attempts: 1
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfigFile(path)
	if err == nil || !strings.Contains(err.Error(), "unknown_setting") {
		t.Fatalf("error = %v, want unknown field error", err)
	}
}

func TestConfigValidationRejectsInvalidFractions(t *testing.T) {
	config := validTestConfig()
	config.Groups[0].CancelFraction = 0.7
	config.Groups[0].RescheduleFraction = 0.4
	if err := config.Validate(); err == nil {
		t.Fatal("Validate() accepted fractions greater than one")
	}
}

func validTestConfig() Config {
	return Config{
		Gateway:  GatewayConfig{URL: "http://localhost:8080", Timeout: Duration(time.Second)},
		Receiver: ReceiverConfig{PublicURL: "http://localhost:18080"},
		Load: LoadConfig{
			CreateConcurrency: 1, StatusConcurrency: 1, PollInterval: Duration(time.Millisecond),
			AwaitTimeout: Duration(time.Second),
		},
		Resources: ResourceConfig{
			SampleInterval: Duration(time.Second), CommandTimeout: Duration(time.Second),
		},
		Groups: []MessageGroup{{
			Name: "one", Count: 1, PayloadBytes: 1, MaxAttempts: 1,
		}},
	}
}
