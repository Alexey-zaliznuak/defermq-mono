package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsForEveryService(t *testing.T) {
	for _, service := range []Service{ServiceGateway, ServiceManager, ServicePusher} {
		t.Run(string(service), func(t *testing.T) {
			cfg, err := LoadFromLookup(service, lookup(map[string]string{
				"DEFERMQ_POSTGRES_DSN": "postgres://user:secret@localhost:5432/defermq?sslmode=disable",
				"DEFERMQ_INSTANCE_ID":  "test-instance",
			}))
			if err != nil {
				t.Fatalf("LoadFromLookup() error = %v", err)
			}
			if cfg.Common.ServiceName != "defermq-"+string(service) {
				t.Fatalf("unexpected service name %q", cfg.Common.ServiceName)
			}
			if cfg.Common.HotHorizon != 2*time.Minute || cfg.Common.MaxPayloadBytes != 1<<20 {
				t.Fatalf("common defaults were not applied: %+v", cfg.Common)
			}
			if cfg.Manager.PromoterBatchSize != 1000 || cfg.Pusher.WorkersHTTP != 32 {
				t.Fatal("service defaults were not fully populated")
			}
		})
	}
}

func TestLoadParsesTypedValuesAndLists(t *testing.T) {
	cfg, err := LoadFromLookup(ServicePusher, lookup(map[string]string{
		"DEFERMQ_POSTGRES_DSN":            "postgres://localhost/defermq",
		"DEFERMQ_INSTANCE_ID":             "pusher-1",
		"DEFERMQ_HOT_HORIZON":             "45s",
		"DEFERMQ_POSTGRES_MAX_CONNS":      "42",
		"DEFERMQ_PUSHER_RETRY_MULTIPLIER": "1.5",
		"DEFERMQ_ENABLED_DESTINATIONS":    "http, kafka,http",
		"DEFERMQ_HTTP_ALLOWED_HOSTS":      "example.com, api.example.com",
		"DEFERMQ_LOG_SAMPLING_ENABLED":    "false",
	}))
	if err != nil {
		t.Fatalf("LoadFromLookup() error = %v", err)
	}
	if cfg.Common.HotHorizon != 45*time.Second || cfg.Postgres.MaxConns != 42 || cfg.Pusher.RetryMultiplier != 1.5 {
		t.Fatal("typed values were not parsed")
	}
	if got := strings.Join(cfg.Common.EnabledDestinations, ","); got != "http,kafka" {
		t.Fatalf("unexpected destinations %q", got)
	}
	if cfg.Common.Log.SamplingEnabled {
		t.Fatal("sampling should be disabled")
	}
}

func TestLoadRejectsParseAndValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "bad duration",
			env: map[string]string{
				"DEFERMQ_POSTGRES_DSN": "postgres://localhost/defermq",
				"DEFERMQ_HOT_HORIZON":  "soon",
			},
			want: "DEFERMQ_HOT_HORIZON",
		},
		{
			name: "missing source dsn",
			env:  map[string]string{},
			want: "DEFERMQ_POSTGRES_DSN is required",
		},
		{
			name: "conflicting nats authentication",
			env: map[string]string{
				"DEFERMQ_POSTGRES_DSN":    "postgres://localhost/defermq",
				"DEFERMQ_NATS_USER":       "user",
				"DEFERMQ_NATS_PASSWORD":   "password",
				"DEFERMQ_NATS_CREDS_FILE": "/run/secrets/nats.creds",
			},
			want: "cannot be combined",
		},
		{
			name: "unsafe table identifier",
			env: map[string]string{
				"DEFERMQ_POSTGRES_DSN":          "postgres://localhost/defermq",
				"DEFERMQ_TARGET_POSTGRES_TABLE": "messages;DROP TABLE deliveries",
			},
			want: "simple SQL identifier",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadFromLookup(ServiceManager, lookup(test.env))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func lookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
