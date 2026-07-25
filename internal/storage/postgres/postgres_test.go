package postgres

import (
	"errors"
	"testing"
	"testing/fstest"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPoolConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PoolConfig
		wantErr bool
	}{
		{name: "valid", cfg: PoolConfig{DSN: "postgres://localhost/db", MinConns: 1, MaxConns: 2}},
		{name: "missing DSN", cfg: PoolConfig{}, wantErr: true},
		{name: "negative limit", cfg: PoolConfig{DSN: "x", MaxConns: -1}, wantErr: true},
		{name: "min exceeds max", cfg: PoolConfig{DSN: "x", MinConns: 3, MaxConns: 2}, wantErr: true},
		{name: "negative timeout", cfg: PoolConfig{DSN: "x", QueryTimeout: -time.Second}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Validate(); (got != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", got, tt.wantErr)
			}
		})
	}
}

func TestOutboxKindFor(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		deliverAt time.Time
		horizon   time.Duration
		want      OutboxKind
		wantOK    bool
	}{
		{name: "overdue is ready", deliverAt: now.Add(-time.Second), horizon: time.Minute, want: OutboxReady, wantOK: true},
		{name: "exactly now is ready", deliverAt: now, horizon: time.Minute, want: OutboxReady, wantOK: true},
		{name: "horizon boundary is hot register", deliverAt: now.Add(time.Minute), horizon: time.Minute, want: OutboxHotRegister, wantOK: true},
		{name: "outside horizon has no outbox", deliverAt: now.Add(time.Minute + time.Nanosecond), horizon: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := outboxKindFor(tt.deliverAt, now, tt.horizon)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("outboxKindFor() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestValidateCreate(t *testing.T) {
	payloadID := uuid.New()
	valid := CreateDeliveryParams{
		Delivery: domain.Delivery{
			ID:              uuid.New(),
			PayloadID:       payloadID,
			DestinationType: domain.DestinationHTTP,
			Destination:     []byte(`{"type":"http"}`),
			DeliverAt:       time.Now(),
			MaxAttempts:     3,
		},
		Payload: domain.Payload{
			ID: payloadID, Body: []byte("abc"), ContentType: "text/plain", SizeBytes: 3,
		},
	}
	if err := validateCreate(valid); err != nil {
		t.Fatalf("valid create rejected: %v", err)
	}
	invalid := valid
	invalid.Payload.SizeBytes = 2
	if err := validateCreate(invalid); err == nil {
		t.Fatal("mismatched payload size was accepted")
	}
}

func TestLoadMigrations(t *testing.T) {
	source := fstest.MapFS{
		"migrations/00002_second.sql": {Data: []byte("SELECT 2;")},
		"migrations/00001_first.sql":  {Data: []byte("SELECT 1;")},
	}
	got, err := loadMigrations(source)
	if err != nil {
		t.Fatalf("loadMigrations() error: %v", err)
	}
	if len(got) != 2 || got[0].version != 1 || got[1].version != 2 {
		t.Fatalf("migrations not sorted by version: %#v", got)
	}
	if got[0].checksum == "" || got[0].checksum == got[1].checksum {
		t.Fatal("migration checksums are missing or equal")
	}
}

func TestLoadMigrationsRejectsDuplicateVersion(t *testing.T) {
	_, err := loadMigrations(fstest.MapFS{
		"migrations/00001_a.sql": {Data: []byte("SELECT 1;")},
		"migrations/00001_b.sql": {Data: []byte("SELECT 2;")},
	})
	if err == nil {
		t.Fatal("duplicate migration version was accepted")
	}
}

func TestErrorMapping(t *testing.T) {
	original := errors.New("original")
	if mapError(original) != original {
		t.Fatal("mapError did not preserve arbitrary error")
	}
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("unique violation was not recognized")
	}
	if isUniqueViolation(&pgconn.PgError{Code: "23503"}) {
		t.Fatal("foreign key violation classified as unique")
	}
}
