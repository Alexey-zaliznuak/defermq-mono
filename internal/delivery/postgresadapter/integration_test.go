package postgresadapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestIntegrationIdempotentInsert(t *testing.T) {
	dsn := os.Getenv("DEFERMQ_TEST_TARGET_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DEFERMQ_TEST_TARGET_POSTGRES_DSN to run target PostgreSQL integration test")
	}
	table := "defermq_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adapter, err := New(ctx, Config{
		DSN: dsn, Table: table, AutoCreateTable: true, MaxConns: 2, QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	defer adapter.pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+pgx.Identifier{table}.Sanitize())

	id := uuid.New()
	request := delivery.PushRequest{
		DeliveryID:  id,
		ScheduledAt: time.Now().UTC(),
		Destination: domain.Destination{
			Type:     domain.DestinationPostgres,
			Postgres: &domain.PostgresDestination{Channel: "test"},
		},
		Payload: []byte("payload"),
	}
	if err := adapter.Push(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Push(ctx, request); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := adapter.pool.QueryRow(ctx,
		`SELECT count(*) FROM `+pgx.Identifier{table}.Sanitize()+` WHERE delivery_id=$1`, id,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}
}
