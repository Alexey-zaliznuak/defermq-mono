package postgresadapter

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIdentifierValidation(t *testing.T) {
	for _, value := range []string{"messages", "_messages2", "DeferMQ"} {
		if !identifierPattern.MatchString(value) {
			t.Errorf("%q should be valid", value)
		}
	}
	for _, value := range []string{"public.messages", `messages; DROP TABLE x`, `"quoted"`, ""} {
		if identifierPattern.MatchString(value) {
			t.Errorf("%q should be invalid", value)
		}
	}
}

func TestPostgresErrorClassification(t *testing.T) {
	if !retryable(&pgconn.PgError{Code: "40001"}) {
		t.Fatal("serialization failure should be retryable")
	}
	if !retryable(&pgconn.PgError{Code: "08006"}) {
		t.Fatal("connection failure should be retryable")
	}
	if retryable(&pgconn.PgError{Code: "42P01"}) {
		t.Fatal("undefined table should not be retryable")
	}
	if !retryable(errors.New("transport closed")) {
		t.Fatal("unknown transport errors should be retryable")
	}
}
