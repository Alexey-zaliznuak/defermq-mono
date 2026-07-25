package postgresmanager

import (
	"strings"
	"testing"
)

func TestMarkOutboxPublishedBatchQueryLocksRowsDeterministically(t *testing.T) {
	assertSQLFragmentsInOrder(t, markOutboxPublishedBatchQuery,
		"locked_outbox AS MATERIALIZED",
		"ORDER BY o.id",
		"FOR UPDATE OF o",
		"UPDATE nats_outbox AS o",
		"locked_deliveries AS MATERIALIZED",
		"ORDER BY d.id",
		"FOR UPDATE OF d",
		"UPDATE deliveries AS d",
	)
	assertSQLContains(t, markOutboxPublishedBatchQuery,
		"FROM locked_outbox",
		"FROM delivery_changes AS changes, locked_deliveries",
	)
}

func TestMarkReadyPublishedQueryLocksRowsDeterministically(t *testing.T) {
	assertSQLFragmentsInOrder(t, markReadyPublishedQuery,
		"requested AS MATERIALIZED",
		"locked_deliveries AS MATERIALIZED",
		"ORDER BY d.id",
		"FOR UPDATE OF d",
		"UPDATE deliveries AS d",
	)
	assertSQLContains(t, markReadyPublishedQuery,
		"FROM requested, locked_deliveries",
	)
}

func assertSQLContains(t *testing.T, query string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(query, fragment) {
			t.Errorf("query does not contain %q", fragment)
		}
	}
}

func assertSQLFragmentsInOrder(t *testing.T, query string, fragments ...string) {
	t.Helper()
	offset := 0
	for _, fragment := range fragments {
		index := strings.Index(query[offset:], fragment)
		if index < 0 {
			t.Fatalf("query does not contain %q after byte %d", fragment, offset)
		}
		offset += index + len(fragment)
	}
}
