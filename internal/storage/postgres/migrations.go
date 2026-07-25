package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const migrationLockID int64 = 0x44454645524d51

type migration struct {
	version  int64
	name     string
	sql      string
	checksum string
}

// Migrate applies embedded, immutable up migrations under a PostgreSQL
// advisory lock. Checksums prevent a deployed migration from being silently
// rewritten. The canonical root migrations are intentionally duplicated in
// this package because Go embed cannot reference parent directories.
func (s *Store) Migrate(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	migrations, err := loadMigrations(migrationFiles)
	if err != nil {
		return err
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.WithoutCancel(ctx), s.queryTimeout)
		defer unlockCancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS defermq_schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return fmt.Errorf("create migration metadata: %w", err)
	}
	for _, m := range migrations {
		var checksum string
		err := conn.QueryRow(ctx,
			`SELECT checksum FROM defermq_schema_migrations WHERE version = $1`,
			m.version).Scan(&checksum)
		switch {
		case err == nil:
			if checksum != m.checksum {
				return fmt.Errorf("migration %d checksum mismatch", m.version)
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("read migration %d state: %w", m.version, err)
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.version, err)
		}
		if _, err = tx.Exec(ctx, m.sql); err == nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO defermq_schema_migrations (version, name, checksum)
				VALUES ($1, $2, $3)`, m.version, m.name, m.checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.version, err)
		}
	}
	return nil
}

func loadMigrations(source fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(source, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	result := make([]migration, 0, len(entries))
	seen := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".sql" {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in %q", entry.Name())
		}
		if _, duplicate := seen[version]; duplicate {
			return nil, fmt.Errorf("duplicate migration version %d", version)
		}
		seen[version] = struct{}{}
		body, err := fs.ReadFile(source, path.Join("migrations", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(body)
		result = append(result, migration{
			version:  version,
			name:     entry.Name(),
			sql:      string(body),
			checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	if len(result) == 0 {
		return nil, errors.New("no embedded migrations found")
	}
	return result, nil
}

func (s *Store) SchemaReady(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var ready bool
	err := s.pool.QueryRow(ctx, `
		SELECT to_regclass('deliveries') IS NOT NULL
		   AND to_regclass('message_payloads') IS NOT NULL
		   AND to_regclass('nats_outbox') IS NOT NULL`).Scan(&ready)
	if err != nil {
		return fmt.Errorf("check postgres schema: %w", err)
	}
	if !ready {
		return errors.New("defermq postgres schema is not installed")
	}
	return nil
}
