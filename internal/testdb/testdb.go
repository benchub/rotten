// Package testdb starts real Postgres containers for tests.
//
// Everything here is an integration test helper: callers skip under -short.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ObservedVersions are the Postgres major versions rotten supports observing.
var ObservedVersions = []int{14, 15, 16, 17, 18}

// RottenRoles are the roles schema/tables.sql grants to.
var RottenRoles = []string{"rotten-client", "readonly", "readwrite", "rotten-interface"}

// DB is a running Postgres container.
type DB struct {
	// DSN connects as the postgres superuser.
	DSN string
}

// Connect opens a superuser connection and closes it at test cleanup.
func (d *DB) Connect(t testing.TB) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), d.DSN)
	if err != nil {
		t.Fatalf("testdb: connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// DSNAs returns the DSN for logging in as role, whose password is the role
// name.
func (d *DB) DSNAs(t testing.TB, role string) string {
	t.Helper()
	u, err := url.Parse(d.DSN)
	if err != nil {
		t.Fatalf("testdb: parse DSN: %v", err)
	}
	u.User = url.UserPassword(role, role)
	return u.String()
}

// RottenImage is the Postgres 18 + pg_partman image `make image` builds from
// docker/rotten-db.Dockerfile.
const RottenImage = "rotten-db-test:18"

// RepoRoot returns the repository root, found from this source file.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func skipShort(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("testdb: integration test skipped under -short")
	}
}

func start(t testing.TB, image, dbName string, extra ...testcontainers.ContainerCustomizer) *DB {
	t.Helper()
	ctx := context.Background()
	opts := append([]testcontainers.ContainerCustomizer{
		postgres.WithDatabase(dbName),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(3 * time.Minute)),
	}, extra...)
	c, err := postgres.Run(ctx, image, opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		if image == RottenImage {
			t.Fatalf("testdb: start %s: %v (is the image built? run `make image`)", image, err)
		}
		t.Fatalf("testdb: start %s: %v", image, err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("testdb: connection string: %v", err)
	}
	return &DB{DSN: dsn}
}

// StartRotten starts Postgres 18 with pg_partman installed in public, a
// database named rotten, and the roles in RottenRoles (LOGIN, password =
// role name; see DSNAs). It uses RottenImage, which `make image` builds. It doesn't load the
// schema.
func StartRotten(t testing.TB) *DB {
	t.Helper()
	skipShort(t)
	db := start(t, RottenImage, "rotten")
	conn := db.Connect(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "create extension pg_partman schema public"); err != nil {
		t.Fatalf("testdb: create pg_partman: %v", err)
	}
	for _, r := range RottenRoles {
		// The password is interpolated as a literal, so role names must not
		// contain single quotes.
		if _, err := conn.Exec(ctx, fmt.Sprintf("create role %s login password '%s'", pgx.Identifier{r}.Sanitize(), r)); err != nil {
			t.Fatalf("testdb: create role %s: %v", r, err)
		}
	}
	return db
}

// StartObserved starts an observed Postgres (14 through 18) with
// pg_stat_statements preloaded and created, and track_planning on.
func StartObserved(t testing.TB, version int) *DB {
	t.Helper()
	skipShort(t)
	if version < 14 || version > 18 {
		t.Fatalf("testdb: unsupported Postgres version %d", version)
	}
	db := start(t, fmt.Sprintf("postgres:%d", version), "observed",
		testcontainers.WithCmdArgs(
			"-c", "shared_preload_libraries=pg_stat_statements",
			"-c", "pg_stat_statements.track_planning=on"))
	conn := db.Connect(t)
	if _, err := conn.Exec(context.Background(), "create extension pg_stat_statements"); err != nil {
		t.Fatalf("testdb: create pg_stat_statements: %v", err)
	}
	return db
}
