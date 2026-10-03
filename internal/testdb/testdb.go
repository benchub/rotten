// Package testdb starts real Postgres containers for tests.
//
// Everything here is an integration test helper: callers skip under -short.
package testdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/migrate"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ObservedVersions are the Postgres major versions rotten supports observing.
var ObservedVersions = []int{14, 15, 16, 17, 18}

// OwnerRole owns the rotten database and runs migrate.
const OwnerRole = "rotten_owner"

// The roles migrations/permissions.sql grants to. An operator creates them
// in production; StartRottenEmpty creates them for tests.
const (
	IngestRole   = "rotten_ingest"
	UIRole       = "rotten_ui"
	ReadonlyRole = "rotten_readonly"
)

// RottenRoles are the login roles StartRottenEmpty creates.
var RottenRoles = []string{OwnerRole, IngestRole, UIRole, ReadonlyRole}

// ConnectAs opens a connection logged in as role and closes it at test
// cleanup.
func (d *DB) ConnectAs(t testing.TB, role string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), d.DSNAs(t, role))
	if err != nil {
		t.Fatalf("testdb: connect as %s: %v", role, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// DB is a running Postgres container.
type DB struct {
	// DSN connects as the postgres superuser.
	DSN string

	c      testcontainers.Container
	dbName string
}

// Container returns the underlying testcontainers container for tests that
// need Docker-network details or container control.
func (d *DB) Container() testcontainers.Container {
	return d.c
}

// Stop terminates the database container early. Cleanup still tolerates the
// already-stopped container.
func (d *DB) Stop(t testing.TB) {
	t.Helper()
	if err := d.c.Terminate(context.Background()); err != nil {
		t.Fatalf("testdb: stop container: %v", err)
	}
}

// Restart stops and starts the same database container, runs whileStopped
// after the stop and before the start, then refreshes the host DSN in case
// Docker assigns a new mapped port.
func (d *DB) Restart(t testing.TB, whileStopped ...func()) {
	t.Helper()
	ctx := context.Background()
	timeout := 10 * time.Second
	if err := d.c.Stop(ctx, &timeout); err != nil {
		t.Fatalf("testdb: stop container for restart: %v", err)
	}
	for _, fn := range whileStopped {
		fn()
	}
	if err := d.c.Start(ctx); err != nil {
		t.Fatalf("testdb: start container after restart: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		host, err := d.c.Host(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		port, err := d.c.MappedPort(ctx, "5432/tcp")
		if err == nil {
			u := url.URL{
				Scheme:   "postgres",
				User:     url.UserPassword("postgres", "postgres"),
				Host:     net.JoinHostPort(host, port.Port()),
				Path:     d.dbName,
				RawQuery: "sslmode=disable",
			}
			d.DSN = u.String()
			conn, err := pgx.Connect(ctx, d.DSN)
			if err == nil {
				conn.Close(ctx)
				return
			}
			lastErr = err
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("testdb: restarted container did not become ready: %v", lastErr)
}

// Connect opens a superuser connection and closes it at test cleanup.
func (d *DB) Connect(t testing.TB) *pgx.Conn {
	t.Helper()
	if d.DSN == "" {
		t.Fatalf("testdb: database has no host DSN; use QueryInContainer or an internal network DSN for containers without published ports")
	}
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
	if d.DSN == "" {
		t.Fatalf("testdb: database has no host DSN; use QueryInContainer or an internal network DSN for containers without published ports")
	}
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
		testcontainers.WithExposedPorts("5432/tcp"),
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
	return &DB{DSN: dsn, c: c, dbName: dbName}
}

func startNoHostDSN(t testing.TB, image, dbName string, extra ...testcontainers.ContainerCustomizer) *DB {
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
	return &DB{c: c, dbName: dbName}
}

// StartRotten is StartRottenEmpty plus migrate.Up, run as OwnerRole, so the
// database has the latest schema.
func StartRotten(t testing.TB) *DB {
	t.Helper()
	db := StartRottenEmpty(t)
	if _, err := migrate.Up(context.Background(), db.DSNAs(t, OwnerRole)); err != nil {
		t.Fatalf("testdb: migrate: %v", err)
	}
	return db
}

// StartRottenEmpty starts Postgres 18 with pg_partman installed in public, a
// database named rotten owned by OwnerRole, and the roles in RottenRoles
// (LOGIN, password = role name; see DSNAs). OwnerRole gets what pg_partman
// needs to create partitions. It uses RottenImage, which `make image` builds.
// It doesn't load the schema.
func StartRottenEmpty(t testing.TB) *DB {
	t.Helper()
	skipShort(t)
	db := start(t, RottenImage, "rotten")
	initializeRottenEmpty(t, db)
	return db
}

func initializeRottenEmpty(t testing.TB, db *DB) {
	t.Helper()
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
	owner := pgx.Identifier{OwnerRole}.Sanitize()
	for _, q := range []string{
		"alter database rotten owner to " + owner,
		// pg_partman's non-superuser setup, with the extension in public.
		"grant all on all tables in schema public to " + owner,
		"grant all on all sequences in schema public to " + owner,
		"grant execute on all functions in schema public to " + owner,
		"grant execute on all procedures in schema public to " + owner,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("testdb: %s: %v", q, err)
		}
	}
}

func initializeRottenEmptyInContainer(t testing.TB, db *DB) {
	t.Helper()
	var sql bytes.Buffer
	sql.WriteString("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA public;\n")
	for _, r := range RottenRoles {
		sql.WriteString(fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s';\n", pgx.Identifier{r}.Sanitize(), r))
	}
	owner := pgx.Identifier{OwnerRole}.Sanitize()
	for _, q := range []string{
		"ALTER DATABASE rotten OWNER TO " + owner,
		"GRANT ALL ON ALL TABLES IN SCHEMA public TO " + owner,
		"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO " + owner,
		"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO " + owner,
		"GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO " + owner,
	} {
		sql.WriteString(q)
		sql.WriteString(";\n")
	}
	execSQLInContainer(t, db, sql.String())
}

func migrateRottenInContainer(t testing.TB, db *DB) {
	t.Helper()
	bin := buildRottenServer(t)
	const dst = "/rotten-server"
	if err := db.c.CopyFileToContainer(context.Background(), bin, dst, 0o755); err != nil {
		t.Fatalf("testdb: copy rotten-server to container: %v", err)
	}
	code, r, err := db.c.Exec(context.Background(), []string{
		dst,
		"migrate",
		"-dsn",
		internalDSN(OwnerRole, OwnerRole, "localhost", db.dbName),
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("testdb: exec rotten-server migrate: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("testdb: rotten-server migrate exited %d: %s", code, out)
	}
}

func buildRottenServer(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "rotten-server")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/rotten-server")
	cmd.Dir = RepoRoot()
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("testdb: build rotten-server: %v\n%s", err, out)
	}
	return bin
}

func execSQLInContainer(t testing.TB, db *DB, sql string) {
	t.Helper()
	ctx := context.Background()
	const path = "/rotten-testdb.sql"
	if err := db.c.CopyToContainer(ctx, []byte(sql), path, 0o644); err != nil {
		t.Fatalf("testdb: copy SQL to container: %v", err)
	}
	code, r, err := db.c.Exec(ctx, []string{"psql", "-X", "-U", "postgres", "-d", db.dbName, "-v", "ON_ERROR_STOP=1", "-f", path}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("testdb: exec SQL in container: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("testdb: SQL in container exited %d: %s", code, out)
	}
}

// QueryInContainer runs query with psql inside the database container and
// returns unaligned, tuples-only rows split on a nonprinting field separator.
// It works for containers with no host-published Postgres port.
func (d *DB) QueryInContainer(t testing.TB, role string, query string) [][]string {
	t.Helper()
	password := role
	if role == "postgres" {
		password = "postgres"
	}
	const sep = "\x1f"
	code, r, err := d.c.Exec(context.Background(), []string{
		"psql",
		"-X",
		"-A",
		"-t",
		"-F", sep,
		internalDSN(role, password, "localhost", d.dbName),
		"-c", query,
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("testdb: query in container: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("testdb: query in container exited %d: %s", code, out)
	}
	text := strings.TrimSuffix(string(out), "\n")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	rows := make([][]string, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, strings.Split(line, sep))
	}
	return rows
}

// PSQL runs the SQL file at path (on the host) with psql inside the database
// container, as the postgres superuser, with ON_ERROR_STOP on. Each vars entry
// becomes a `-v name=value` psql variable. It returns psql's combined output,
// and an error if psql exits nonzero.
func (d *DB) PSQL(t testing.TB, path string, vars map[string]string) (string, error) {
	t.Helper()
	ctx := context.Background()
	const dst = "/tmp/rotten-psql.sql"
	if err := d.c.CopyFileToContainer(ctx, path, dst, 0o644); err != nil {
		t.Fatalf("testdb: copy %s: %v", path, err)
	}
	cmd := []string{"psql", "-X", "-U", "postgres", "-d", d.dbName, "-v", "ON_ERROR_STOP=1", "-f", dst}
	for k, v := range vars {
		cmd = append(cmd, "-v", k+"="+v)
	}
	code, r, err := d.c.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("testdb: exec psql: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		return string(out), fmt.Errorf("psql exited %d: %s", code, out)
	}
	return string(out), nil
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
