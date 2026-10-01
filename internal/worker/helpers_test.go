package worker

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/internal/identity"
	"github.com/benchub/rotten/internal/testdb"
)

// The sample regexes from the descriptions in conf.
const (
	sampleController = `/\*.*controller(_with_namespace)?:([^,]+).*\*/`
	sampleAction     = `/\*.*action:([^,]+).*\*/`
	sampleJob        = `/\*.*job(_tag)?:([^,]+).*\*/`
)

// startIdentityDB starts the rotten DB, loads the schema, and returns a pool.
func startIdentityDB(t *testing.T) (*testdb.DB, *pgxpool.Pool) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test skipped under -short")
	}
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	sql, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "schema", "tables.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), string(sql)); err != nil {
		t.Fatalf("load schema: %v", err)
	}
	// New connections pick up the database's search_path, which includes
	// rotten, the same way production sessions do.
	pool, err := pgxpool.New(context.Background(), db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return db, pool
}

func sampleRegexes(t *testing.T) (c, a, j *regexp.Regexp) {
	t.Helper()
	c, a, j, err := identity.CompileRegexes(sampleController, sampleAction, sampleJob)
	if err != nil {
		t.Fatal(err)
	}
	return c, a, j
}

func count(t *testing.T, pool *pgxpool.Pool, q string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}
