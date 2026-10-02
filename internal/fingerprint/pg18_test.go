package fingerprint

import (
	"context"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

// The golden file alone could pin errors for invalid SQL. Execute each
// PG18 case to prove it's supported syntax, not a malformed fixture.
func TestPostgres18CorpusSyntax(t *testing.T) {
	db := testdb.StartObserved(t, 18)
	conn := db.Connect(t)
	ctx := context.Background()
	for _, q := range []string{
		"create extension btree_gist",
		"create table users (id int, name text)",
		"insert into users values (1, 'original')",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, c := range loadCorpus(t) {
		if !strings.HasPrefix(c.name, "pg18_") {
			continue
		}
		n++
		t.Run(c.name, func(t *testing.T) {
			if _, err := conn.Exec(ctx, c.query); err != nil {
				t.Fatalf("Postgres 18 rejected corpus query: %v", err)
			}
		})
	}
	if n != 6 {
		t.Fatalf("want 6 Postgres 18 syntax cases, got %d", n)
	}
}
