package fingerprint_test

// This test lives in fingerprint's external test package because it needs
// pgss and testdb as well as fingerprint. pgss doesn't import fingerprint
// today, but an external test package can't form an import cycle even if it
// does later, and it keeps those imports out of the fingerprint package.

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/testdb"
)

// crossTables names one table per logical query, so each pg_stat_statements
// row can be traced back to its logical query by the table it mentions.
var crossTables = []string{"x_in_lit", "x_in_param", "x_values_lit", "x_values_param"}

// runCrossWorkload runs each logical query with several list lengths and
// row counts, with literals (simple protocol) and with bind parameters.
func runCrossWorkload(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	for _, tbl := range crossTables {
		if _, err := conn.Exec(ctx, fmt.Sprintf("create table %s (id int, name text)", tbl)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, "select pg_stat_statements_reset()"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2, 3, 7, 20} {
		var lits, params, vlits, vparams []string
		var args, vargs []any
		for i := 1; i <= n; i++ {
			lits = append(lits, fmt.Sprint(i))
			params = append(params, fmt.Sprintf("$%d", i))
			args = append(args, i)
			vlits = append(vlits, fmt.Sprintf("(%d, 'n%d')", i, i))
			vparams = append(vparams, fmt.Sprintf("($%d::int, $%d::text)", 2*i-1, 2*i))
			vargs = append(vargs, i, fmt.Sprintf("n%d", i))
		}
		stmts := []struct {
			sql  string
			args []any
		}{
			{"select * from x_in_lit where id in (" + strings.Join(lits, ", ") + ")", nil},
			{"select * from x_in_param where id in (" + strings.Join(params, ", ") + ")", args},
			{"insert into x_values_lit (id, name) values " + strings.Join(vlits, ", "), nil},
			{"insert into x_values_param (id, name) values " + strings.Join(vparams, ", "), vargs},
		}
		for _, s := range stmts {
			if _, err := conn.Exec(ctx, s.sql, s.args...); err != nil {
				t.Fatalf("%s: %v", s.sql, err)
			}
		}
	}
}

// crossFingerprints runs the workload on one version and returns, per
// table, fingerprint -> the pg_stat_statements texts that produced it.
func crossFingerprints(t *testing.T, version int) map[string]map[string][]string {
	t.Helper()
	db := testdb.StartObserved(t, version)
	su := db.Connect(t)
	ctx := context.Background()
	const observer = "obs"
	path := filepath.Join(testdb.RepoRoot(), "schema", "observer.sql")
	if out, err := db.PSQL(t, path, map[string]string{"observer_role": observer, "observer_schema": "rotten"}); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	if _, err := su.Exec(ctx, fmt.Sprintf("alter role %s password '%s'", observer, observer)); err != nil {
		t.Fatal(err)
	}
	runCrossWorkload(t, su)

	obs, err := pgx.Connect(ctx, db.DSNAs(t, observer))
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close(ctx)
	stats, err := pgss.NewReader(obs).Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string][]string{}
	for _, s := range stats {
		for _, tbl := range crossTables {
			if !strings.Contains(s.Query, tbl+" ") || strings.HasPrefix(strings.ToLower(s.Query), "create") {
				continue
			}
			fp, err := fingerprint.Normalized(s.Query, fingerprint.Options{})
			if err != nil {
				fp = "error: " + err.Error()
			}
			if out[tbl] == nil {
				out[tbl] = map[string][]string{}
			}
			out[tbl][fp] = append(out[tbl][fp], s.Query)
		}
	}
	return out
}

// TestCrossVersionListFingerprints checks that Postgres 18's squashed
// IN-list text in pg_stat_statements fingerprints the same as the
// per-length texts that Postgres 16 records, for every list length.
func TestCrossVersionListFingerprints(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	byVersion := map[int]map[string]map[string][]string{}
	for _, v := range []int{16, 18} {
		byVersion[v] = crossFingerprints(t, v)
	}
	for _, tbl := range crossTables {
		all := map[string][]string{}
		for _, v := range []int{16, 18} {
			got := byVersion[v][tbl]
			if len(got) == 0 {
				t.Errorf("%s: pg%d recorded no statements", tbl, v)
			}
			for fp, qs := range got {
				for _, q := range qs {
					all[fp] = append(all[fp], fmt.Sprintf("pg%d: %s", v, q))
				}
			}
		}
		// Guard against a vacuous pass: pg18 must really squash the IN
		// lists (and, as of 18.0, leaves VALUES alone).
		squashed := false
		for _, qs := range byVersion[18][tbl] {
			for _, q := range qs {
				squashed = squashed || strings.Contains(q, "/*, ... */")
			}
		}
		if wantSquash := strings.HasPrefix(tbl, "x_in_"); squashed != wantSquash {
			t.Errorf("%s: pg18 squashed = %v, want %v: %v", tbl, squashed, wantSquash, byVersion[18][tbl])
		}
		if len(all) != 1 {
			var lines []string
			for fp, qs := range all {
				sort.Strings(qs)
				lines = append(lines, fmt.Sprintf("  %s\n    %s", fp, strings.Join(qs, "\n    ")))
			}
			sort.Strings(lines)
			t.Errorf("%s: want one fingerprint across pg16 and pg18, got %d:\n%s", tbl, len(all), strings.Join(lines, "\n"))
		}
	}
}
