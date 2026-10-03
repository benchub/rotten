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
var crossTables = []string{"x_in_lit", "x_in_param", "x_in_cast_param", "x_any_lit", "x_any_param", "x_any_cast_lit", "x_any_cast_wide_lit", "x_any_cast_param", "x_not_in_lit", "x_all_ne_lit", "x_not_in_sub", "x_all_ne_sub", "x_values_lit", "x_values_param"}

// crossAlias pairs a table whose query should group with another table's.
// The fingerprint is taken with the table name swapped for its alias, since
// table names are part of the fingerprint.
//
// The cast arrays group with the IN list too. With a matching cast (::int[]
// on an int column), Postgres gives them the IN list's queryid. With a
// widening cast (::bigint[]), Postgres gives them the queryid of
// IN (1::bigint, 2::bigint), and the fingerprint already ignores element
// casts in IN lists, so that's the IN list's fingerprint as well.
var crossAlias = map[string]string{
	"x_any_lit":           "x_in_lit",
	"x_any_param":         "x_in_cast_param",
	"x_any_cast_lit":      "x_in_lit",
	"x_any_cast_wide_lit": "x_in_lit",
	"x_any_cast_param":    "x_in_cast_param",
	"x_all_ne_lit":        "x_not_in_lit",
}

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
	if _, err := conn.Exec(ctx, "create table cross_accounts (user_id int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select pg_stat_statements_reset()"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2, 3, 7, 20} {
		var lits, params, castParams, vlits, vparams []string
		var args, vargs []any
		for i := 1; i <= n; i++ {
			lits = append(lits, fmt.Sprint(i))
			params = append(params, fmt.Sprintf("$%d", i))
			castParams = append(castParams, fmt.Sprintf("$%d::int", i))
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
			{"select * from x_any_lit where id = any(array[" + strings.Join(lits, ", ") + "])", nil},
			// Postgres can't infer a type for array[$1], so the parameter
			// forms cast each element, and the IN baseline casts too.
			{"select * from x_in_cast_param where id in (" + strings.Join(castParams, ", ") + ")", args},
			{"select * from x_any_param where id = any(array[" + strings.Join(castParams, ", ") + "])", args},
			{"select * from x_any_cast_lit where id = any(array[" + strings.Join(lits, ", ") + "]::int[])", nil},
			{"select * from x_any_cast_wide_lit where id = any(array[" + strings.Join(lits, ", ") + "]::bigint[])", nil},
			// The array cast gives array[$1] its type, so no element casts.
			{"select * from x_any_cast_param where id = any(array[" + strings.Join(params, ", ") + "]::int[])", args},
			{"select * from x_not_in_lit where id not in (" + strings.Join(lits, ", ") + ")", nil},
			{"select * from x_all_ne_lit where id <> all(array[" + strings.Join(lits, ", ") + "])", nil},
			{"select * from x_not_in_sub where id not in (select user_id from cross_accounts)", nil},
			{"select * from x_all_ne_sub where id <> all(select user_id from cross_accounts)", nil},
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
	r := pgss.NewReader(obs)
	stats, err := r.ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgss.NewTextCache(r).Fill(ctx, stats); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string][]string{}
	for _, s := range stats {
		for _, tbl := range crossTables {
			if !strings.Contains(s.Query, tbl+" ") || strings.HasPrefix(strings.ToLower(s.Query), "create") {
				continue
			}
			q := s.Query
			if alias, ok := crossAlias[tbl]; ok {
				q = strings.ReplaceAll(q, tbl, alias)
			}
			fp, err := fingerprint.Normalized(q, fingerprint.Options{})
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

func crossQueryIDs(t *testing.T, version int, tables ...string) map[string]map[int64][]string {
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
	r := pgss.NewReader(obs)
	stats, err := r.ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgss.NewTextCache(r).Fill(ctx, stats); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[int64][]string{}
	for _, s := range stats {
		for _, tbl := range tables {
			if !strings.Contains(s.Query, tbl+" ") || strings.HasPrefix(strings.ToLower(s.Query), "create") {
				continue
			}
			if out[tbl] == nil {
				out[tbl] = map[int64][]string{}
			}
			out[tbl][s.QueryID] = append(out[tbl][s.QueryID], s.Query)
		}
	}
	return out
}

func TestPostgresNotInSubqueryQueryIDs(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, v := range []int{14, 15, 16, 17, 18} {
		got := crossQueryIDs(t, v, "x_not_in_sub", "x_all_ne_sub")
		if len(got["x_not_in_sub"]) == 0 || len(got["x_all_ne_sub"]) == 0 {
			t.Fatalf("pg%d recorded no NOT IN or <> ALL subquery statements: %v", v, got)
		}
		var notInQIDs, allQIDs []int64
		for qid, notInQueries := range got["x_not_in_sub"] {
			notInQIDs = append(notInQIDs, qid)
			if allQueries, ok := got["x_all_ne_sub"][qid]; ok {
				t.Errorf("pg%d merged NOT IN subquery with <> ALL subquery as queryid %d:\n  NOT IN: %v\n  <> ALL: %v", v, qid, notInQueries, allQueries)
			}
		}
		for qid := range got["x_all_ne_sub"] {
			allQIDs = append(allQIDs, qid)
		}
		sort.Slice(notInQIDs, func(i, j int) bool { return notInQIDs[i] < notInQIDs[j] })
		sort.Slice(allQIDs, func(i, j int) bool { return allQIDs[i] < allQIDs[j] })
		t.Logf("pg%d kept NOT IN subquery apart from <> ALL subquery: NOT IN queryids %v; <> ALL queryids %v", v, notInQIDs, allQIDs)
	}
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
		if wantSquash := !strings.HasPrefix(tbl, "x_values_") && !strings.HasSuffix(tbl, "_sub"); squashed != wantSquash {
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
	// = ANY(ARRAY[...]) must group with the IN-list form on both versions.
	for tbl, alias := range crossAlias {
		for _, v := range []int{16, 18} {
			if len(byVersion[v][tbl]) == 0 || len(byVersion[v][alias]) == 0 {
				t.Errorf("%s/%s: pg%d recorded no statements to compare", tbl, alias, v)
			}
			for fp, qs := range byVersion[v][tbl] {
				if _, ok := byVersion[v][alias][fp]; !ok {
					t.Errorf("%s: pg%d fingerprint %s %v doesn't match %s: %v", tbl, v, fp, qs, alias, byVersion[v][alias])
				}
			}
		}
	}
}
