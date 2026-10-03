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
// Casted elements stay separate from uncast IN lists when PostgreSQL keeps
// them separate. No-op casts that PostgreSQL omits from the query tree, such
// as ::int on an int column, keep grouping with the uncast list.
var crossAlias = map[string]string{
	"x_any_lit":    "x_in_lit",
	"x_any_param":  "x_in_cast_param",
	"x_all_ne_lit": "x_not_in_lit",
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

func oneQueryID(t *testing.T, conn *pgx.Conn, table, query string, args ...any) (int64, string) {
	t.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "select pg_stat_statements_reset()"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	rows, err := conn.Query(ctx, "select queryid, query from pg_stat_statements where query like $1 and lower(query) not like 'create%' order by calls desc, query", "%"+table+"%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []struct {
		qid   int64
		query string
	}
	for rows.Next() {
		var r struct {
			qid   int64
			query string
		}
		if err := rows.Scan(&r.qid, &r.query); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("%s: got %d pg_stat_statements rows, want 1: %v", query, len(got), got)
	}
	return got[0].qid, got[0].query
}

func TestPostgresInListElementCastQueryIDs(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	type caseDef struct {
		name string
		sql  string
	}
	cases := []caseDef{
		{"plain_one", "select * from x_cast_id where id in (1)"},
		{"plain_many", "select * from x_cast_id where id in (1, 2, 3)"},
		{"int_cast", "select * from x_cast_id where id in (1::int4)"},
		{"int_cast_many", "select * from x_cast_id where id in (1::int, 2::int, 3::int)"},
		{"single_cast", "select * from x_cast_id where id in (1::bigint)"},
		{"single_cast_many", "select * from x_cast_id where id in (1::bigint, 2::bigint, 3::bigint)"},
		{"nested_cast", "select * from x_cast_id where id in (1::bigint::int)"},
		{"mixed_cast", "select * from x_cast_id where id in (1, 2::bigint)"},
		{"any_array_nested_cast", "select * from x_cast_id where id = any(array[1::bigint]::int[])"},
	}
	for _, v := range []int{14, 15, 16, 17, 18} {
		db := testdb.StartObserved(t, v)
		conn := db.Connect(t)
		ctx := context.Background()
		if _, err := conn.Exec(ctx, "create table x_cast_id (id int)"); err != nil {
			t.Fatal(err)
		}
		qids := map[string]int64{}
		texts := map[string]string{}
		for _, c := range cases {
			qid, text := oneQueryID(t, conn, "x_cast_id", c.sql)
			qids[c.name] = qid
			texts[c.name] = text
		}
		t.Logf("pg%d IN cast queryids: %v", v, qids)
		t.Logf("pg%d IN cast texts: %v", v, texts)

		if qids["int_cast"] != qids["plain_one"] {
			t.Errorf("pg%d split no-op single int cast from plain IN-list: int cast %d, plain %d", v, qids["int_cast"], qids["plain_one"])
		}
		if qids["int_cast_many"] != qids["plain_many"] {
			t.Errorf("pg%d split no-op multi int casts from plain IN-list: int casts %d, plain %d", v, qids["int_cast_many"], qids["plain_many"])
		}
		for _, name := range []string{"single_cast", "single_cast_many", "nested_cast", "mixed_cast", "any_array_nested_cast"} {
			if qids[name] == qids["plain_one"] {
				t.Errorf("pg%d merged %s with plain IN-list as queryid %d", v, name, qids[name])
			}
		}
		if v == 18 {
			if qids["single_cast_many"] != qids["mixed_cast"] {
				t.Errorf("pg18 split squashed mixed and all-cast IN lists: all-cast %d, mixed %d", qids["single_cast_many"], qids["mixed_cast"])
			}
		} else if qids["single_cast_many"] == qids["mixed_cast"] {
			t.Errorf("pg%d merged mixed and all-cast IN lists as queryid %d", v, qids["mixed_cast"])
		}
		for _, pair := range [][2]string{
			{"plain_one", "plain_many"},
			{"single_cast", "single_cast_many"},
			{"single_cast", "nested_cast"},
			{"nested_cast", "any_array_nested_cast"},
		} {
			if qids[pair[0]] == qids[pair[1]] {
				t.Errorf("pg%d merged %s and %s as queryid %d", v, pair[0], pair[1], qids[pair[0]])
			}
		}
	}
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
