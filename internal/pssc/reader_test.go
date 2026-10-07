package pssc_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pssc"
	"github.com/benchub/rotten/internal/testdb"
)

func observer(t *testing.T, db *testdb.DB) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	su := db.Connect(t)
	if _, err := su.Exec(ctx, "alter role rotten_observer password '"+db.RolePassword("rotten_observer")+"'"); err != nil {
		t.Fatal(err)
	}
	obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { obs.Close(context.Background()) })
	return obs
}

// TestReaderWithPSSC runs tagged statements as the superuser and reads them
// back as the observer, then reads again after more calls and diffs.
func TestReaderWithPSSC(t *testing.T) {
	for _, v := range testdb.ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObserved(t, v)
			su := db.Connect(t)
			run := func(sql string, n int) {
				for i := 0; i < n; i++ {
					if _, err := su.Exec(ctx, sql); err != nil {
						t.Fatalf("%s: %v", sql, err)
					}
				}
			}
			run("/*controller:users,action:show*/ select 1", 3)
			run("select 1 /*controller:posts,action:index*/", 2)

			r := pssc.NewReader(observer(t, db))
			det, err := r.Detect(ctx)
			if err != nil {
				t.Fatalf("Detect: %v", err)
			}
			if !det.Preloaded || !det.Created || det.Schema != "public" || !det.Available() {
				t.Fatalf("Detect = %+v, want preloaded, created in public", det)
			}
			stats, ok, err := r.ReadStats(ctx)
			if err != nil || !ok {
				t.Fatalf("ReadStats ok=%v err=%v", ok, err)
			}
			calls := func(stats []pssc.Stat, controller string) (pssc.Stat, int) {
				var found pssc.Stat
				n := 0
				for _, s := range stats {
					if s.Tags["controller"] == controller {
						found = s
						n++
					}
				}
				return found, n
			}
			users, n := calls(stats, "users")
			if n != 1 || users.Calls != 3 || users.Tags["action"] != "show" {
				t.Fatalf("users entries=%d %+v, want one with 3 calls", n, users)
			}
			if users.QueryID == 0 || users.UserID == 0 || users.DBID == 0 || !users.TopLevel ||
				users.ExecTime <= 0 || users.StatsSince.IsZero() {
				t.Errorf("fields not set: %+v", users)
			}
			if posts, n := calls(stats, "posts"); n != 1 || posts.Calls != 2 {
				t.Errorf("posts entries=%d %+v, want one with 2 calls", n, posts)
			}
			if users.QueryID != func() int64 { p, _ := calls(stats, "posts"); return p.QueryID }() {
				t.Errorf("both comments should share one queryid")
			}

			_, prev := pssc.Diff(pssc.Snapshot{}, stats)
			run("/*controller:users,action:show*/ select 1", 4)
			stats2, _, err := r.ReadStats(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ds, _ := pssc.Diff(prev, stats2)
			var usersDelta *pssc.Delta
			for i := range ds {
				if ds[i].Tags["controller"] == "users" {
					usersDelta = &ds[i]
				}
				if ds[i].Tags["controller"] == "posts" {
					t.Errorf("posts had no calls but has a delta: %+v", ds[i])
				}
			}
			if usersDelta == nil || usersDelta.Calls != 4 || usersDelta.New {
				t.Fatalf("users delta = %+v, want 4 calls, not new", usersDelta)
			}
		})
	}
}

// TestReaderCappedTagIsDistinct sets a cardinality cap of one value for
// controller, so a second controller value comes back as JSON null.
func TestReaderCappedTagIsDistinct(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartObserved(t, testdb.ObservedVersions[len(testdb.ObservedVersions)-1])
	su := db.Connect(t)
	for _, s := range []string{
		"alter system set pg_stat_statement_context.cardinality_cap = 1",
		"select pg_reload_conf()",
	} {
		if _, err := su.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	su.Close(ctx)
	su = db.Connect(t)
	for _, c := range []string{"one", "two", "three"} {
		if _, err := su.Exec(ctx, "/*controller:"+c+",action:x*/ select 1"); err != nil {
			t.Fatal(err)
		}
	}
	stats, ok, err := pssc.NewReader(observer(t, db)).ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("ReadStats ok=%v err=%v", ok, err)
	}
	var one, capped int64
	for _, s := range stats {
		switch s.Tags["controller"] {
		case "one":
			one += s.Calls
		case pssc.Capped:
			capped += s.Calls
		}
	}
	if one != 1 || capped != 2 {
		t.Fatalf("one=%d capped=%d, want 1 and 2; stats %+v", one, capped, stats)
	}
}

func TestReaderWithoutPSSC(t *testing.T) {
	for _, v := range testdb.ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObservedWithoutPSSC(t, v)
			// A pssc setting without the library is only a placeholder and
			// mustn't count as preloaded.
			obs := observer(t, db)
			if _, err := obs.Exec(ctx, "set pg_stat_statement_context.tags = 'controller'"); err != nil {
				t.Fatal(err)
			}
			r := pssc.NewReader(obs)
			det, err := r.Detect(ctx)
			if err != nil {
				t.Fatalf("Detect: %v", err)
			}
			if det.Preloaded || det.Created || det.Available() {
				t.Fatalf("Detect = %+v, want nothing", det)
			}
			stats, ok, err := r.ReadStats(ctx)
			if err != nil || ok || stats != nil {
				t.Fatalf("ReadStats = %v, %v, %v; want nil, false, nil", stats, ok, err)
			}
		})
	}
}

// TestReaderExtensionChanges: dropping or creating the extension after the
// first read is picked up by the next ReadStats, without an error.
func TestReaderExtensionChanges(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartObserved(t, testdb.ObservedVersions[0])
	su := db.Connect(t)
	r := pssc.NewReader(observer(t, db))
	if _, ok, err := r.ReadStats(ctx); err != nil || !ok {
		t.Fatalf("first read ok=%v err=%v", ok, err)
	}
	if _, err := su.Exec(ctx, "drop extension pg_stat_statement_context"); err != nil {
		t.Fatal(err)
	}
	if stats, ok, err := r.ReadStats(ctx); err != nil || ok || stats != nil {
		t.Fatalf("after drop: %v, %v, %v; want nil, false, nil", stats, ok, err)
	}
	for _, s := range []string{
		"create schema ctx",
		"grant usage on schema ctx to public",
		"create extension pg_stat_statement_context schema ctx",
		"/*controller:back,action:x*/ select 1",
	} {
		if _, err := su.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	stats, ok, err := r.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("after create: ok=%v err=%v", ok, err)
	}
	found := false
	for _, s := range stats {
		found = found || s.Tags["controller"] == "back"
	}
	if !found {
		t.Fatalf("tagged call missing after re-create: %+v", stats)
	}
}

// TestReaderSchema covers pssc created in a schema off the observer's
// search path: the reader finds it through pg_extension.
func TestReaderSchema(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartObserved(t, testdb.ObservedVersions[0])
	su := db.Connect(t)
	for _, s := range []string{
		"drop extension pg_stat_statement_context",
		"create schema ctx",
		"grant usage on schema ctx to public",
		"create extension pg_stat_statement_context schema ctx",
		"/*controller:sch,action:x*/ select 1",
	} {
		if _, err := su.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	r := pssc.NewReader(observer(t, db))
	det, err := r.Detect(ctx)
	if err != nil || det.Schema != "ctx" || !det.Available() {
		t.Fatalf("Detect = %+v, %v; want schema ctx", det, err)
	}
	stats, ok, err := r.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("ReadStats ok=%v err=%v", ok, err)
	}
	found := false
	for _, s := range stats {
		found = found || s.Tags["controller"] == "sch"
	}
	if !found {
		t.Fatalf("tagged call missing: %+v", stats)
	}
}
