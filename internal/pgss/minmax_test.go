package pgss_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/testdb"
)

func TestWindowMinMax(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	base := pgss.Stat{Calls: 3, MinTime: 1, MaxTime: 9}
	with := func(s pgss.Stat, since *time.Time) *pgss.Stat { s.MinmaxStatsSince = since; return &s }
	for _, c := range []struct {
		name string
		d    pgss.Delta
		want bool
	}{
		{"new entry", pgss.Delta{Stat: base, New: true}, false},
		{"pre-17", pgss.Delta{Stat: base, Prev: &base}, true},
		{"17, reset since prev harvest", pgss.Delta{Stat: *with(base, &t1), Prev: with(base, &t0)}, false},
		{"17, no reset since prev harvest", pgss.Delta{Stat: *with(base, &t0), Prev: with(base, &t0)}, true},
		{"17, prev missing since", pgss.Delta{Stat: *with(base, &t1), Prev: &base}, true},
	} {
		min, max, lifetime := pgss.WindowMinMax(c.d)
		if min != 1 || max != 9 || lifetime != c.want {
			t.Errorf("%s: got %v %v %v, want 1 9 %v", c.name, min, max, lifetime, c.want)
		}
	}
}

// TestMinmaxResetWindow runs a slow call, harvests, resets min/max (17+),
// runs a fast call of the same statement, and harvests again. On 17+ the
// second window's max reflects only the fast call. On 16 the reset isn't
// available and the max is flagged as lifetime.
func TestMinmaxResetWindow(t *testing.T) {
	for _, c := range []struct {
		v      int
		schema string
	}{{16, "rotten"}, {17, "rotten"}, {18, `Rotten "mm"`}} {
		t.Run(fmt.Sprintf("pg%d", c.v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObserved(t, c.v)
			if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"),
				map[string]string{"observer_schema": c.schema}); err != nil {
				t.Fatalf("observer.sql: %v\n%s", err, out)
			}
			su := db.Connect(t)
			if _, err := su.Exec(ctx, "alter role rotten_observer password 'rotten_observer'"); err != nil {
				t.Fatal(err)
			}
			obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			r := pgss.NewReader(obs)

			run := func(secs float64) {
				t.Helper()
				if _, err := su.Exec(ctx, "select /*mm_marker*/ pg_sleep($1)", secs); err != nil {
					t.Fatal(err)
				}
			}
			harvest := func(prev pgss.Snapshot) ([]pgss.Delta, pgss.Snapshot) {
				t.Helper()
				stats, err := r.ReadStats(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := pgss.NewTextCache(r).Fill(ctx, stats); err != nil {
					t.Fatal(err)
				}
				info, err := r.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				return pgss.Diff(prev, stats, info)
			}

			run(0.3)
			_, snap := harvest(pgss.Snapshot{})
			has, err := r.HasMinmaxReset(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if has != (c.v >= 17) {
				t.Fatalf("HasMinmaxReset = %v on %d", has, c.v)
			}
			err = r.MinmaxReset(ctx, c.schema)
			if c.v >= 17 && err != nil {
				t.Fatalf("MinmaxReset: %v", err)
			}
			if c.v < 17 && err == nil {
				t.Fatalf("MinmaxReset on %d: want error", c.v)
			}
			if c.v >= 17 {
				// A missing schema, and a real schema without the wrapper.
				for _, bad := range []string{"no_such_schema", "public"} {
					err := r.MinmaxReset(ctx, bad)
					if err == nil || !strings.Contains(err.Error(), "schema/observer.sql") || !strings.Contains(err.Error(), "MinmaxResetSchema") {
						t.Errorf("MinmaxReset(%q): err = %v, want a hint naming schema/observer.sql and MinmaxResetSchema", bad, err)
					}
				}
			}
			run(0)
			deltas, _ := harvest(snap)

			var d *pgss.Delta
			for i := range deltas {
				if strings.Contains(deltas[i].Query, "mm_marker") {
					d = &deltas[i]
				}
			}
			if d == nil {
				t.Fatal("marker delta missing")
			}
			if d.New || d.Calls != 1 {
				t.Fatalf("delta New=%v Calls=%d, want a one-call diff", d.New, d.Calls)
			}
			_, max, lifetime := pgss.WindowMinMax(*d)
			if c.v >= 17 {
				if lifetime || max >= 100 {
					t.Errorf("max=%vms lifetime=%v, want window-only max under 100ms", max, lifetime)
				}
			} else if !lifetime || max < 300 {
				t.Errorf("max=%vms lifetime=%v, want lifetime max of at least 300ms", max, lifetime)
			}
		})
	}
}
