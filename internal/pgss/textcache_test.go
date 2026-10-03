package pgss_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/testdb"
)

// TestTextCacheMatrix harvests the same workload twice. The first harvest
// fetches text once, and the second fetches none. It also checks that the
// observer sees another role's text, and that evicted keys leave the cache.
func TestTextCacheMatrix(t *testing.T) {
	for _, v := range testdb.ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObserved(t, v)
			if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
				t.Fatalf("observer.sql: %v\n%s", err, out)
			}
			su := db.Connect(t)
			for _, s := range []string{
				"alter role rotten_observer password 'rotten_observer'",
				"create table tc_t (id int primary key)",
			} {
				if _, err := su.Exec(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			// The function name and signature on every supported version.
			var sig string
			if err := su.QueryRow(ctx,
				`select 'pg_stat_statements(boolean)'::regprocedure::text`).Scan(&sig); err != nil {
				t.Fatalf("pg_stat_statements(boolean) missing: %v", err)
			}
			// The observer's own Read and Fill queries are new keys on each
			// early harvest, so select only the superuser's entries, as the
			// worker selects its top N.
			var suID uint32
			if err := su.QueryRow(ctx, "select current_user::regrole::oid").Scan(&suID); err != nil {
				t.Fatal(err)
			}
			workload := func() {
				for i := 0; i < 3; i++ {
					if _, err := su.Exec(ctx, "select /*tc_marker*/ id from tc_t where id = $1", i); err != nil {
						t.Fatal(err)
					}
				}
			}
			workload()

			obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			r := pgss.NewReader(obs)
			c := pgss.NewTextCache(r)

			harvest := func() []pgss.Stat {
				t.Helper()
				stats, err := r.ReadStats(ctx)
				if err != nil {
					t.Fatalf("ReadStats: %v", err)
				}
				for _, s := range stats {
					if s.Query != "" {
						t.Fatalf("ReadStats returned text %q, want none", s.Query)
					}
				}
				c.Retain(stats)
				var picked []pgss.Stat
				for _, s := range stats {
					if s.UserID == suID {
						picked = append(picked, s)
					}
				}
				if err := c.Fill(ctx, picked); err != nil {
					t.Fatalf("Fill: %v", err)
				}
				seen := map[string]bool{}
				for _, s := range picked {
					if s.Query == "" || seen[s.Query] {
						t.Errorf("after Fill, key %+v has empty or duplicate text %q", pgss.KeyOf(s), s.Query)
					}
					seen[s.Query] = true
				}
				return picked
			}
			find := func(stats []pgss.Stat) *pgss.Stat {
				for i := range stats {
					if strings.Contains(stats[i].Query, "tc_marker") {
						return &stats[i]
					}
				}
				return nil
			}

			s1 := harvest()
			if c.TextFetches() != 1 {
				t.Errorf("first harvest: %d text fetches, want 1", c.TextFetches())
			}
			sel := find(s1)
			if sel == nil {
				t.Fatalf("observer can't see the superuser's text in %d stats", len(s1))
			}

			workload()
			s2 := harvest()
			if c.TextFetches() != 1 {
				t.Errorf("second harvest: %d text fetches total, want still 1", c.TextFetches())
			}
			if find(s2) == nil {
				t.Errorf("second harvest lost the cached text")
			}

			// Evict: Retain with a set that lacks the marker key drops it.
			var rest []pgss.Stat
			for _, s := range s2 {
				if pgss.KeyOf(s) != pgss.KeyOf(*sel) {
					rest = append(rest, s)
				}
			}
			c.Retain(rest)
			if c.Has(pgss.KeyOf(*sel)) {
				t.Errorf("evicted key still cached")
			}
			s3 := harvest()
			if c.TextFetches() != 2 {
				t.Errorf("after eviction: %d text fetches, want 2", c.TextFetches())
			}
			if find(s3) == nil {
				t.Errorf("refetch after eviction didn't restore text")
			}
		})
	}
}

// TestTextCacheSkipsHiddenKeys reads as a role without pg_read_all_stats, so
// other roles' rows come back with a NULL queryid (QueryID 0) and the
// "<insufficient privilege>" placeholder. Fill must leave those empty and
// never cache them, since QueryID 0 folds many entries into one key.
func TestTextCacheSkipsHiddenKeys(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartObserved(t, 18)
	su := db.Connect(t)
	for _, s := range []string{
		"create role tc_nopriv login password 'tc_nopriv'",
		"select /*tc_hidden*/ 1",
	} {
		if _, err := su.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	conn, err := pgx.Connect(ctx, db.DSNAs(t, "tc_nopriv"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	c := pgss.NewTextCache(pgss.NewReader(conn))
	stats, err := pgss.NewReader(conn).ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hidden []pgss.Stat
	for _, s := range stats {
		if s.QueryID == 0 {
			hidden = append(hidden, s)
		}
	}
	if len(hidden) == 0 {
		t.Fatalf("no hidden rows in %d stats", len(stats))
	}
	if err := c.Fill(ctx, hidden); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	for _, s := range hidden {
		if s.Query != "" {
			t.Errorf("hidden row got text %q, want empty", s.Query)
		}
	}
	if c.Len() != 0 {
		t.Errorf("cache holds %d entries, want 0", c.Len())
	}
}

func TestTextCacheAppliesCachedTextWhenMissFetchFails(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartObserved(t, 18)
	if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	su := db.Connect(t)
	for _, s := range []string{
		"alter role rotten_observer password 'rotten_observer'",
		"select /*tc_cached_before_fetch_error*/ 1",
	} {
		if _, err := su.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close(ctx)
	r := pgss.NewReader(obs)
	c := pgss.NewTextCache(r)
	stats, err := r.ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var cached pgss.Stat
	for _, s := range stats {
		if s.QueryID != 0 {
			cached = s
			break
		}
	}
	if cached.QueryID == 0 {
		t.Fatal("no visible row to cache")
	}
	if err := c.Fill(ctx, []pgss.Stat{cached}); err != nil {
		t.Fatal(err)
	}
	cached.Query = ""
	miss := cached
	miss.QueryID = cached.QueryID + 1
	mixed := []pgss.Stat{cached, miss}
	if err := obs.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Fill(ctx, mixed); err == nil {
		t.Fatal("Fill succeeded after connection close, want fetch error for the miss")
	}
	if mixed[0].Query == "" {
		t.Fatal("cached row query stayed empty when a miss fetch failed")
	}
	if mixed[1].Query != "" {
		t.Fatalf("miss row query = %q, want empty", mixed[1].Query)
	}
}
