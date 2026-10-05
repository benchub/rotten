package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
)

// TestWorkerCreditsRecreatedEntryToNewText runs a query under context A,
// harvests, recreates its pg_stat_statements entry, runs the same query under
// context B, and harvests again. The second window's calls belong to B, the
// recreated entry's text, not to A's cached text. In the entry reset and
// eviction cases every counter climbs past A's, so no counter goes down and
// only stats_since (17+) or dealloc (14 through 16) shows the recreation.
func TestWorkerCreditsRecreatedEntryToNewText(t *testing.T) {
	type recreate func(t *testing.T, su *pgx.Conn, query string)
	fullReset := func(t *testing.T, su *pgx.Conn, _ string) {
		t.Helper()
		if _, err := su.Exec(context.Background(), `select pg_stat_statements_reset()`); err != nil {
			t.Fatal(err)
		}
	}
	// entryReset resets only this entry, so stats_reset and dealloc stay put
	// and B's calls climb past A's: only stats_since (17+) shows it.
	entryReset := func(t *testing.T, su *pgx.Conn, query string) {
		t.Helper()
		qid := queryIDOf(t, su, query)
		if _, err := su.Exec(context.Background(), `select pg_stat_statements_reset(0, 0, $1)`, qid); err != nil {
			t.Fatal(err)
		}
		if n := entriesFor(t, su, qid); n != 0 {
			t.Fatalf("entry reset left %d entries", n)
		}
	}
	// evict floods pg_stat_statements (max 100) with distinct statements
	// until the entry is deallocated. B's calls then climb past A's, so only
	// the dealloc count (14 through 16) or stats_since (17+) shows it.
	evict := func(t *testing.T, su *pgx.Conn, query string) {
		t.Helper()
		ctx := context.Background()
		qid := queryIDOf(t, su, query)
		var before int64
		if err := su.QueryRow(ctx, `select dealloc from pg_stat_statements_info`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		cols := 0
		for round := 0; round < 10 && entriesFor(t, su, qid) > 0; round++ {
			for i := 0; i < 150; i++ {
				cols++
				if _, err := su.Exec(ctx, "select "+strings.TrimSuffix(strings.Repeat("1,", cols), ","), pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatal(err)
				}
			}
		}
		if n := entriesFor(t, su, qid); n != 0 {
			t.Fatalf("flood didn't evict the entry (%d left)", n)
		}
		var after int64
		if err := su.QueryRow(ctx, `select dealloc from pg_stat_statements_info`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after <= before {
			t.Fatalf("dealloc %d didn't grow past %d", after, before)
		}
	}
	smallMax := testcontainers.WithCmdArgs("-c", "pg_stat_statements.max=100")

	for _, tc := range []struct {
		version int
		extra   []testcontainers.ContainerCustomizer
		cases   []string
	}{
		{14, []testcontainers.ContainerCustomizer{smallMax}, []string{"full reset", "eviction"}},
		{17, []testcontainers.ContainerCustomizer{smallMax}, []string{"entry reset", "eviction"}},
		{18, nil, []string{"full reset", "entry reset"}},
	} {
		t.Run(fmt.Sprintf("pg%d", tc.version), func(t *testing.T) {
			observed := startObservedVersionForWorker(t, tc.version, tc.extra...)
			su := observed.Connect(t)
			obsDSN := observed.DSNAs(t, "rotten_observer")
			ops := map[string]recreate{"full reset": fullReset, "entry reset": entryReset, "eviction": evict}
			for i, name := range tc.cases {
				t.Run(name, func(t *testing.T) {
					// A distinct statement per case, so cases don't share an entry.
					query := []string{
						`select count(name) from widgets where id > 3`,
						`select max(name) from widgets where id > 3`,
					}[i]
					// Warm the backend, then clear the warm-up's cold-cache cost from
					// the entry, so B's counters can climb past A's.
					runWithContext(t, su, query, "warmup", 1)
					qid := queryIDOf(t, su, query)
					if _, err := su.Exec(context.Background(), `select pg_stat_statements_reset(0, 0, $1)`, qid); err != nil {
						t.Fatal(err)
					}
					store := openStore(t, t.TempDir())
					defer store.Close()
					rn := startOutboxWorker(t, obsDSN, uint32(20+i), uint32(60+i), store, store)
					runWithContext(t, su, query, "alpha", 1)
					h1 := rn.window(t)
					before := countersOf(t, su, qid)
					ops[name](t, su, query)
					runWithContext(t, su, query, "beta", 20)
					betaCalls := 20
					if name != "full reset" {
						// Plan and exec times are wall clock, so a stall during A's one
						// call can outweigh B's first 20. Keep running B until every
						// counter has climbed past A's.
						for {
							c, below := counterBelow(before, countersOf(t, su, qid))
							if !below {
								break
							}
							if betaCalls >= 2000 {
								t.Fatalf("%s still below A's %v after %d calls; the case needs every counter to climb", c, before[c], betaCalls)
							}
							runWithContext(t, su, query, "beta", 1)
							betaCalls++
						}
					}
					h2 := rn.window(t)
					rn.stop(t)
					want := uint64(betaCalls)
					if name == "eviction" && tc.version < 17 {
						// Diff can't see the recreation on 14 through 16, so it
						// subtracts A's one call (see Diff's doc comment).
						want--
					}

					batches := drainOutboxBatches(t, store)
					var found bool
					for _, b := range batches {
						if b.GetWindowStart().AsTime().Unix() != h1 || b.GetWindowEnd().AsTime().Unix() != h2 {
							continue
						}
						found = true
						for _, a := range b.GetAggregates() {
							if a.GetFingerprint() != fingerprintOf(t, query) {
								continue
							}
							if a.GetMetrics().GetCalls() != want {
								t.Errorf("calls = %d, want %d", a.GetMetrics().GetCalls(), want)
							}
							cs := a.GetContexts()
							if len(cs) != 1 || cs[0].GetController() != "beta" || cs[0].GetCount() != want {
								t.Fatalf("contexts = %+v, want beta with count %d", cs, want)
							}
							return
						}
						t.Fatalf("window [%d,%d] has no aggregate for %q", h1, h2, query)
					}
					if !found {
						t.Fatalf("window [%d,%d] not found", h1, h2)
					}
				})
			}
		})
	}
}

func runWithContext(t *testing.T, conn *pgx.Conn, query, controller string, n int) {
	t.Helper()
	q := query + fmt.Sprintf(" /*controller:%s,action:show*/", controller)
	for i := 0; i < n; i++ {
		rows, err := conn.Query(context.Background(), q, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func queryIDOf(t *testing.T, conn *pgx.Conn, query string) int64 {
	t.Helper()
	var qid int64
	if err := conn.QueryRow(context.Background(),
		`select queryid from pg_stat_statements where starts_with(query, $1) and toplevel limit 1`, strings.SplitN(query, " where", 2)[0]).Scan(&qid); err != nil {
		t.Fatalf("queryid of %q: %v", query, err)
	}
	return qid
}

// countersOf reads the cumulative counters every supported version has.
func countersOf(t *testing.T, conn *pgx.Conn, qid int64) map[string]float64 {
	t.Helper()
	cols := []string{"plans", "total_plan_time", "calls", "total_exec_time", "rows",
		"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
		"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
		"temp_blks_read", "temp_blks_written", "wal_records", "wal_fpi", "wal_bytes"}
	vals := make([]float64, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := conn.QueryRow(context.Background(), `select `+strings.Join(cols, "::float8, ")+`::float8
		from pg_stat_statements where queryid = $1 and toplevel`, qid).Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

// counterBelow returns a counter whose value in after is below its value in
// before, if there is one.
func counterBelow(before, after map[string]float64) (string, bool) {
	for c := range before {
		if after[c] < before[c] {
			return c, true
		}
	}
	return "", false
}

func entriesFor(t *testing.T, conn *pgx.Conn, qid int64) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `select count(*) from pg_stat_statements where queryid = $1`, qid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
