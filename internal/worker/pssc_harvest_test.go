package worker

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/testdb"
)

const psscQuery = `select count(*) from widgets where id > 4`

func runTimes(t *testing.T, conn *pgx.Conn, sql string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rows, err := conn.Query(context.Background(), sql, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// contextCounts maps controller/action/job to count.
func contextCounts(a *rottenv1.FingerprintAggregate) map[[3]string]uint64 {
	out := map[[3]string]uint64{}
	for _, c := range a.GetContexts() {
		out[[3]string{c.GetController(), c.GetAction(), c.GetJobTag()}] = c.GetCount()
	}
	return out
}

func harvestOneWindow(t *testing.T, db *testdb.DB, workload func(*pgx.Conn)) *rottenv1.FingerprintAggregate {
	t.Helper()
	store := openStore(t, t.TempDir())
	defer store.Close()
	conn := db.Connect(t)
	rn := startOutboxWorker(t, db.DSNAs(t, "rotten_observer"), 7, 42, store, store)
	workload(conn)
	rn.window(t)
	rn.stop(t)
	return findAggregate(t, drainOutboxBatches(t, store), fingerprintOf(t, psscQuery))
}

// One fingerprint from two controllers, plus calls with no tags: each gets
// its own exact count. With query-text parsing, the first text's context
// took every call.
func TestPSSCContextsShipExactCountsPerController(t *testing.T) {
	db := startObservedVersionForWorker(t, 16)
	agg := harvestOneWindow(t, db, func(c *pgx.Conn) {
		runTimes(t, c, psscQuery+` /*controller:users,action:show*/`, 3)
		runTimes(t, c, psscQuery+` /*controller:posts,action:index,job:Reindex*/`, 5)
		runTimes(t, c, psscQuery, 2)
	})
	want := map[[3]string]uint64{
		{"users", "show", ""}:         3,
		{"posts", "index", "Reindex"}: 5,
		{"", "", ""}:                  2,
	}
	got := contextCounts(agg)
	if len(got) != len(want) {
		t.Fatalf("contexts = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("contexts = %v, want %v", got, want)
		}
	}
	if agg.GetMetrics().GetCalls() != 10 {
		t.Fatalf("calls = %d, want 10", agg.GetMetrics().GetCalls())
	}
}

// Without pssc there's no regex fallback: every call is untagged, even
// those whose text carries marginalia the regexes would match.
func TestNoPSSCShipsOnlyUntaggedContext(t *testing.T) {
	db := testdb.StartObservedWithoutPSSC(t, 16)
	setupObservedForWorker(t, db)
	agg := harvestOneWindow(t, db, func(c *pgx.Conn) {
		runTimes(t, c, psscQuery+` /*controller:users,action:show,job:CleanupJob*/`, 4)
	})
	got := contextCounts(agg)
	if len(got) != 1 || got[[3]string{"", "", ""}] != 4 {
		t.Fatalf("contexts = %v, want only untagged with 4 calls", got)
	}
}

// Postgres 18 drops leading comments from pgss's text, but pssc still sees
// them (with position=any extractors), so prepended marginalia attribute.
func TestPSSCContextsFromPrependedMarginaliaOnPG18(t *testing.T) {
	db := startObservedVersionForWorker(t, 18)
	agg := harvestOneWindow(t, db, func(c *pgx.Conn) {
		runTimes(t, c, `/*controller:users,action:show*/ `+psscQuery, 2)
		runTimes(t, c, `/*controller:posts,action:index*/ `+psscQuery, 1)
	})
	got := contextCounts(agg)
	if len(got) != 2 || got[[3]string{"users", "show", ""}] != 2 || got[[3]string{"posts", "index", ""}] != 1 {
		t.Fatalf("contexts = %v, want users/show 2 and posts/index 1", got)
	}
}

// The second window ships only its own pssc calls, so the pssc snapshot was
// saved with the first and diffed against.
func TestPSSCContextsDiffAgainstSavedSnapshot(t *testing.T) {
	db := startObservedVersionForWorker(t, 17)
	store := openStore(t, t.TempDir())
	defer store.Close()
	conn := db.Connect(t)
	rn := startOutboxWorker(t, db.DSNAs(t, "rotten_observer"), 7, 42, store, store)
	runTimes(t, conn, psscQuery+` /*controller:users,action:show*/`, 3)
	rn.window(t)
	if got := drainOutboxBatches(t, store); len(got) != 1 {
		t.Fatalf("batches = %d, want 1", len(got))
	}
	runTimes(t, conn, psscQuery+` /*controller:users,action:show*/`, 2)
	runTimes(t, conn, psscQuery, 1)
	rn.window(t)
	rn.stop(t)
	got := contextCounts(findAggregate(t, drainOutboxBatches(t, store), fingerprintOf(t, psscQuery)))
	if len(got) != 2 || got[[3]string{"users", "show", ""}] != 2 || got[[3]string{"", "", ""}] != 1 {
		t.Fatalf("second window contexts = %v, want users/show 2 and untagged 1", got)
	}
}
