package worker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/harvestlimits"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/state"
	"github.com/benchub/rotten/internal/testdb"
)

// diffRig is one observed Postgres (with only schema/observer.sql loaded, so
// the observer can't run a full reset) and a rotten DB pool. Each scenario
// gets its own logical source, so its events are its own.
type diffRig struct {
	version int
	pool    *pgxpool.Pool
	obsDSN  string
	su      *pgx.Conn
}

func newDiffRig(t *testing.T, version int, pool *pgxpool.Pool) *diffRig {
	t.Helper()
	db := testdb.StartObserved(t, version)
	if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	su := db.Connect(t)
	for _, s := range []string{
		"alter role rotten_observer password 'rotten_observer'",
		"create table widgets (id int primary key, name text)",
		"insert into widgets select g, 'w' || g from generate_series(1, 10) g",
	} {
		if _, err := su.Exec(context.Background(), s); err != nil {
			t.Fatalf("%.60s: %v", s, err)
		}
	}
	return &diffRig{version: version, pool: pool, obsDSN: db.DSNAs(t, "rotten_observer"), su: su}
}

func (r *diffRig) source(t *testing.T, name string) (logical, physical uint32) {
	t.Helper()
	name = fmt.Sprintf("pg%d-%s", r.version, name)
	ctx := context.Background()
	if err := r.pool.QueryRow(ctx, `insert into logical_sources (project,environment,cluster,role) values ($1,'e','c','r') returning id`, name).Scan(&logical); err != nil {
		t.Fatal(err)
	}
	if err := r.pool.QueryRow(ctx, `insert into physical_sources (fqdn) values ($1) returning id`, name).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pool.Exec(ctx, `insert into logical_physical_sources (logical_source_id, physical_source_id) values ($1, $2)`, logical, physical); err != nil {
		t.Fatal(err)
	}
	return logical, physical
}

// diffQuery is the scenario workload. Scenarios share it, and their events
// stay apart by logical source.
const diffQuery = `select count(*) from widgets where id > 3 /*diff_marker*/`

func (r *diffRig) work(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rows, err := r.su.Query(context.Background(), diffQuery, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func (r *diffRig) statsReset(t *testing.T) time.Time {
	t.Helper()
	var ts time.Time
	if err := r.su.QueryRow(context.Background(), `select stats_reset from pg_stat_statements_info`).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	return ts
}

// running is one worker Run in progress, driven a window at a time.
type running struct {
	w      *Worker
	clk    *stepClock
	cancel context.CancelFunc
	ran    chan error
	// harvests holds each harvest's time (Unix seconds), in order.
	harvests []int64
}

// start runs a worker for the source against store and waits for its first
// harvest.
func (r *diffRig) start(t *testing.T, logical, physical uint32, store StateStore) *running {
	t.Helper()
	reC, reA, reJ := sampleRegexes(t)
	cfg := Config{
		RottenDB:            r.pool,
		ObservedDB:          observerConn(t, r.obsDSN),
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           logical,
		PhysicalID:          physical,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		State:               store,
	}
	clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
	w := New(cfg, clk)
	parkStats(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	rn := &running{w: w, clk: clk, cancel: cancel, ran: make(chan error, 1)}
	go func() { rn.ran <- w.Run(ctx) }()
	t.Cleanup(func() { rn.stop(t) })
	clk.waitSleep(t)
	rn.harvests = append(rn.harvests, w.lastHarvest.Load())
	return rn
}

func (r *diffRig) startOutbox(t *testing.T, logical, physical uint32, store *state.Store) *running {
	t.Helper()
	reC, reA, reJ := sampleRegexes(t)
	cfg := Config{
		ObservedDB:          observerConn(t, r.obsDSN),
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           logical,
		PhysicalID:          physical,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		State:               store,
		ServerOutbox:        store,
	}
	clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
	w := New(cfg, clk)
	parkStats(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	rn := &running{w: w, clk: clk, cancel: cancel, ran: make(chan error, 1)}
	go func() { rn.ran <- w.Run(ctx) }()
	t.Cleanup(func() { rn.stop(t) })
	clk.waitSleep(t)
	rn.harvests = append(rn.harvests, w.lastHarvest.Load())
	return rn
}

// window lets the worker sleep out its window and waits for the next harvest.
// It returns that harvest's time.
func (rn *running) window(t *testing.T) int64 {
	t.Helper()
	rn.clk.proceed <- struct{}{}
	rn.clk.waitSleep(t)
	h := rn.w.lastHarvest.Load()
	if h <= rn.harvests[len(rn.harvests)-1] {
		t.Fatalf("harvest time %d didn't move past %d", h, rn.harvests[len(rn.harvests)-1])
	}
	rn.harvests = append(rn.harvests, h)
	return h
}

func (rn *running) stop(t *testing.T) {
	if rn.cancel == nil {
		return
	}
	rn.cancel()
	rn.cancel = nil
	select {
	case err := <-rn.ran:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("run returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("run didn't return after cancel")
	}
}

type windowEvent struct {
	ws, we int64
	calls  float64
	time   float64
}

// diffEvents returns the source's events for diffQuery's fingerprint, by
// window end.
func (r *diffRig) diffEvents(t *testing.T, logical uint32) map[int64][]windowEvent {
	t.Helper()
	rows, err := r.pool.Query(context.Background(), `
		select extract(epoch from e.observed_window_start)::bigint, extract(epoch from e.observed_window_end)::bigint,
		       e.calls, e.time
		  from rotten.events e join rotten.fingerprints f on f.id = e.fingerprint_id
		 where e.logical_source_id = $1 and f.fingerprint = $2`, logical, fingerprintOf(t, diffQuery))
	if err != nil {
		t.Fatal(err)
	}
	out := map[int64][]windowEvent{}
	var e windowEvent
	_, err = pgx.ForEachRow(rows, []any{&e.ws, &e.we, &e.calls, &e.time}, func() error {
		out[e.we] = append(out[e.we], e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// sourceEvents counts all of the source's events, and those with a negative
// counter.
func (r *diffRig) sourceEvents(t *testing.T, logical uint32) (all, negative int) {
	t.Helper()
	if err := r.pool.QueryRow(context.Background(), `
		select count(*), count(*) filter (where calls < 0 or time < 0)
		  from rotten.events where logical_source_id = $1`, logical).Scan(&all, &negative); err != nil {
		t.Fatal(err)
	}
	return all, negative
}

// expectWindow waits for diffQuery's event in the window [ws, we] and checks
// its calls. calls 0 means no diffQuery event: it waits a moment and checks
// there's none ending at we.
func (r *diffRig) expectWindow(t *testing.T, logical uint32, ws, we int64, calls float64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	if calls == 0 {
		deadline = time.Now().Add(time.Second)
	}
	for {
		evs := r.diffEvents(t, logical)[we]
		if len(evs) > 0 || time.Now().After(deadline) {
			if calls == 0 {
				if len(evs) != 0 {
					t.Errorf("window ending %d has events %+v, want none", we, evs)
				}
				return
			}
			if len(evs) != 1 {
				t.Fatalf("window ending %d has %d diff events (%+v), want 1", we, len(evs), evs)
			}
			e := evs[0]
			if e.ws != ws || e.we != we {
				t.Errorf("window = [%d,%d], want [%d,%d]", e.ws, e.we, ws, we)
			}
			if e.calls != calls {
				t.Errorf("window [%d,%d] calls = %v, want %v (the window's workload)", ws, we, e.calls, calls)
			}
			if e.time <= 0 {
				t.Errorf("window [%d,%d] time = %v, want > 0", ws, we, e.time)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// expectBaseline checks that the harvest at we wrote no events at all for
// the source, after giving processEvent a moment.
func (r *diffRig) expectBaseline(t *testing.T, logical uint32, we int64) {
	t.Helper()
	time.Sleep(time.Second)
	var n int
	if err := r.pool.QueryRow(context.Background(), `select count(*) from rotten.events
		where logical_source_id = $1 and observed_window_end = to_timestamp($2)`, logical, we).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("baseline harvest at %d wrote %d events, want 0", we, n)
	}
}

// faultStore wraps a real store and fails Load or Save on demand.
type faultStore struct {
	inner              *state.Store
	failLoad, failSave atomic.Bool
}

func (f *faultStore) Load(ctx context.Context) (state.Loaded, error) {
	if f.failLoad.Load() {
		return state.Loaded{}, errors.New("injected load failure")
	}
	return f.inner.Load(ctx)
}

func (f *faultStore) Save(ctx context.Context, snap pgss.Snapshot, takenAt time.Time) error {
	if f.failSave.Load() {
		return errors.New("injected save failure")
	}
	return f.inner.Save(ctx, snap, takenAt)
}

func openStore(t *testing.T, dir string) *state.Store {
	t.Helper()
	s, err := state.Open(dir, state.Options{MaxSnapshotAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestWorkerDiffing runs the worker end to end against each observed version,
// with only the observer grants (no full reset available), and checks that
// events hold each window's own workload.
func TestWorkerDiffing(t *testing.T) {
	_, pool := startIdentityDB(t)
	for _, v := range []int{14, 18} {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			rig := newDiffRig(t, v, pool)

			// Subtests run one at a time: they share pg_stat_statements,
			// and one does an outside reset.
			t.Run("windows", func(t *testing.T) {
				logical, physical := rig.source(t, "windows")
				resetBefore := rig.statsReset(t)
				rig.work(t, 7) // before the worker starts: not in any window
				store := openStore(t, t.TempDir())
				defer store.Close()

				rn := rig.start(t, logical, physical, store)
				base := rn.harvests[0]
				// The first run, with no state, is a baseline.
				rig.expectBaseline(t, logical, base)
				if all, _ := rig.sourceEvents(t, logical); all != 0 {
					t.Errorf("first harvest with no state wrote %d events, want 0", all)
				}

				rig.work(t, 3)
				h1 := rn.window(t)
				rig.expectWindow(t, logical, base, h1, 3)

				rig.work(t, 5)
				h2 := rn.window(t)
				rig.expectWindow(t, logical, h1, h2, 5)

				h3 := rn.window(t)
				rig.expectWindow(t, logical, h2, h3, 0)
				rn.stop(t)

				if got := rig.statsReset(t); !got.Equal(resetBefore) {
					t.Errorf("stats_reset moved from %v to %v; the worker must never reset", resetBefore, got)
				}
				if _, neg := rig.sourceEvents(t, logical); neg != 0 {
					t.Errorf("%d events have negative counters", neg)
				}
				if v >= 17 {
					// The min/max reset ran after the harvests, so the
					// entry's min/max cover less than its counters do.
					var moved bool
					if err := rig.su.QueryRow(context.Background(), `select bool_and(minmax_stats_since > stats_since)
						from pg_stat_statements where query like '%diff_marker%'`).Scan(&moved); err != nil {
						t.Fatal(err)
					}
					if !moved {
						t.Error("minmax_stats_since never moved; the worker didn't call the min/max reset")
					}
				}
			})

			t.Run("outside reset", func(t *testing.T) {
				logical, physical := rig.source(t, "reset")
				store := openStore(t, t.TempDir())
				defer store.Close()
				rn := rig.start(t, logical, physical, store)
				base := rn.harvests[0]

				rig.work(t, 3)
				h1 := rn.window(t)
				rig.expectWindow(t, logical, base, h1, 3)

				// Mid-window: some calls, an outside full reset, then
				// more calls. Only the calls after the reset survive.
				rig.work(t, 2)
				if _, err := rig.su.Exec(context.Background(), `select pg_stat_statements_reset()`); err != nil {
					t.Fatal(err)
				}
				rig.work(t, 4)
				h2 := rn.window(t)
				rig.expectWindow(t, logical, h1, h2, 4)

				rig.work(t, 6)
				h3 := rn.window(t)
				rig.expectWindow(t, logical, h2, h3, 6)
				rn.stop(t)

				if _, neg := rig.sourceEvents(t, logical); neg != 0 {
					t.Errorf("%d events have negative counters", neg)
				}
			})

			t.Run("restart", func(t *testing.T) {
				logical, physical := rig.source(t, "restart")
				dir := t.TempDir()
				store := openStore(t, dir)
				rn := rig.start(t, logical, physical, store)
				base := rn.harvests[0]
				rig.work(t, 3)
				h1 := rn.window(t)
				rig.expectWindow(t, logical, base, h1, 3)
				rn.stop(t)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}

				// Down for a while, with work going on.
				rig.work(t, 4)
				time.Sleep(1100 * time.Millisecond)

				store = openStore(t, dir)
				defer store.Close()
				rn = rig.start(t, logical, physical, store)
				// The first harvest after the restart isn't a baseline:
				// its window starts at the saved snapshot.
				rig.expectWindow(t, logical, h1, rn.harvests[0], 4)
				rn.stop(t)
			})

			t.Run("state errors", func(t *testing.T) {
				logical, physical := rig.source(t, "staterr")
				inner := openStore(t, t.TempDir())
				defer inner.Close()
				fs := &faultStore{inner: inner}
				rn := rig.start(t, logical, physical, fs)
				rig.expectBaseline(t, logical, rn.harvests[0])

				// A failed Load makes the harvest a baseline.
				rig.work(t, 3)
				fs.failLoad.Store(true)
				h1 := rn.window(t)
				fs.failLoad.Store(false)
				rig.expectBaseline(t, logical, h1)

				rig.work(t, 5)
				h2 := rn.window(t)
				rig.expectWindow(t, logical, h1, h2, 5)

				// A failed Save still sends its window, but the stored
				// snapshot is now stale, so the next harvest is a
				// baseline instead of counting this window twice.
				rig.work(t, 2)
				fs.failSave.Store(true)
				h3 := rn.window(t)
				fs.failSave.Store(false)
				rig.expectWindow(t, logical, h2, h3, 2)

				rig.work(t, 6)
				h4 := rn.window(t)
				rig.expectBaseline(t, logical, h4)

				rig.work(t, 1)
				h5 := rn.window(t)
				rig.expectWindow(t, logical, h4, h5, 1)
				rn.stop(t)
			})
		})
	}
}
func TestWorkerOutboxHarvestQueuesValidPayloadsInWindowOrder(t *testing.T) {
	_, pool := startIdentityDB(t)
	rig := newDiffRig(t, 18, pool)
	logical, physical := rig.source(t, "outbox")
	store := openStore(t, t.TempDir())
	defer store.Close()
	rn := rig.startOutbox(t, logical, physical, store)
	base := rn.harvests[0]

	rig.work(t, 1)
	h1 := rn.window(t)
	rig.work(t, 2)
	rn.window(t)
	rig.work(t, 3)
	rn.window(t)
	rn.stop(t)

	first, err := store.NextOutboxBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("outbox is empty, want first queued harvest")
	}
	var msg rottenv1.SubmitHarvestRequest
	if err := proto.Unmarshal(first.Payload, &msg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := harvestlimits.ValidateHarvest(&msg, time.Now().UTC(), harvestlimits.CheckFutureSkew); err != nil {
		t.Fatalf("queued payload failed harvest validation: %v", err)
	}

	client := &fakeSubmitter{}
	sender := NewOutboxSender(store, client, nil)
	if sent, err := sender.Drain(context.Background()); err != nil || sent != 3 {
		t.Fatalf("Drain sent=%d err=%v, want three queued harvests", sent, err)
	}
	if len(client.batchIDs) != 3 {
		t.Fatalf("sent batch count = %d, want 3", len(client.batchIDs))
	}
	var lastEnd int64
	for i, batchID := range client.batchIDs {
		var gotPhysical uint32
		var start, end int64
		if _, err := fmt.Sscanf(batchID, "%d:%d:%d", &gotPhysical, &start, &end); err != nil {
			t.Fatalf("parse batch_id %q: %v", batchID, err)
		}
		if gotPhysical != physical {
			t.Fatalf("batch %d physical = %d, want %d", i, gotPhysical, physical)
		}
		if i == 0 {
			if time.UnixMicro(start).Unix() != base || time.UnixMicro(end).Unix() != h1 {
				t.Fatalf("first batch window seconds = [%d,%d], want [%d,%d]", time.UnixMicro(start).Unix(), time.UnixMicro(end).Unix(), base, h1)
			}
		} else if start != lastEnd {
			t.Fatalf("batch %d starts at %d, want previous end %d; order=%v", i, start, lastEnd, client.batchIDs)
		}
		if end <= start {
			t.Fatalf("batch %d window [%d,%d] is not increasing", i, start, end)
		}
		lastEnd = end
	}
}
