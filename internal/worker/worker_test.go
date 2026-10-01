package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/testdb"
)

// stepClock is the real clock, except that each Sleep first reports itself
// on sleeping and waits for the test to send on proceed. Then it really
// sleeps, because the worker's windows are whole seconds and events need
// observed_window_end > observed_window_start. It records every Now.
type stepClock struct {
	sleeping chan time.Duration
	proceed  chan struct{}

	mu   sync.Mutex
	nows []int64
}

func (c *stepClock) Now() time.Time {
	n := time.Now()
	c.mu.Lock()
	c.nows = append(c.nows, n.Unix())
	c.mu.Unlock()
	return n
}

func (c *stepClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case c.sleeping <- d:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-c.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return RealClock{}.Sleep(ctx, d)
}

func (c *stepClock) times() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.nows...)
}

// waitSleep waits for the worker to reach a Sleep and returns its duration.
func (c *stepClock) waitSleep(t *testing.T) time.Duration {
	t.Helper()
	select {
	case d := <-c.sleeping:
		return d
	case <-time.After(30 * time.Second):
		t.Fatal("worker never reached its sleep")
		return 0
	}
}

// parkStats makes every reportSamples goroutine w starts wait until the test
// ends and then return without touching the database. This test checks
// events, contexts, and fingerprints. stats_test.go covers fingerprint_stats.
// Call it before starting w. statsWait belongs to w, so no other test's
// goroutines can see it.
func parkStats(t *testing.T, w *Worker) {
	done := make(chan struct{})
	w.statsWait = func(time.Duration) bool {
		<-done
		return false
	}
	t.Cleanup(func() { close(done) })
}

// startObservedForWorker starts Postgres 16 with the dba functions from
// schema/functions-pg13.sql, owned by postgres, and an unprivileged
// rotten_observer login that may only call them.
func startObservedForWorker(t *testing.T) *testdb.DB {
	t.Helper()
	db := testdb.StartObserved(t, 16)
	conn := db.Connect(t)
	sql, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "schema", "functions-pg13.sql"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, s := range []string{
		"create schema dba",
		"set search_path = dba, public",
		string(sql),
		"reset search_path",
		"create role rotten_observer login password 'rotten_observer'",
		"grant usage on schema dba to rotten_observer",
		"create table widgets (id int primary key, name text)",
		"insert into widgets select g, 'w' || g from generate_series(1, 10) g",
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%.60s: %v", s, err)
		}
	}
	return db
}

func observerConn(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// The workload. The two IN queries are separate pg_stat_statements entries
// but one rotten fingerprint, so the worker merges them and their contexts.
var (
	workloadShow  = `select * from widgets where id in (1, 2) /*controller:users,action:show*/`
	workloadJob   = `select * from widgets where id in (1, 2, 3) /*job:CleanupJob,*/`
	workloadCount = `select count(*) from widgets`
)

func runWorkload(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, w := range []struct {
		q string
		n int
	}{{workloadShow, 3}, {workloadJob, 2}, {workloadCount, 4}} {
		for i := 0; i < w.n; i++ {
			// Simple protocol, so the literals reach pg_stat_statements as
			// written and it normalizes them.
			rows, err := conn.Query(context.Background(), w.q, pgx.QueryExecModeSimpleProtocol)
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func fingerprintOf(t *testing.T, query string) string {
	t.Helper()
	fp, err := fingerprinting.Normalized(query)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

// waitWorkload polls until both workload fingerprints have an event and those
// events have their three event_context rows. processEvent writes each event
// and its contexts in one transaction, so this is a complete state.
func waitWorkload(t *testing.T, pool *pgxpool.Pool, fps ...string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var events, contexts int
		if err := pool.QueryRow(context.Background(), `
			with e as (select e.id from rotten.events e join rotten.fingerprints f on f.id=e.fingerprint_id
			           where f.fingerprint = any($1))
			select (select count(*) from e),
			       (select count(*) from rotten.event_context c where c.event_id in (select id from e))`,
			fps).Scan(&events, &contexts); err != nil {
			t.Fatal(err)
		}
		if events >= len(fps) && contexts >= 3 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("worker never wrote the workload's events and contexts")
}

func TestWorkerEndToEnd(t *testing.T) {
	observed := startObservedForWorker(t)
	rotten, _ := startIdentityDB(t)

	pcfg, err := pgxpool.ParseConfig(rotten.DSN)
	if err != nil {
		t.Fatal(err)
	}
	pcfg.MaxConnLifetime = 10 * time.Second
	pcfg.MaxConns = 5
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(context.Background(), pcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	var logical, physical uint32
	if err := pool.QueryRow(context.Background(), `insert into logical_sources (project,environment,cluster,role) values ('p','e','c','r') returning id`).Scan(&logical); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `insert into physical_sources (fqdn) values ('db1.example') returning id`).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	reC, reA, reJ := sampleRegexes(t)

	obsDSN := observed.DSNAs(t, "rotten_observer")
	cfg := Config{
		RottenDB:            pool,
		ObservedDB:          observerConn(t, obsDSN),
		ObservedDBReset:     observerConn(t, obsDSN),
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           logical,
		PhysicalID:          physical,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
	}
	clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
	w := New(cfg, clk)
	parkStats(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan error, 1)

	// main starts ReportProgress next to Run, so do the same here: it reads
	// the counters Run writes. It logs once a second until the test ends.
	// noIdleHands is off so it never panics.
	progressCtx, stopProgress := context.WithCancel(context.Background())
	progressDone := make(chan struct{})
	go func() {
		w.ReportProgress(progressCtx, false, 1)
		close(progressDone)
	}()
	t.Cleanup(func() {
		stopProgress()
		select {
		case <-progressDone:
		case <-time.After(10 * time.Second):
			t.Error("ReportProgress didn't return after its context ended")
		}
	})
	go func() { ran <- w.Run(ctx) }()

	// The first sleep follows the initial reset, so the window is open.
	if d := clk.waitSleep(t); d != 2*time.Second {
		t.Errorf("first sleep = %v, want the 2s window", d)
	}
	workload := observed.Connect(t)
	runWorkload(t, workload)
	clk.proceed <- struct{}{}

	// The next sleep is the slackoff after the first window is processed.
	if d := clk.waitSleep(t); d <= 0 || d > 2*time.Second {
		t.Errorf("slackoff = %v, want in (0, 2s]", d)
	}

	// Now calls: window start, window end, next window start, then the
	// slackoff check and computation.
	nows := clk.times()
	if len(nows) < 3 {
		t.Fatalf("clock read %d times, want at least 3", len(nows))
	}
	wantStart, wantEnd := nows[0], nows[1]
	if wantEnd <= wantStart {
		t.Fatalf("window [%d,%d] is empty", wantStart, wantEnd)
	}

	inFP := fingerprintOf(t, `select * from widgets where id in ($1, $2)`)
	if other := fingerprintOf(t, `select * from widgets where id in ($1, $2, $3)`); other != inFP {
		t.Fatalf("IN-list queries fingerprint differently (%s, %s), so they won't merge", inFP, other)
	}
	countFP := fingerprintOf(t, workloadCount)
	waitWorkload(t, pool, inFP, countFP)

	type evt struct {
		id                uint64
		logical, physical uint32
		ws, we            int64
		calls             float64
		time              float64
		normalized        string
	}
	eventFor := func(fp string) evt {
		t.Helper()
		var e evt
		var n int
		if err := pool.QueryRow(context.Background(), `select count(*) from rotten.events e join rotten.fingerprints f on f.id=e.fingerprint_id where f.fingerprint=$1`, fp).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("fingerprint %s has %d events, want 1", fp, n)
		}
		if err := pool.QueryRow(context.Background(), `select e.id, e.logical_source_id, e.physical_source_id,
			extract(epoch from e.observed_window_start)::bigint, extract(epoch from e.observed_window_end)::bigint,
			e.calls, e.time, f.normalized
			from rotten.events e join rotten.fingerprints f on f.id=e.fingerprint_id where f.fingerprint=$1`, fp).
			Scan(&e.id, &e.logical, &e.physical, &e.ws, &e.we, &e.calls, &e.time, &e.normalized); err != nil {
			t.Fatal(err)
		}
		if e.logical != logical || e.physical != physical {
			t.Errorf("%s sources = (%d,%d), want (%d,%d)", fp, e.logical, e.physical, logical, physical)
		}
		if e.ws != wantStart || e.we != wantEnd {
			t.Errorf("%s window = [%d,%d], want [%d,%d]", fp, e.ws, e.we, wantStart, wantEnd)
		}
		if e.time <= 0 {
			t.Errorf("%s time = %v, want > 0", fp, e.time)
		}
		return e
	}

	in := eventFor(inFP)
	if in.calls != 5 {
		t.Errorf("merged IN event calls = %v, want 3+2", in.calls)
	}
	// The first pg_stat_statements row for the fingerprint names it, and the
	// row order isn't fixed. pg_query.Normalize keeps the comment.
	if in.normalized != `select * from widgets where id in ($1, $2) /*controller:users,action:show*/` &&
		in.normalized != `select * from widgets where id in ($1, $2, $3) /*job:CleanupJob,*/` {
		t.Errorf("IN normalized = %q, want one of the two workload queries", in.normalized)
	}
	cnt := eventFor(countFP)
	if cnt.calls != 4 {
		t.Errorf("count event calls = %v, want 4", cnt.calls)
	}
	if cnt.normalized != workloadCount {
		t.Errorf("count normalized = %q, want %q", cnt.normalized, workloadCount)
	}

	id := func(table, col, val string) uint32 {
		t.Helper()
		var v uint32
		if err := pool.QueryRow(context.Background(), fmt.Sprintf("select id from rotten.%s where %s=$1", table, col), val).Scan(&v); err != nil {
			t.Fatalf("%s %q: %v", table, val, err)
		}
		return v
	}
	users, show, job := id("controllers", "controller", "users"), id("actions", "action", "show"), id("job_tags", "job_tag", "CleanupJob")

	var all []contextRow
	for _, r := range contextRows(t, pool) {
		if r.event == in.id || r.event == cnt.id {
			all = append(all, r)
		}
	}
	// contextRows orders by c.
	want := []struct {
		event         uint64
		c             uint32
		ctl, act, job any
	}{
		{in.id, 2, nil, nil, job},
		{in.id, 3, users, show, nil},
		{cnt.id, 4, nil, nil, nil},
	}
	if len(all) != len(want) {
		t.Fatalf("workload event_context rows = %+v, want %d", all, len(want))
	}
	for i, w := range want {
		r := all[i]
		if r.event != w.event || r.c != w.c || ptr(r.ctl) != w.ctl || ptr(r.act) != w.act || ptr(r.job) != w.job {
			t.Errorf("context %d = event %d c %d ctl %v act %v job %v, want event %d c %d ctl %v act %v job %v",
				i, r.event, r.c, ptr(r.ctl), ptr(r.act), ptr(r.job), w.event, w.c, w.ctl, w.act, w.job)
		}
		if r.ws != wantStart || r.we != wantEnd {
			t.Errorf("context %d window = [%d,%d], want [%d,%d]", i, r.ws, r.we, wantStart, wantEnd)
		}
	}

	// Cancelling ctx stops the worker in its slackoff sleep.
	cancel()
	select {
	case err := <-ran:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("run returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run didn't return after cancel")
	}
	before := count(t, pool, "select count(*) from rotten.events")
	// One more window: a worker that ignored cancel would have written by now.
	time.Sleep(2 * time.Second)
	if after := count(t, pool, "select count(*) from rotten.events"); after != before {
		t.Errorf("events grew from %d to %d after run returned", before, after)
	}
}

// TestReportProgressStops checks the stop hook: ReportProgress returns once
// its context ends, from its sleep between reports.
func TestReportProgressStops(t *testing.T) {
	w := New(Config{ObservationInterval: 2}, RealClock{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.ReportProgress(ctx, false, 3600)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ReportProgress didn't return after its context ended")
	}
}
