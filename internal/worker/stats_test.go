package worker

import (
	"context"
	"math"
	"testing"
	"time"

	runningstat "github.com/benchub/runningstat"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stepper replaces Worker.statsWait so a test decides when reportSamples runs a
// pass. reportSamples sends on waiting each time it reaches the wait, then
// blocks until the test sends true (run a pass) or false (return).
type stepper struct {
	waiting chan time.Duration
	tick    chan bool
	done    chan struct{}
}

// installStepper sets w.statsWait to a stepper. Call it before starting
// anything that runs reportSamples.
func installStepper(t *testing.T, w *Worker) *stepper {
	t.Helper()
	s := &stepper{waiting: make(chan time.Duration), tick: make(chan bool), done: make(chan struct{})}
	w.statsWait = func(d time.Duration) bool {
		s.waiting <- d
		return <-s.tick
	}
	return s
}

// arrive waits until reportSamples reaches its wait and returns the duration
// it asked for.
func (s *stepper) arrive(t *testing.T) time.Duration {
	t.Helper()
	select {
	case d := <-s.waiting:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("reportSamples never reached its wait")
		return 0
	}
}

// step runs one reportSamples pass and waits for it to finish.
func (s *stepper) step(t *testing.T) {
	t.Helper()
	s.tick <- true
	s.arrive(t)
}

// stop makes reportSamples return and waits for it.
func (s *stepper) stop(t *testing.T) {
	t.Helper()
	s.tick <- false
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		t.Fatal("reportSamples didn't return")
	}
}

// startReport starts reportSamples with the stepper installed and waits for
// it to reach its first wait.
func startReport(t *testing.T, s *stepper, w *Worker, pool *pgxpool.Pool, f *Fingerprint, logical uint32, interval uint32) time.Duration {
	t.Helper()
	go func() {
		w.reportSamples(pool, f, logical, interval)
		close(s.done)
	}()
	return s.arrive(t)
}

// newTestFingerprint inserts a fingerprints row and builds a Fingerprint the
// same way processEvent does. It isn't registered in any Worker.
func newTestFingerprint(t *testing.T, pool *pgxpool.Pool, fingerprint string) *Fingerprint {
	t.Helper()
	var id uint64
	if err := pool.QueryRow(context.Background(),
		`insert into rotten.fingerprints (fingerprint, normalized) values ($1, $1) returning id`,
		fingerprint).Scan(&id); err != nil {
		t.Fatal(err)
	}
	f := &Fingerprint{db_id: id, stats: make(map[string]*runningstat.RunningStat)}
	for _, d := range knownStatsDomains {
		f.stats[d] = &runningstat.RunningStat{}
	}
	return f
}

// sampleOf gives every stats domain the value v*(i+1), where i is the
// domain's index, so each of the 19 rows carries different numbers.
func sampleOf(v float64, unixtime int64) *Samples {
	s := &Samples{metrics: make(map[string]float64), unixtime: unixtime}
	for i, d := range knownStatsDomains {
		s.metrics[d] = v * float64(i+1)
	}
	return s
}

// feed runs consumeSamples in this goroutine over the given samples. The
// channel is closed, so consumeSamples returns once it has drained it.
func feed(f *Fingerprint, samples ...*Samples) {
	ch := make(chan *Samples, len(samples))
	for _, s := range samples {
		ch <- s
	}
	close(ch)
	f.samples = ch
	consumeSamples(f)
}

type statRow struct {
	count     int64
	mean, dev float64
	last      int64
}

func statRows(t *testing.T, pool *pgxpool.Pool, fp uint64, source uint32) map[string]statRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`select type::text, count, mean, deviation, last from rotten.fingerprint_stats
		 where fingerprint_id=$1 and logical_source_id=$2`, fp, source)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]statRow{}
	for rows.Next() {
		var typ string
		var r statRow
		if err := rows.Scan(&typ, &r.count, &r.mean, &r.dev, &r.last); err != nil {
			t.Fatal(err)
		}
		got[typ] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

// checkStats asserts all 19 rows for source have count n, last, and mean and
// deviation equal to scale*(i+1) times the given base values.
func checkStats(t *testing.T, pool *pgxpool.Pool, fp uint64, source uint32, n int64, mean, dev float64, last int64) {
	t.Helper()
	got := statRows(t, pool, fp, source)
	if len(got) != len(knownStatsDomains) {
		t.Fatalf("source %d: %d fingerprint_stats rows, want %d", source, len(got), len(knownStatsDomains))
	}
	for i, d := range knownStatsDomains {
		k := float64(i + 1)
		r := got[d]
		if r.count != n || !near(r.mean, mean*k) || !near(r.dev, dev*k) || r.last != last {
			t.Errorf("source %d %s = count %d mean %v dev %v last %d, want count %d mean %v dev %v last %d",
				source, d, r.count, r.mean, r.dev, r.last, n, mean*k, dev*k, last)
		}
	}
}

func TestConsumeSamplesPushesEveryDomain(t *testing.T) {
	f := &Fingerprint{stats: make(map[string]*runningstat.RunningStat)}
	for _, d := range knownStatsDomains {
		f.stats[d] = &runningstat.RunningStat{}
	}
	feed(f, sampleOf(1, 100), sampleOf(2, 200), sampleOf(6, 150))

	// last is the most recent sample's time, not the largest.
	if f.last != 150 {
		t.Errorf("last = %d, want 150", f.last)
	}
	// calls is index 0 (scale 1), total_time index 1 (scale 2).
	if f.calls_since_start != 9 || f.time_since_start != 18 {
		t.Errorf("calls,time since start = %v,%v, want 9,18", f.calls_since_start, f.time_since_start)
	}
	// 1, 2, 6: mean 3, sample variance (4+1+9)/2 = 7.
	for i, d := range knownStatsDomains {
		k := float64(i + 1)
		s := f.stats[d]
		if s.RunningStatCount() != 3 || !near(s.RunningStatMean(), 3*k) || !near(s.RunningStatDeviation(), math.Sqrt(7)*k) {
			t.Errorf("%s = count %d mean %v dev %v, want 3, %v, %v", d, s.RunningStatCount(), s.RunningStatMean(), s.RunningStatDeviation(), 3*k, math.Sqrt(7)*k)
		}
	}
}

func TestReportSamplesInsertsThenMerges(t *testing.T) {
	pf := startProcessDB(t)
	s := installStepper(t, pf.w)
	f := newTestFingerprint(t, pf.pool, "select stats")
	if d := startReport(t, s, pf.w, pf.pool, f, pf.logical, 7); d != 14*time.Second {
		t.Errorf("wait = %v, want 2*observation_interval = 14s", d)
	}
	defer s.stop(t)

	// Nothing consumed yet, so f.last isn't newer and the pass writes nothing.
	s.step(t)
	if n := count(t, pf.pool, "select count(*) from rotten.fingerprint_stats"); n != 0 {
		t.Fatalf("pass with no samples wrote %d rows, want 0", n)
	}

	// First flush: 1, 2, 6 gives count 3, mean 3, deviation sqrt(7).
	feed(f, sampleOf(1, 1000), sampleOf(2, 1001), sampleOf(6, 1002))
	s.step(t)
	for _, src := range []uint32{0, pf.logical} {
		checkStats(t, pf.pool, f.db_id, src, 3, 3, math.Sqrt(7), 1002)
	}
	if c := f.stats["calls"].RunningStatCount(); c != 0 {
		t.Errorf("in-memory count after flush = %d, want 0 (reset)", c)
	}

	// No new samples: nothing changes.
	s.step(t)
	checkStats(t, pf.pool, f.db_id, 0, 3, 3, math.Sqrt(7), 1002)

	// Second flush: 5, 5, 5 merged with 1, 2, 6. Mean 24/6 = 4. Squared
	// deviations 9+4+4+1+1+1 = 20, sample variance 20/5 = 4, deviation 2.
	feed(f, sampleOf(5, 2000), sampleOf(5, 2001), sampleOf(5, 2002))
	s.step(t)
	for _, src := range []uint32{0, pf.logical} {
		checkStats(t, pf.pool, f.db_id, src, 6, 4, 2, 2002)
	}
}

func TestReportSamplesMergesExistingRows(t *testing.T) {
	pf := startProcessDB(t)
	s := installStepper(t, pf.w)
	f := newTestFingerprint(t, pf.pool, "select seeded")
	// Seed source 0 with count 3, mean 6, deviation 1 for each domain
	// (scaled). Init reads that as squared-deviation sum 1*1*(3-1) = 2.
	for i, d := range knownStatsDomains {
		k := float64(i + 1)
		if _, err := pf.pool.Exec(context.Background(),
			`insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count, mean, deviation, last)
			 values ($1, 0, $2, 3, $3, $4, 5)`, f.db_id, d, 6*k, 1*k); err != nil {
			t.Fatal(err)
		}
	}
	startReport(t, s, pf.w, pf.pool, f, pf.logical, 7)
	defer s.stop(t)

	// Two 7s: mean (18+14)/5 = 6.4. Sum of squares 2 + 0 + 2*3*1/5 = 3.2,
	// variance 3.2/4 = 0.8.
	feed(f, sampleOf(7, 3000), sampleOf(7, 3001))
	s.step(t)
	checkStats(t, pf.pool, f.db_id, 0, 5, 6.4, math.Sqrt(0.8), 3001)
	// The logical source had no rows, so it gets only the new samples.
	checkStats(t, pf.pool, f.db_id, pf.logical, 2, 7, 0, 3001)
}

func TestReportSamplesPartialRowsRollBack(t *testing.T) {
	pf := startProcessDB(t)
	s := installStepper(t, pf.w)
	f := newTestFingerprint(t, pf.pool, "select partial")
	// Only five of the 19 types exist for source 0.
	for _, d := range knownStatsDomains[:5] {
		if _, err := pf.pool.Exec(context.Background(),
			`insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count, mean, deviation, last)
			 values ($1, 0, $2, 10, 1, 0, 5)`, f.db_id, d); err != nil {
			t.Fatal(err)
		}
	}
	startReport(t, s, pf.w, pf.pool, f, pf.logical, 7)
	defer s.stop(t)

	feed(f, sampleOf(1, 4000), sampleOf(3, 4001))
	s.step(t)

	got := statRows(t, pf.pool, f.db_id, 0)
	if len(got) != 5 {
		t.Fatalf("source 0 rows = %d, want the 5 seeded rows untouched", len(got))
	}
	for _, d := range knownStatsDomains[:5] {
		if r := got[d]; r.count != 10 || r.mean != 1 || r.dev != 0 || r.last != 5 {
			t.Errorf("source 0 %s = %+v, want the seeded row unchanged", d, r)
		}
	}
	// The logical source is its own transaction, so it still gets inserted.
	// 1, 3: mean 2, variance 2.
	checkStats(t, pf.pool, f.db_id, pf.logical, 2, 2, math.Sqrt(2), 4001)
	// The in-memory stats are reset even though source 0 rolled back.
	if c := f.stats["calls"].RunningStatCount(); c != 0 {
		t.Errorf("in-memory count after partial flush = %d, want 0 (dropped)", c)
	}
}

func TestProcessEventNewFingerprintStartsGoroutines(t *testing.T) {
	pf := startProcessDB(t)
	s := installStepper(t, pf.w)
	query := "select * from t where a = 5"
	e := newEvent(query, 4, 8, map[string]uint32{})
	e.rows = 9

	done := make(chan struct{})
	go func() {
		pf.w.processEvent(pf.pool, pf.logical, pf.physical, 7, query, e)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processEvent blocked delivering the first sample")
	}
	if d := s.arrive(t); d != 14*time.Second {
		t.Errorf("wait = %v, want 14s", d)
	}

	var id uint64
	var normalized string
	if err := pf.pool.QueryRow(context.Background(),
		`select id, normalized from rotten.fingerprints where fingerprint=$1`, query).Scan(&id, &normalized); err != nil {
		t.Fatalf("fingerprint row: %v", err)
	}
	if normalized != "select * from t where a = $1" {
		t.Errorf("normalized = %q, want %q", normalized, "select * from t where a = $1")
	}
	pf.w.fingerprintsMu.RLock()
	f := pf.w.fingerprints[id]
	pf.w.fingerprintsMu.RUnlock()
	if f == nil {
		t.Fatal("processEvent didn't register the new fingerprint")
	}
	if f.db_id != id {
		t.Errorf("db_id = %d, want %d", f.db_id, id)
	}

	// reportSamples copies f.last into lastReport when it starts, and
	// consumeSamples may record processEvent's sample before or after that
	// copy. If it lands first, the next pass sees no change and flushes
	// nothing (task 20261001-120544-1). reportSamples is parked in its wait,
	// so it has made its copy. A second sample with a later time makes f.last
	// newer than lastReport either way.
	second := &Samples{metrics: map[string]float64{"calls": 4, "rows": 9}, unixtime: time.Now().Unix() + 100}
	f.samples <- second

	// Wait until consumeSamples has taken the lock and recorded it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		f.statsLock.RLock()
		last := f.last
		f.statsLock.RUnlock()
		if last == second.unixtime {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumeSamples never recorded the sample")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.statsLock.RLock()
	last := f.last
	f.statsLock.RUnlock()

	s.step(t)
	close(f.samples)
	// processEvent started this reportSamples, so there's no done channel.
	// Returning false makes it return without touching the database.
	s.tick <- false

	rows := statRows(t, pf.pool, id, 0)
	if len(rows) != len(knownStatsDomains) {
		t.Fatalf("source 0 rows = %d, want 19", len(rows))
	}
	if r := rows["calls"]; r.count != 2 || r.mean != 4 || r.last != last {
		t.Errorf("calls row = %+v, want count 2 mean 4 last %d", r, last)
	}
	if r := rows["rows"]; r.mean != 9 {
		t.Errorf("rows row mean = %v, want 9", r.mean)
	}
	if n := len(statRows(t, pf.pool, id, pf.logical)); n != len(knownStatsDomains) {
		t.Errorf("logical source rows = %d, want 19", n)
	}
	if v := pf.w.stillProcessing(); v != 0 {
		t.Errorf("stillProcessing = %d, want 0", v)
	}
}

// TestReportSamplesWhileConsuming feeds samples from another goroutine while
// reportSamples runs its passes, the way production does. Under -race this
// catches reportSamples reading f.last without the lock. Every sample is
// flushed exactly once, so the rows end up holding all of them.
func TestReportSamplesWhileConsuming(t *testing.T) {
	pf := startProcessDB(t)
	s := installStepper(t, pf.w)
	f := newTestFingerprint(t, pf.pool, "select concurrent")
	ch := make(chan *Samples)
	f.samples = ch
	consumed := make(chan struct{})
	go func() {
		consumeSamples(f)
		close(consumed)
	}()
	startReport(t, s, pf.w, pf.pool, f, pf.logical, 7)
	defer s.stop(t)

	const n = 50
	go func() {
		for i := 0; i < n; i++ {
			ch <- sampleOf(1, int64(1000+i))
		}
		close(ch)
	}()
	for i := 0; i < 10; i++ {
		s.step(t)
	}
	<-consumed
	// One last pass flushes whatever arrived after the previous one.
	s.step(t)
	for _, src := range []uint32{0, pf.logical} {
		checkStats(t, pf.pool, f.db_id, src, n, 1, 0, 1000+n-1)
	}
}
