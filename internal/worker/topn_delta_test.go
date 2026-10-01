package worker

import (
	"math"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pgss"
)

// metricSetters set each of the 19 ranked metrics, in deltaMetrics order.
var metricSetters = []func(*pgss.Stat, int64){
	func(s *pgss.Stat, v int64) { s.Calls = v },
	func(s *pgss.Stat, v int64) { s.TotalTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.MinTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.MaxTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.MeanTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.StddevTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.Rows = v },
	func(s *pgss.Stat, v int64) { s.SharedBlksHit = v },
	func(s *pgss.Stat, v int64) { s.SharedBlksRead = v },
	func(s *pgss.Stat, v int64) { s.SharedBlksWritten = v },
	func(s *pgss.Stat, v int64) { s.SharedBlksDirtied = v },
	func(s *pgss.Stat, v int64) { s.LocalBlksHit = v },
	func(s *pgss.Stat, v int64) { s.LocalBlksRead = v },
	func(s *pgss.Stat, v int64) { s.LocalBlksWritten = v },
	func(s *pgss.Stat, v int64) { s.LocalBlksDirtied = v },
	func(s *pgss.Stat, v int64) { s.TempBlksRead = v },
	func(s *pgss.Stat, v int64) { s.TempBlksWritten = v },
	func(s *pgss.Stat, v int64) { s.SharedBlkWriteTime = float64(v) },
	func(s *pgss.Stat, v int64) { s.SharedBlkReadTime = float64(v) },
}

// TestEventFromDeltaNew: on a New delta every field is the entry's current
// value, and min and max aren't lifetime.
func TestEventFromDeltaNew(t *testing.T) {
	s := pgss.Stat{Query: "q", Calls: 1, TotalTime: 2, MinTime: 3, MaxTime: 4, MeanTime: 5, StddevTime: 6,
		Rows: 7, SharedBlksHit: 8, SharedBlksRead: 9, SharedBlksDirtied: 10, SharedBlksWritten: 11,
		LocalBlksHit: 12, LocalBlksRead: 13, LocalBlksDirtied: 14, LocalBlksWritten: 15,
		TempBlksRead: 16, TempBlksWritten: 17, SharedBlkReadTime: 18, SharedBlkWriteTime: 19}
	e := eventFromDelta(pgss.Delta{Stat: s, New: true})
	got := []float64{e.calls, e.total_time, e.min_time, e.max_time, e.mean_time, e.stddev_time,
		e.rows, e.shared_blks_hit, e.shared_blks_read, e.shared_blks_dirtied, e.shared_blks_written,
		e.local_blks_hit, e.local_blks_read, e.local_blks_dirtied, e.local_blks_written,
		e.temp_blks_read, e.temp_blks_written, e.blk_read_time, e.blk_write_time}
	for i, v := range got {
		if v != float64(i+1) {
			t.Errorf("field %d = %v, want %d", i, v, i+1)
		}
	}
	if e.query != "q" || e.stddev_absent || e.minmax_lifetime {
		t.Errorf("query %q, stddev_absent %v, minmax_lifetime %v; want q, false, false", e.query, e.stddev_absent, e.minmax_lifetime)
	}
}

// TestEventFromDeltaWindow: on a diffed delta, the counters are the deltas,
// mean and stddev are the window's, and min and max are lifetime unless a
// min/max reset moved minmax_stats_since.
func TestEventFromDeltaWindow(t *testing.T) {
	// Prev: 4 calls of 10ms. Window: 2 calls of 10ms and 2 of 30ms, so the
	// window mean is 20 and its population stddev 10.
	prev := pgss.Stat{Calls: 4, TotalExecTime: 40, MeanTime: 10, StddevTime: 0}
	cur := pgss.Stat{Calls: 8, TotalExecTime: 120, MeanTime: 15}
	// Current M2 = M2_prev + M2_win + δ²·n_prev·n_win/n = 0 + 400 + 100·4·4/8 = 600.
	cur.StddevTime = math.Sqrt(600.0 / 8)
	cur.MinTime, cur.MaxTime = 10, 30
	d := pgss.Delta{Stat: cur, Prev: &prev}
	d.Calls, d.TotalExecTime, d.TotalTime = 4, 80, 80
	e := eventFromDelta(d)
	if e.calls != 4 || e.total_time != 80 {
		t.Errorf("calls, total = %v, %v; want 4, 80", e.calls, e.total_time)
	}
	approx(t, "mean", e.mean_time, 20)
	approx(t, "stddev", e.stddev_time, 10)
	if e.stddev_absent {
		t.Error("stddev_absent on a clean window")
	}
	if !e.minmax_lifetime || e.min_time != 10 || e.max_time != 30 {
		t.Errorf("min, max, lifetime = %v, %v, %v; want 10, 30, true", e.min_time, e.max_time, e.minmax_lifetime)
	}

	// A min/max reset since the snapshot makes them window-only.
	t0 := time.Unix(100, 0)
	t1 := t0.Add(time.Minute)
	d.Prev = &pgss.Stat{Calls: 4, TotalExecTime: 40, MeanTime: 10, MinmaxStatsSince: &t0}
	d.MinmaxStatsSince = &t1
	if e := eventFromDelta(d); e.minmax_lifetime {
		t.Error("minmax_lifetime after a min/max reset, want false")
	}
}

// TestEventFromDeltaUnreliableStddev: when WindowStats says the stddev can't
// be trusted, the event records it as absent instead of passing it on.
func TestEventFromDeltaUnreliableStddev(t *testing.T) {
	// A huge history and a tiny window: the subtraction is all noise.
	prev := pgss.Stat{Calls: 1e9, TotalExecTime: 1e10, MeanTime: 10, StddevTime: 5}
	cur := prev
	cur.Calls += 2
	cur.TotalExecTime += 20
	d := pgss.Delta{Stat: cur, Prev: &prev}
	d.Calls, d.TotalExecTime, d.TotalTime = 2, 20, 20
	if _, _, ok := pgss.WindowStats(d); ok {
		t.Fatal("WindowStats trusts this stddev; the fixture needs a noisier window")
	}
	e := eventFromDelta(d)
	if !e.stddev_absent || e.stddev_time != 0 {
		t.Errorf("stddev_absent %v, stddev_time %v; want true, 0", e.stddev_absent, e.stddev_time)
	}
}

// TestTopNDeltasPerMetric gives each metric its own winner, checks each is
// kept at n=1, and that a row winning nothing is dropped.
func TestTopNDeltasPerMetric(t *testing.T) {
	var deltas []pgss.Delta
	for i, set := range metricSetters {
		s := pgss.Stat{QueryID: int64(i + 1), Calls: 1}
		set(&s, 10)
		deltas = append(deltas, pgss.Delta{Stat: s, New: true})
	}
	deltas = append(deltas, pgss.Delta{Stat: pgss.Stat{QueryID: 200, Calls: 1}, New: true})
	got := topNDeltas(deltas, 1)
	ids := map[int64]int{}
	for _, d := range got {
		ids[d.QueryID]++
	}
	for i := 1; i <= 19; i++ {
		if ids[int64(i)] != 1 {
			t.Errorf("metric %d's winner appears %d times, want 1", i, ids[int64(i)])
		}
	}
	if ids[200] != 0 || len(got) != 19 {
		t.Errorf("kept %d rows (query 200: %d), want 19 without 200", len(got), ids[200])
	}
}

// TestTopNDeltasKeepsNewAndPrev checks that New and Prev survive selection.
func TestTopNDeltasKeepsNewAndPrev(t *testing.T) {
	// a: huge lifetime mean (MeanTime 1000), but this window's 10 calls took
	// 10 ms in total, so its window mean is 1.
	prevA := pgss.Stat{QueryID: 1, Calls: 1000, TotalExecTime: 1e6, MeanTime: 1000}
	a := pgss.Delta{Stat: pgss.Stat{QueryID: 1, Calls: 10, TotalExecTime: 10, MeanTime: 1000}, Prev: &prevA}
	// b: window mean 50, lifetime mean 50.
	b := pgss.Delta{Stat: pgss.Stat{QueryID: 2, Calls: 1, TotalExecTime: 50, MeanTime: 50}, New: true}
	got := topNDeltas([]pgss.Delta{a, b}, 1)
	if len(got) != 2 || got[0].Prev != &prevA || got[0].New || !got[1].New {
		t.Errorf("New/Prev not preserved: %+v", got)
	}

}

// keptIDs returns the QueryIDs topNDeltas keeps at n=1.
func keptIDs(ds ...pgss.Delta) map[int64]bool {
	ids := map[int64]bool{}
	for _, d := range topNDeltas(ds, 1) {
		ids[d.QueryID] = true
	}
	return ids
}

// In the slot tests below, the rival has QueryID 1 and the row under test has
// QueryID 2. Every other metric ties or goes to the rival (ties break by
// QueryID), so the row under test is kept only if it wins the tested slot.

func TestTopNDeltasLowWindowMeanLoses(t *testing.T) {
	prev := pgss.Stat{QueryID: 2, Calls: 1000, TotalExecTime: 1e6, MeanTime: 1000}
	high := pgss.Delta{Stat: pgss.Stat{QueryID: 2, Calls: 1, TotalExecTime: 1, MeanTime: 1000}, Prev: &prev}
	rival := pgss.Delta{Stat: pgss.Stat{QueryID: 1, Calls: 1, TotalExecTime: 1, MeanTime: 50}, New: true}
	if keptIDs(high, rival)[2] {
		t.Error("high lifetime mean, low window mean won the mean slot")
	}
}

func TestTopNDeltasLifetimeMaxLoses(t *testing.T) {
	// No MinmaxStatsSince, so WindowMinMax reports lifetime = true.
	prev := pgss.Stat{QueryID: 2, Calls: 1, MaxTime: 1e6}
	old := pgss.Delta{Stat: pgss.Stat{QueryID: 2, Calls: 1, MaxTime: 1e6}, Prev: &prev}
	if _, _, lifetime := pgss.WindowMinMax(old); !lifetime {
		t.Fatal("fixture: want a lifetime max")
	}
	rival := pgss.Delta{Stat: pgss.Stat{QueryID: 1, Calls: 1, MaxTime: 5}, New: true}
	if keptIDs(old, rival)[2] {
		t.Error("lifetime max won the max slot")
	}
}

func TestTopNDeltasUnreliableStddevLoses(t *testing.T) {
	// A huge history (1e6 calls, mean 100, stddev 10) plus a 2-call window at
	// the same mean whose M2 is 50, far below minM2Ratio of the cumulative M2.
	np, nw := 1e6, 2.0
	m2p := 100 * np
	prev := pgss.Stat{QueryID: 2, Calls: int64(np), TotalExecTime: 100 * np, StddevTime: 10}
	noisy := pgss.Delta{Stat: pgss.Stat{QueryID: 2, Calls: int64(nw), TotalExecTime: 100 * nw,
		StddevTime: math.Sqrt((m2p + 50) / (np + nw))}, Prev: &prev}
	_, sd, ok := pgss.WindowStats(noisy)
	if ok || sd <= 1 {
		t.Fatalf("fixture: got stddev %v, ok %v; want an unreliable stddev above 1", sd, ok)
	}
	rival := pgss.Delta{Stat: pgss.Stat{QueryID: 1, Calls: 2, TotalExecTime: 200, MeanTime: 100, StddevTime: 1}, New: true}
	if keptIDs(noisy, rival)[2] {
		t.Error("unreliable stddev won the stddev slot")
	}
}

func TestTopNDeltasTiesBreakByKey(t *testing.T) {
	mk := func(k pgss.Key) pgss.Delta {
		return pgss.Delta{Stat: pgss.Stat{QueryID: k.QueryID, UserID: k.UserID, DBID: k.DBID,
			TopLevel: k.TopLevel, Calls: 1}, New: true}
	}
	for _, c := range []struct{ win, lose pgss.Key }{
		{pgss.Key{QueryID: 3}, pgss.Key{QueryID: 7}},
		{pgss.Key{QueryID: 3, UserID: 1}, pgss.Key{QueryID: 3, UserID: 2}},
		{pgss.Key{QueryID: 3, DBID: 1}, pgss.Key{QueryID: 3, DBID: 2}},
		{pgss.Key{QueryID: 3}, pgss.Key{QueryID: 3, TopLevel: true}},
	} {
		for _, in := range [][]pgss.Delta{{mk(c.win), mk(c.lose)}, {mk(c.lose), mk(c.win)}} {
			got := topNDeltas(in, 1)
			if len(got) != 1 || pgss.KeyOf(got[0].Stat) != c.win {
				t.Errorf("ties %v vs %v: kept %+v", c.win, c.lose, got)
			}
		}
	}
}
