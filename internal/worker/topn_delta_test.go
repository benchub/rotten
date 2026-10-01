package worker

import (
	"math"
	"math/rand"
	"testing"

	"github.com/benchub/rotten/internal/pgss"
)

// TestTopNDeltasMatchesTopN builds rows with distinct values in every metric
// (so there are no ties at the cutoff), wraps them as New deltas, and checks
// that topNDeltas picks exactly what topN picks from the same rows.
func TestTopNDeltasMatchesTopN(t *testing.T) {
	if len(deltaMetrics) != len(topMetrics) {
		t.Fatalf("got %d delta metrics, want %d", len(deltaMetrics), len(topMetrics))
	}
	r := rand.New(rand.NewSource(1))
	const rows = 400
	stats := make([]pgss.Stat, rows)
	for i := range stats {
		stats[i].QueryID = int64(i + 1)
		stats[i].Calls = 1 // MeanTime is set directly; Calls only needs to be nonzero.
	}
	for _, set := range metricSetters {
		for i, v := range r.Perm(rows) {
			set(&stats[i], int64(v+1))
		}
	}
	deltas := make([]pgss.Delta, rows)
	for i, s := range stats {
		deltas[i] = pgss.Delta{Stat: s, New: true}
	}
	for _, n := range []int{1, 5, 100} {
		want := map[int64]bool{}
		for _, s := range topN(stats, n) {
			want[s.QueryID] = true
		}
		got := topNDeltas(deltas, n)
		if len(got) != len(want) {
			t.Errorf("n=%d: kept %d, want %d", n, len(got), len(want))
		}
		for _, d := range got {
			if !want[d.QueryID] {
				t.Errorf("n=%d: kept query %d, topN didn't", n, d.QueryID)
			}
		}
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
