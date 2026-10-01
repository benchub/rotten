package worker

import (
	"testing"

	"github.com/benchub/rotten/internal/pgss"
)

// metricSetters set each of the 19 ranked metrics, in topMetrics order.
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

// TestTopN gives each of the 19 metrics its own winning row (QueryID 1-19,
// value 10), plus a runner-up in every metric (QueryID 100, value 5) and a
// row that's last in everything (QueryID 200).
func TestTopN(t *testing.T) {
	if len(metricSetters) != 19 || len(topMetrics) != 19 {
		t.Fatalf("got %d setters and %d metrics, want 19", len(metricSetters), len(topMetrics))
	}
	var stats []pgss.Stat
	for i, set := range metricSetters {
		s := pgss.Stat{QueryID: int64(i + 1)}
		set(&s, 10)
		stats = append(stats, s)
	}
	runnerUp := pgss.Stat{QueryID: 100}
	for _, set := range metricSetters {
		set(&runnerUp, 5)
	}
	stats = append(stats, runnerUp, pgss.Stat{QueryID: 200})

	check := func(n int, want []int64) {
		t.Helper()
		got := topN(stats, n)
		ids := map[int64]int{}
		for _, s := range got {
			ids[s.QueryID]++
		}
		if len(got) != len(want) {
			t.Errorf("topN(%d) kept %d rows, want %d", n, len(got), len(want))
		}
		for _, id := range want {
			if ids[id] != 1 {
				t.Errorf("topN(%d): query %d appears %d times, want 1", n, id, ids[id])
			}
		}
	}
	var winners []int64
	for i := 1; i <= 19; i++ {
		winners = append(winners, int64(i))
	}
	check(1, winners)
	check(2, append(append([]int64{}, winners...), 100))
	check(100, append(append([]int64{}, winners...), 100, 200))
}

func TestEventFromStat(t *testing.T) {
	s := pgss.Stat{Query: "q", Calls: 1, TotalTime: 2, MinTime: 3, MaxTime: 4, MeanTime: 5, StddevTime: 6,
		Rows: 7, SharedBlksHit: 8, SharedBlksRead: 9, SharedBlksDirtied: 10, SharedBlksWritten: 11,
		LocalBlksHit: 12, LocalBlksRead: 13, LocalBlksDirtied: 14, LocalBlksWritten: 15,
		TempBlksRead: 16, TempBlksWritten: 17, SharedBlkReadTime: 18, SharedBlkWriteTime: 19}
	e := eventFromStat(s)
	got := []float64{e.calls, e.total_time, e.min_time, e.max_time, e.mean_time, e.stddev_time,
		e.rows, e.shared_blks_hit, e.shared_blks_read, e.shared_blks_dirtied, e.shared_blks_written,
		e.local_blks_hit, e.local_blks_read, e.local_blks_dirtied, e.local_blks_written,
		e.temp_blks_read, e.temp_blks_written, e.blk_read_time, e.blk_write_time}
	for i, v := range got {
		if v != float64(i+1) {
			t.Errorf("field %d = %v, want %d", i, v, i+1)
		}
	}
	if e.query != "q" {
		t.Errorf("query = %q", e.query)
	}
}
