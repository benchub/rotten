package worker

import (
	"context"
	"log"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pgss"
)

// resetWindow starts a new observation window with a full reset, through the
// legacy dba.pg_stat_statements_user_reset() from schema/legacy_reset.sql.
//
// TODO(20261001-103222-25): drop the full reset once diffing replaces it.
// On 17+, call the observer's min/max wrapper instead (see -21).
func resetWindow(conn *pgx.Conn) {
	if _, err := conn.Exec(context.Background(), `select dba.pg_stat_statements_user_reset()`); err != nil {
		log.Fatalln("couldn't reset pg_stat_statements", err)
	}
}

// topMetrics are the metrics the worker ranks entries by, the same 19 the old
// SQL UNION used.
var topMetrics = []func(*pgss.Stat) float64{
	func(s *pgss.Stat) float64 { return float64(s.Calls) },
	func(s *pgss.Stat) float64 { return s.TotalTime },
	func(s *pgss.Stat) float64 { return s.MinTime },
	func(s *pgss.Stat) float64 { return s.MaxTime },
	func(s *pgss.Stat) float64 { return s.MeanTime },
	func(s *pgss.Stat) float64 { return s.StddevTime },
	func(s *pgss.Stat) float64 { return float64(s.Rows) },
	func(s *pgss.Stat) float64 { return float64(s.SharedBlksHit) },
	func(s *pgss.Stat) float64 { return float64(s.SharedBlksRead) },
	func(s *pgss.Stat) float64 { return float64(s.SharedBlksWritten) },
	func(s *pgss.Stat) float64 { return float64(s.SharedBlksDirtied) },
	func(s *pgss.Stat) float64 { return float64(s.LocalBlksHit) },
	func(s *pgss.Stat) float64 { return float64(s.LocalBlksRead) },
	func(s *pgss.Stat) float64 { return float64(s.LocalBlksWritten) },
	func(s *pgss.Stat) float64 { return float64(s.LocalBlksDirtied) },
	func(s *pgss.Stat) float64 { return float64(s.TempBlksRead) },
	func(s *pgss.Stat) float64 { return float64(s.TempBlksWritten) },
	func(s *pgss.Stat) float64 { return s.SharedBlkWriteTime },
	func(s *pgss.Stat) float64 { return s.SharedBlkReadTime },
}

// topN returns the union of the top n entries for each metric, each entry
// once, in input order. Ties at the cutoff are broken arbitrarily, as the old
// SQL LIMIT did. Task -22 owns the rewrite of this selection.
func topN(stats []pgss.Stat, n int) []pgss.Stat {
	keep := make([]bool, len(stats))
	idx := make([]int, len(stats))
	for _, m := range topMetrics {
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool { return m(&stats[idx[a]]) > m(&stats[idx[b]]) })
		for _, i := range idx[:min(n, len(idx))] {
			keep[i] = true
		}
	}
	var out []pgss.Stat
	for i, k := range keep {
		if k {
			out = append(out, stats[i])
		}
	}
	return out
}

// eventFromStat maps a Stat onto the worker's event fields.
func eventFromStat(s pgss.Stat) QueryEvent {
	return QueryEvent{
		query:               s.Query,
		calls:               float64(s.Calls),
		total_time:          s.TotalTime,
		min_time:            s.MinTime,
		max_time:            s.MaxTime,
		mean_time:           s.MeanTime,
		stddev_time:         s.StddevTime,
		rows:                float64(s.Rows),
		shared_blks_hit:     float64(s.SharedBlksHit),
		shared_blks_read:    float64(s.SharedBlksRead),
		shared_blks_dirtied: float64(s.SharedBlksDirtied),
		shared_blks_written: float64(s.SharedBlksWritten),
		local_blks_hit:      float64(s.LocalBlksHit),
		local_blks_read:     float64(s.LocalBlksRead),
		local_blks_dirtied:  float64(s.LocalBlksDirtied),
		local_blks_written:  float64(s.LocalBlksWritten),
		temp_blks_read:      float64(s.TempBlksRead),
		temp_blks_written:   float64(s.TempBlksWritten),
		blk_read_time:       s.SharedBlkReadTime,
		blk_write_time:      s.SharedBlkWriteTime,
	}
}

// deltaMetrics are the same 19 metrics as topMetrics, in the same order, read
// from a Delta. The counters rank by their delta. Min, max, mean, and stddev
// aren't counters, so they rank by their window value: WindowMinMax for min
// and max, WindowStats for mean and stddev. On a New delta all four are the
// entry's current values, the same ones topN ranks.
//
// A value that doesn't describe this window ranks as 0, so it can't win a
// slot: a stddev that WindowStats flags as unreliable (rounding noise), and a
// min or max that WindowMinMax reports as lifetime (14 through 16, or 17+
// without a min/max reset). Otherwise, once full resets stop, one old outlier
// would hold a max slot forever.
var deltaMetrics = []func(*pgss.Delta) float64{
	func(d *pgss.Delta) float64 { return float64(d.Calls) },
	func(d *pgss.Delta) float64 { return d.TotalTime },
	func(d *pgss.Delta) float64 {
		if mn, _, lifetime := pgss.WindowMinMax(*d); !lifetime {
			return mn
		}
		return 0
	},
	func(d *pgss.Delta) float64 {
		if _, mx, lifetime := pgss.WindowMinMax(*d); !lifetime {
			return mx
		}
		return 0
	},
	func(d *pgss.Delta) float64 { m, _, _ := pgss.WindowStats(*d); return m },
	func(d *pgss.Delta) float64 {
		if _, sd, ok := pgss.WindowStats(*d); ok {
			return sd
		}
		return 0
	},
	func(d *pgss.Delta) float64 { return float64(d.Rows) },
	func(d *pgss.Delta) float64 { return float64(d.SharedBlksHit) },
	func(d *pgss.Delta) float64 { return float64(d.SharedBlksRead) },
	func(d *pgss.Delta) float64 { return float64(d.SharedBlksWritten) },
	func(d *pgss.Delta) float64 { return float64(d.SharedBlksDirtied) },
	func(d *pgss.Delta) float64 { return float64(d.LocalBlksHit) },
	func(d *pgss.Delta) float64 { return float64(d.LocalBlksRead) },
	func(d *pgss.Delta) float64 { return float64(d.LocalBlksWritten) },
	func(d *pgss.Delta) float64 { return float64(d.LocalBlksDirtied) },
	func(d *pgss.Delta) float64 { return float64(d.TempBlksRead) },
	func(d *pgss.Delta) float64 { return float64(d.TempBlksWritten) },
	func(d *pgss.Delta) float64 { return d.SharedBlkWriteTime },
	func(d *pgss.Delta) float64 { return d.SharedBlkReadTime },
}

// topNDeltas returns the union of the top n deltas for each metric in
// deltaMetrics, each delta once, in input order, with New and Prev intact.
// Ties break by key (QueryID, then UserID, DBID, and TopLevel), so the
// selection doesn't depend on input order.
//
// TODO(20261001-103222-25): switch the worker to this and delete topN.
func topNDeltas(deltas []pgss.Delta, n int) []pgss.Delta {
	keep := make([]bool, len(deltas))
	idx := make([]int, len(deltas))
	vals := make([]float64, len(deltas))
	for _, m := range deltaMetrics {
		for i := range idx {
			idx[i] = i
			vals[i] = m(&deltas[i])
		}
		sort.Slice(idx, func(a, b int) bool {
			x, y := idx[a], idx[b]
			if vals[x] != vals[y] {
				return vals[x] > vals[y]
			}
			return keyLess(pgss.KeyOf(deltas[x].Stat), pgss.KeyOf(deltas[y].Stat))
		})
		for _, i := range idx[:min(n, len(idx))] {
			keep[i] = true
		}
	}
	var out []pgss.Delta
	for i, k := range keep {
		if k {
			out = append(out, deltas[i])
		}
	}
	return out
}

// keyLess orders keys by QueryID, then UserID, DBID, and TopLevel (false first).
func keyLess(a, b pgss.Key) bool {
	switch {
	case a.QueryID != b.QueryID:
		return a.QueryID < b.QueryID
	case a.UserID != b.UserID:
		return a.UserID < b.UserID
	case a.DBID != b.DBID:
		return a.DBID < b.DBID
	default:
		return !a.TopLevel && b.TopLevel
	}
}
