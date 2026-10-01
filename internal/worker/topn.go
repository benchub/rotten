package worker

import (
	"sort"

	"github.com/benchub/rotten/internal/pgss"
)

// eventFromDelta maps a window's delta onto the worker's event fields. The
// counters are the window's changes. Mean and stddev come from
// pgss.WindowStats; a stddev it flags as unreliable is recorded as absent
// (stddev_absent true, stddev_time 0). Min and max come from pgss.WindowMinMax,
// flagged when they're lifetime values.
func eventFromDelta(d pgss.Delta) QueryEvent {
	mean, stddev, stddevOK := pgss.WindowStats(d)
	if !stddevOK {
		stddev = 0
	}
	mn, mx, lifetime := pgss.WindowMinMax(d)
	return QueryEvent{
		query:               d.Query,
		calls:               float64(d.Calls),
		total_time:          d.TotalTime,
		min_time:            mn,
		max_time:            mx,
		minmax_lifetime:     lifetime,
		mean_time:           mean,
		stddev_time:         stddev,
		stddev_absent:       !stddevOK,
		rows:                float64(d.Rows),
		shared_blks_hit:     float64(d.SharedBlksHit),
		shared_blks_read:    float64(d.SharedBlksRead),
		shared_blks_dirtied: float64(d.SharedBlksDirtied),
		shared_blks_written: float64(d.SharedBlksWritten),
		local_blks_hit:      float64(d.LocalBlksHit),
		local_blks_read:     float64(d.LocalBlksRead),
		local_blks_dirtied:  float64(d.LocalBlksDirtied),
		local_blks_written:  float64(d.LocalBlksWritten),
		temp_blks_read:      float64(d.TempBlksRead),
		temp_blks_written:   float64(d.TempBlksWritten),
		blk_read_time:       d.SharedBlkReadTime,
		blk_write_time:      d.SharedBlkWriteTime,
	}
}

// deltaMetrics are the 19 metrics the worker ranks entries by, the same ones
// the old SQL UNION used, read from a Delta. The counters rank by their
// delta. Min, max, mean, and stddev aren't counters, so they rank by their
// window value: WindowMinMax for min and max, WindowStats for mean and
// stddev. On a New delta all four are the entry's current values.
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
