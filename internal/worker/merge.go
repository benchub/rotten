package worker

import (
	"math"

	"github.com/benchub/rotten/internal/harvestlimits"
)

func (w *Worker) stillProcessing() uint32 {
	return w.processing.Load()
}

// mergeEvent folds b into a, for two events with the same fingerprint in the
// same observation window, and returns the result. Counters and times are
// summed, min and max are kept, mean and stddev are combined with
// runningstat, and b's context histogram counts are added into a's. The
// merged stddev is absent (stddev_absent) if either side's is. Lifetime min
// and max values are ignored when the other side has window-only min and max;
// the merged min and max are lifetime only when both sides are lifetime. The
// returned event shares a's context map, which is updated in place. Everything
// else (query, window) comes from a.
func mergeEvent(a, b QueryEvent) QueryEvent {
	aCalls := a.calls
	bCalls := b.calls
	a.calls += b.calls
	a.total_time += b.total_time
	a.min_time, a.max_time, a.minmax_lifetime = mergeMinMax(a, b)

	a.mean_time, a.stddev_time = mergePopulationStats(aCalls, a.mean_time, a.stddev_time, bCalls, b.mean_time, b.stddev_time)
	a.stddev_absent = a.stddev_absent || b.stddev_absent
	if a.stddev_absent {
		a.stddev_time = 0
	}

	a.rows += b.rows
	a.shared_blks_hit += b.shared_blks_hit
	a.shared_blks_read += b.shared_blks_read
	a.shared_blks_written += b.shared_blks_written
	a.shared_blks_dirtied += b.shared_blks_dirtied
	a.local_blks_written += b.local_blks_written
	a.local_blks_dirtied += b.local_blks_dirtied
	a.local_blks_read += b.local_blks_read
	a.local_blks_hit += b.local_blks_hit
	a.temp_blks_read += b.temp_blks_read
	a.temp_blks_written += b.temp_blks_written
	a.blk_read_time += b.blk_read_time
	a.blk_write_time += b.blk_write_time

	for hash, count := range b.context {
		a.context[hash] += count
	}

	return a
}

func mergeMinMax(a, b QueryEvent) (float64, float64, bool) {
	if a.minmax_lifetime && !b.minmax_lifetime {
		return b.min_time, b.max_time, false
	}
	if !a.minmax_lifetime && b.minmax_lifetime {
		return a.min_time, a.max_time, false
	}
	minTime := a.min_time
	if minTime > b.min_time {
		minTime = b.min_time
	}
	maxTime := a.max_time
	if maxTime < b.max_time {
		maxTime = b.max_time
	}
	return minTime, maxTime, a.minmax_lifetime && b.minmax_lifetime
}

func mergePopulationStats(aCalls, aMean, aStddev, bCalls, bMean, bStddev float64) (float64, float64) {
	calls := aCalls + bCalls
	if calls <= 0 {
		return 0, 0
	}

	mean := (aMean*aCalls + bMean*bCalls) / calls
	aM2 := aStddev * aStddev * aCalls
	bM2 := bStddev * bStddev * bCalls
	delta := bMean - aMean
	m2 := aM2 + bM2 + delta*delta*aCalls*bCalls/calls
	return mean, math.Sqrt(m2 / calls)
}

// wholeCount converts pg_stat_statements counters to protobuf counters.
// Those counters are integral in Postgres; fractional test-only values are
// rounded instead of silently truncated.
func wholeCount(v float64) uint64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0
	}
	rounded := math.Round(v)
	if rounded >= float64(harvestlimits.MaxContextCount) {
		return harvestlimits.MaxContextCount
	}
	return uint64(rounded)
}
