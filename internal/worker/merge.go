package worker

import runningstat "github.com/benchub/runningstat"

func (w *Worker) fingerprintCount() int {
	return 0
}

func (w *Worker) stillProcessing() uint32 {
	return w.processing.Load()
}

// mergeEvent folds b into a, for two events with the same fingerprint in the
// same observation window, and returns the result. Counters and times are
// summed, min and max are kept, mean and stddev are combined with
// runningstat, and b's context histogram counts are added into a's. The
// merged stddev is absent (stddev_absent) if either side's is, and min and
// max are lifetime if either side's are. The returned event shares a's
// context map, which is updated in place. Everything else (query, window)
// comes from a.
func mergeEvent(a, b QueryEvent) QueryEvent {
	a.calls += b.calls
	a.total_time += b.total_time
	if a.min_time > b.min_time {
		a.min_time = b.min_time
	}
	if a.max_time < b.max_time {
		a.max_time = b.max_time
	}
	a.minmax_lifetime = a.minmax_lifetime || b.minmax_lifetime

	rs1 := runningstat.RunningStat{}
	rs2 := runningstat.RunningStat{}

	// Note: a.calls already includes b.calls here. That's how it's always
	// worked, so this characterization keeps it.
	rs1.Init(int64(a.calls), a.mean_time, a.stddev_time)
	rs2.Init(int64(b.calls), b.mean_time, b.stddev_time)
	rs1.Merge(rs2)

	a.mean_time = rs1.RunningStatMean()
	a.stddev_time = rs1.RunningStatDeviation()
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
