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
