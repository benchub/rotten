package pgss_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/testdb"
)

// TestReaderMatrix runs a known workload on each supported version, then
// reads it back as the observer role from schema/observer.sql.
func TestReaderMatrix(t *testing.T) {
	for _, v := range testdb.ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObserved(t, v)
			if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
				t.Fatalf("observer.sql: %v\n%s", err, out)
			}
			su := db.Connect(t)
			for _, s := range []string{
				"alter role rotten_observer password 'rotten_observer'",
				"create table pgss_t (id int primary key, v text)",
				"insert into pgss_t select g, 'x' from generate_series(1, 100) g",
			} {
				if _, err := su.Exec(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			for i := 1; i <= 5; i++ {
				if _, err := su.Exec(ctx, "select /*pgss_marker*/ v from pgss_t where id = $1", i); err != nil {
					t.Fatal(err)
				}
			}

			obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			r := pgss.NewReader(obs)

			stats, err := r.Read(ctx)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			var sel, ins *pgss.Stat
			for i := range stats {
				s := &stats[i]
				switch {
				case strings.Contains(s.Query, "pgss_marker"):
					sel = s
				case strings.HasPrefix(s.Query, "insert into pgss_t"):
					ins = s
				}
			}
			if sel == nil || ins == nil {
				t.Fatalf("workload rows missing (select %v, insert %v) in %d stats", sel != nil, ins != nil, len(stats))
			}

			if sel.Calls != 5 || sel.Rows != 5 {
				t.Errorf("calls=%d rows=%d, want 5 and 5", sel.Calls, sel.Rows)
			}
			if sel.QueryID == 0 || sel.UserID == 0 || sel.DBID == 0 || !sel.TopLevel {
				t.Errorf("key fields not set: %+v", sel)
			}
			if sel.Plans < 1 || sel.TotalPlanTime <= 0 || sel.TotalExecTime <= 0 {
				t.Errorf("plans=%d plan=%v exec=%v, want planning and exec time", sel.Plans, sel.TotalPlanTime, sel.TotalExecTime)
			}
			if sel.TotalTime != sel.TotalPlanTime+sel.TotalExecTime {
				t.Errorf("TotalTime %v != plan %v + exec %v", sel.TotalTime, sel.TotalPlanTime, sel.TotalExecTime)
			}
			if sel.MinTime <= 0 || sel.MinTime > sel.MaxTime || sel.MeanTime < sel.MinTime || sel.MeanTime > sel.MaxTime {
				t.Errorf("min/mean/max out of order: %v %v %v", sel.MinTime, sel.MeanTime, sel.MaxTime)
			}
			// min, max, mean, and stddev are exec only, so mean*calls is the
			// exec total, not plan + exec.
			if d := sel.MeanTime*float64(sel.Calls) - sel.TotalExecTime; d > 1e-6 || d < -1e-6 {
				t.Errorf("mean*calls = %v, want exec total %v", sel.MeanTime*float64(sel.Calls), sel.TotalExecTime)
			}
			if sel.StddevTime < 0 {
				t.Errorf("stddev %v", sel.StddevTime)
			}
			if sel.SharedBlksHit+sel.SharedBlksRead == 0 {
				t.Errorf("no shared block access recorded")
			}
			if ins.WALRecords == 0 || ins.WALBytes == 0 {
				t.Errorf("insert WAL records=%d bytes=%v, want > 0", ins.WALRecords, ins.WALBytes)
			}

			pg15 := v >= 15
			if (sel.TempBlkReadTime != nil) != pg15 || (sel.TempBlkWriteTime != nil) != pg15 {
				t.Errorf("temp_blk_*_time set=%v/%v, want %v", sel.TempBlkReadTime != nil, sel.TempBlkWriteTime != nil, pg15)
			}
			pg17 := v >= 17
			if (sel.StatsSince != nil) != pg17 || (sel.MinmaxStatsSince != nil) != pg17 {
				t.Errorf("stats_since set=%v minmax_stats_since set=%v, want %v", sel.StatsSince != nil, sel.MinmaxStatsSince != nil, pg17)
			}
			if (sel.LocalBlkReadTime != nil) != pg17 || (sel.LocalBlkWriteTime != nil) != pg17 {
				t.Errorf("local_blk_*_time set=%v/%v, want %v", sel.LocalBlkReadTime != nil, sel.LocalBlkWriteTime != nil, pg17)
			}
			pg18 := v >= 18
			if (ins.WALBuffersFull != nil) != pg18 || (ins.ParallelWorkersToLaunch != nil) != pg18 || (ins.ParallelWorkersLaunched != nil) != pg18 {
				t.Errorf("wal_buffers_full set=%v parallel_workers_to_launch set=%v parallel_workers_launched set=%v, want %v",
					ins.WALBuffersFull != nil, ins.ParallelWorkersToLaunch != nil, ins.ParallelWorkersLaunched != nil, pg18)
			}

			info, err := r.Info(ctx)
			if err != nil {
				t.Fatalf("Info: %v", err)
			}
			if info.StatsReset.IsZero() || info.Dealloc != 0 {
				t.Errorf("info = %+v, want stats_reset set and dealloc 0", info)
			}
		})
	}
}
