// Package pgss reads pg_stat_statements directly, as the observer role that
// schema/observer.sql sets up (pg_read_all_stats), on Postgres 14 through 18.
package pgss

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Stat is one pg_stat_statements row, the same shape on every version.
// Pointer fields are nil when the installed extension doesn't have them.
type Stat struct {
	UserID   uint32
	DBID     uint32
	TopLevel bool
	QueryID  int64
	// Query is empty from Reader.ReadStats. TextCache.Fill sets it.
	Query string

	Plans         int64
	TotalPlanTime float64
	Calls         int64
	TotalExecTime float64

	// TotalTime is plan + exec. MinTime, MaxTime, MeanTime, and StddevTime
	// come from exec only, since plan and exec stats can't be combined.
	TotalTime  float64
	MinTime    float64
	MaxTime    float64
	MeanTime   float64
	StddevTime float64

	Rows              int64
	SharedBlksHit     int64
	SharedBlksRead    int64
	SharedBlksDirtied int64
	SharedBlksWritten int64
	LocalBlksHit      int64
	LocalBlksRead     int64
	LocalBlksDirtied  int64
	LocalBlksWritten  int64
	TempBlksRead      int64
	TempBlksWritten   int64

	// SharedBlkReadTime and SharedBlkWriteTime are blk_read_time and
	// blk_write_time before 17.
	SharedBlkReadTime  float64
	SharedBlkWriteTime float64

	WALRecords int64
	WALFPI     int64
	WALBytes   float64

	// 15+.
	TempBlkReadTime  *float64
	TempBlkWriteTime *float64
	// 17+.
	LocalBlkReadTime  *float64
	LocalBlkWriteTime *float64
	StatsSince        *time.Time
	MinmaxStatsSince  *time.Time
	// 18+.
	WALBuffersFull          *int64
	ParallelWorkersToLaunch *int64
	ParallelWorkersLaunched *int64
}

// Info is pg_stat_statements_info.
type Info struct {
	Dealloc    int64
	StatsReset time.Time
}

// Reader reads pg_stat_statements on one connection. It looks up the
// extension version once, on first use.
type Reader struct {
	conn  *pgx.Conn
	ver   [2]int
	query string // the ReadStats query, set by ExtVersion
}

func NewReader(conn *pgx.Conn) *Reader { return &Reader{conn: conn} }

// ExtVersion returns the installed extension version as (major, minor).
func (r *Reader) ExtVersion(ctx context.Context) ([2]int, error) {
	if r.query != "" {
		return r.ver, nil
	}
	var s string
	if err := r.conn.QueryRow(ctx,
		`select extversion from pg_extension where extname = 'pg_stat_statements'`).Scan(&s); err != nil {
		return r.ver, fmt.Errorf("pgss: read extversion: %w", err)
	}
	v, err := checkSupportedVersion(s)
	if err != nil {
		return r.ver, err
	}
	r.ver = v
	r.query = selectSQL(v[1])
	return r.ver, nil
}

// checkSupportedVersion parses extversion s and checks that the reader
// supports it. Majors other than 1 are rejected because selectSQL and col key
// only on the 1.x minor and a 2.x's columns are unknown; accepting a new major
// means extending selectSQL and col first.
func checkSupportedVersion(s string) ([2]int, error) {
	v, err := parseExtVersion(s)
	if err != nil {
		return [2]int{}, err
	}
	if v[0] != 1 {
		return [2]int{}, fmt.Errorf("pgss: pg_stat_statements %s has unsupported major version %d; rotten supports 1.x", s, v[0])
	}
	if v[1] < 9 {
		return [2]int{}, fmt.Errorf("pgss: extversion %s is older than 1.9 (Postgres 14)", s)
	}
	return v, nil
}

// HasMinmaxReset reports whether the extension supports the min/max only
// reset (1.11, Postgres 17+).
func (r *Reader) HasMinmaxReset(ctx context.Context) (bool, error) {
	v, err := r.ExtVersion(ctx)
	return hasMinmaxReset(v), err
}

// parseExtVersion parses an extversion like "1.11" into (major, minor). Like
// observer.sql's string_to_array(extversion, '.')::int[] cast, it accepts any
// number of dot-separated parts and errors on non-numeric ones. A missing
// minor part counts as 0, and parts after the minor are ignored.
func parseExtVersion(s string) ([2]int, error) {
	var v [2]int
	for i, p := range strings.Split(s, ".") {
		n, err := strconv.Atoi(p)
		if err != nil || p == "" || p[0] < '0' || p[0] > '9' {
			return [2]int{}, fmt.Errorf("pgss: unexpected extversion %q", s)
		}
		if i < len(v) {
			v[i] = n
		}
	}
	return v, nil
}

// hasMinmaxReset reports whether version v is at least 1.11. The major > 1
// branch matches observer.sql's int-array comparison; it becomes reachable
// once checkSupportedVersion accepts a new major.
func hasMinmaxReset(v [2]int) bool { return v[0] > 1 || (v[0] == 1 && v[1] >= 11) }

// col returns expr when the extension minor version is at least since, and a
// typed NULL otherwise.
func col(minor, since int, expr, typ string) string {
	if minor >= since {
		return expr
	}
	return "null::" + typ
}

// selectSQL builds the query for extension version 1.<minor>. It passes
// showtext := false, so the query column is NULL and Query comes back empty.
func selectSQL(minor int) string {
	sharedRead, sharedWrite := "blk_read_time", "blk_write_time"
	if minor >= 11 {
		sharedRead, sharedWrite = "shared_blk_read_time", "shared_blk_write_time"
	}
	cols := []string{
		"userid", "dbid", "toplevel", "coalesce(queryid, 0)", "coalesce(query, '')",
		"plans", "total_plan_time", "calls", "total_exec_time",
		"min_exec_time", "max_exec_time", "mean_exec_time", "stddev_exec_time",
		"rows",
		"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
		"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
		"temp_blks_read", "temp_blks_written",
		sharedRead, sharedWrite,
		"wal_records", "wal_fpi", "wal_bytes::float8",
		col(minor, 10, "temp_blk_read_time", "float8"),
		col(minor, 10, "temp_blk_write_time", "float8"),
		col(minor, 11, "local_blk_read_time", "float8"),
		col(minor, 11, "local_blk_write_time", "float8"),
		col(minor, 11, "stats_since", "timestamptz"),
		col(minor, 11, "minmax_stats_since", "timestamptz"),
		col(minor, 12, "wal_buffers_full", "int8"),
		col(minor, 12, "parallel_workers_to_launch", "int8"),
		col(minor, 12, "parallel_workers_launched", "int8"),
	}
	return "select " + strings.Join(cols, ", ") + " from pg_stat_statements(showtext := false)"
}

// ReadStats returns every row of pg_stat_statements without query text
// (Query is empty). It calls pg_stat_statements(showtext := false), so the
// server doesn't read the query text file. Use a TextCache to attach text to
// the rows you keep.
func (r *Reader) ReadStats(ctx context.Context) ([]Stat, error) {
	if _, err := r.ExtVersion(ctx); err != nil {
		return nil, err
	}
	rows, err := r.conn.Query(ctx, r.query)
	if err != nil {
		return nil, fmt.Errorf("pgss: read: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Stat, error) {
		var s Stat
		err := row.Scan(&s.UserID, &s.DBID, &s.TopLevel, &s.QueryID, &s.Query,
			&s.Plans, &s.TotalPlanTime, &s.Calls, &s.TotalExecTime,
			&s.MinTime, &s.MaxTime, &s.MeanTime, &s.StddevTime,
			&s.Rows,
			&s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied, &s.SharedBlksWritten,
			&s.LocalBlksHit, &s.LocalBlksRead, &s.LocalBlksDirtied, &s.LocalBlksWritten,
			&s.TempBlksRead, &s.TempBlksWritten,
			&s.SharedBlkReadTime, &s.SharedBlkWriteTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes,
			&s.TempBlkReadTime, &s.TempBlkWriteTime,
			&s.LocalBlkReadTime, &s.LocalBlkWriteTime,
			&s.StatsSince, &s.MinmaxStatsSince,
			&s.WALBuffersFull, &s.ParallelWorkersToLaunch, &s.ParallelWorkersLaunched)
		s.TotalTime = s.TotalPlanTime + s.TotalExecTime
		return s, err
	})
}

// Info reads pg_stat_statements_info.
func (r *Reader) Info(ctx context.Context) (Info, error) {
	var i Info
	err := r.conn.QueryRow(ctx, `select dealloc, stats_reset from pg_stat_statements_info`).Scan(&i.Dealloc, &i.StatsReset)
	if err != nil {
		return i, fmt.Errorf("pgss: read info: %w", err)
	}
	return i, nil
}
