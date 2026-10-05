//go:build perf

// The report performance suite: `make test-perf`. It isn't part of `make test`
// or `make test-all`, because seeding takes minutes. It seeds about 10 million
// events (ROTTEN_PERF_EVENTS overrides the count) across 21 daily partitions,
// then runs every report the way the UI does: a prepared statement with bound
// parameters, in a read-only transaction with a 15s statement_timeout, once
// with plan_cache_mode = force_custom_plan and once with force_generic_plan,
// because Rails reuses prepared statements and Postgres switches to a generic
// plan after five executions. For each run it asserts that only the
// partitions overlapping the range are scanned (from EXPLAIN (ANALYZE,
// BUFFERS, FORMAT JSON)) and that the median latency is under the case's
// budget. docs/perf.md records the numbers.
package reports_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	perfDefaultEvents = 10_000_000
	perfDays          = 21
	perfWindow        = 5 * time.Minute
	perfUITimeout     = "15s"
	perfIndex         = "events_fingerprint_window"
	perfProject       = "canvas"
	perfCluster       = "13"
	perfEnvironment   = "production"
	// The canvas fingerprint pool. Index 1 is the hottest fingerprint, so it
	// has the most events: the worst case for a per-fingerprint report.
	perfCanvasPool = 15000
	perfBridgePool = 5000
	// A mid-popularity fingerprint: it shows up in about a third of cluster
	// 13's windows, where the hot one shows up in all of them.
	perfTypicalFP = 200
)

// perfAnchor is the end of the seeded data and of every report range. It's
// fixed so runs are comparable: noon UTC, so the 3h ranges sit in one
// partition and the 24h ranges in two, every time.
var perfAnchor = time.Date(2026, time.January, 21, 12, 0, 0, 0, time.UTC)

type perfStream struct {
	logicalID, physicalID int
	project, cluster      string
	role, fqdn            string
	fpBase, pool          int
	weight                float64
	samples               int
}

// perfStreams: canvas on clusters 13 (the busy one the reports look at), 7
// and 21, and bridge on cluster 1. Each has a primary with one host and a
// replica with two hosts. Every host harvests its own events.
func perfStreams() []perfStream {
	var out []perfStream
	logical, physical := 0, 0
	for _, c := range []struct {
		project, cluster string
		fpBase, pool     int
		weight           float64
	}{
		{"canvas", "13", 0, perfCanvasPool, 8},
		{"canvas", "7", 0, perfCanvasPool, 2},
		{"canvas", "21", 0, perfCanvasPool, 2},
		{"bridge", "1", perfCanvasPool, perfBridgePool, 2},
	} {
		for _, r := range []struct {
			role  string
			hosts int
		}{{testdb.ReportPrimaryRole, 1}, {testdb.ReportReplicaRole, 2}} {
			logical++
			for h := 1; h <= r.hosts; h++ {
				physical++
				out = append(out, perfStream{
					logicalID: logical, physicalID: physical,
					project: c.project, cluster: c.cluster, role: r.role,
					fqdn:   fmt.Sprintf("%s-%s-%s-%d.perf.example", c.project, c.cluster, r.role, h),
					fpBase: c.fpBase, pool: c.pool, weight: c.weight,
				})
			}
		}
	}
	return out
}

// samplesForDistinct returns how many draws of idx = floor(pool*u^2)+1 give
// about want distinct values. P(idx <= n) = sqrt(n/pool), so low indexes are
// hot: they show up in almost every window, and the tail rarely does.
func samplesForDistinct(pool int, want float64) int {
	expected := func(s int) float64 {
		var sum float64
		for i := 1; i <= pool; i++ {
			p := math.Sqrt(float64(i)/float64(pool)) - math.Sqrt(float64(i-1)/float64(pool))
			sum += 1 - math.Pow(1-p, float64(s))
		}
		return sum
	}
	lo, hi := 1, pool*50
	for lo < hi {
		mid := (lo + hi) / 2
		if expected(mid) < want {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

type perfFixture struct {
	anchor    time.Time
	hotFP     int64
	typicalFP int64
	events    int64
	contexts  int64
	seedTime  time.Duration
	pgVersion string
}

func perfEventTarget(t *testing.T) int {
	t.Helper()
	s := os.Getenv("ROTTEN_PERF_EVENTS")
	if s == "" {
		return perfDefaultEvents
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		t.Fatalf("ROTTEN_PERF_EVENTS=%q: want a positive integer", s)
	}
	return n
}

const perfSeedDaySQL = `
insert into rotten.events (fingerprint_id, logical_source_id, physical_source_id,
    observed_window_start, observed_window_end, recorded_at, calls, time)
select
  st.fp_base + p.idx,
  st.logical_id,
  st.physical_id,
  w.ws,
  w.ws + interval '5 minutes',
  w.ws + interval '5 minutes',
  p.calls,
  p.calls * (0.05 + ((p.idx * 7919) % 1000) / 100.0) * (0.8 + 0.4 * random())
    -- A few fingerprints get much slower in the last two hours, so the
    -- outliers report has something to find.
    * case when p.idx % 250 = 3 and w.ws >= $3::timestamptz - interval '2 hours' then 10 else 1 end
from generate_series($1::timestamptz, $2::timestamptz - interval '5 minutes', interval '5 minutes') w(ws)
cross join public.perf_streams st
cross join lateral (
  select d.idx, greatest(1, floor(50000.0 / d.idx * (0.5 + random())))::float as calls
  from (
    select distinct floor(st.pool * power(random(), 2))::int + 1 as idx
    from generate_series(1, st.samples)
    where w.ws is not null
  ) d
) p`

const perfSeedContextSQL = `
insert into rotten.event_context (event_id, observed_window_start, observed_window_end,
    controller_id, action_id, job_tag_id, c, logical_source_id, attributed_time)
select
  e.id,
  e.observed_window_start,
  e.observed_window_end,
  case when j.job then null else 1 + (e.fingerprint_id * 7 + k.n * 13) % 200 end,
  case when j.job then null else 1 + (e.fingerprint_id * 3 + k.n) % 50 end,
  case when j.job then 1 + (e.fingerprint_id + k.n) % 100 end,
  greatest(1, round(e.calls / k.total))::bigint,
  e.logical_source_id,
  -- Every context of an event gets the same c, so each carries an equal share.
  e.time / k.total
from rotten.events e
-- A hash of the event's natural key, not e.id: ids depend on which seeding
-- connection got to the sequence first, so they aren't deterministic.
cross join lateral (select (e.fingerprint_id * 31 + e.physical_source_id * 17
                            + extract(epoch from e.observed_window_start)::bigint / 300) % 10 as v) h
cross join lateral (select n, case when h.v < 3 then 2 else 1 end as total
                    from generate_series(1, case when h.v < 3 then 2 else 1 end) n) k
cross join lateral (select (e.fingerprint_id + k.n) % 5 = 0 as job) j
where e.observed_window_start >= $1::timestamptz
  and e.observed_window_start < $2::timestamptz`

func seedPerf(t *testing.T, db *testdb.DB) perfFixture {
	t.Helper()
	ctx := context.Background()
	started := time.Now()
	conn := db.Connect(t)
	target := perfEventTarget(t)

	anchor := perfAnchor
	firstDay := anchor.Truncate(24*time.Hour).AddDate(0, 0, -(perfDays - 1))
	windows := float64(anchor.Sub(firstDay) / perfWindow)
	perWindow := float64(target) / windows

	streams := perfStreams()
	var totalWeight float64
	for _, s := range streams {
		totalWeight += s.weight
	}
	for i := range streams {
		streams[i].samples = samplesForDistinct(streams[i].pool, perWindow*streams[i].weight/totalWeight)
	}

	var days []time.Time
	for d := firstDay; d.Before(anchor); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	var setup []string
	setup = append(setup,
		fmt.Sprintf(`select public.create_partition_time('rotten.events', array[%s]::timestamptz[])`, perfTimeList(days)),
		fmt.Sprintf(`select public.create_partition_time('rotten.event_context', array[%s]::timestamptz[])`, perfTimeList(days)),
		`insert into rotten.fingerprints (id, fingerprint, normalized)
		 select i, 'select perf_' || i || ' from t where id = ' || i,
		        'SELECT perf_' || i || ', ' || repeat('col, ', 30) || 'x FROM t WHERE id = $1 AND kind = $2'
		 from generate_series(1, `+strconv.Itoa(perfCanvasPool+perfBridgePool)+`) i`,
		`select setval('rotten.fingerprints_id_seq', `+strconv.Itoa(perfCanvasPool+perfBridgePool)+`)`,
		`insert into rotten.controllers (id, controller) select i, 'Controller' || i from generate_series(1, 200) i`,
		`insert into rotten.actions (id, action) select i, 'action_' || i from generate_series(1, 50) i`,
		`insert into rotten.job_tags (id, job_tag) select i, 'Job' || i || '#perform' from generate_series(1, 100) i`,
		`create table public.perf_streams (logical_id int, physical_id int, fp_base int, pool int, samples int)`,
	)
	for _, q := range setup {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("perf setup %s: %v", q, err)
		}
	}
	seenLogical := map[int]bool{}
	for _, s := range streams {
		if !seenLogical[s.logicalID] {
			seenLogical[s.logicalID] = true
			if _, err := conn.Exec(ctx, `insert into rotten.logical_sources (id, project, environment, cluster, role) values ($1, $2, $3, $4, $5)`,
				s.logicalID, s.project, perfEnvironment, s.cluster, s.role); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := conn.Exec(ctx, `insert into rotten.physical_sources (id, fqdn) values ($1, $2)`, s.physicalID, s.fqdn); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `insert into public.perf_streams values ($1, $2, $3, $4, $5)`,
			s.logicalID, s.physicalID, s.fpBase, s.pool, s.samples); err != nil {
			t.Fatal(err)
		}
	}

	// One connection per worker, a day at a time. Seeding is the slow part,
	// and days are independent. Each day seeds random() from its index first,
	// on the same connection, so the data doesn't depend on scheduling.
	type perfDay struct {
		i   int
		day time.Time
	}
	work := make(chan perfDay)
	errs := make(chan error, len(days))
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(ctx, db.DSN)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close(ctx)
			// Every foreign key holds by construction; skip the checks.
			if _, err := c.Exec(ctx, "set session_replication_role = replica"); err != nil {
				errs <- err
				return
			}
			if _, err := c.Exec(ctx, "set enable_memoize = off"); err != nil {
				errs <- err
				return
			}
			for d := range work {
				day := d.day
				end := day.AddDate(0, 0, 1)
				if end.After(anchor) {
					end = anchor
				}
				if _, err := c.Exec(ctx, "select setseed($1)", float64(d.i+1)/float64(len(days)+1)); err != nil {
					errs <- fmt.Errorf("setseed %s: %w", day, err)
					continue
				}
				if _, err := c.Exec(ctx, perfSeedDaySQL, day, end, anchor); err != nil {
					errs <- fmt.Errorf("seed events %s: %w", day, err)
					continue
				}
				if _, err := c.Exec(ctx, perfSeedContextSQL, day, end); err != nil {
					errs <- fmt.Errorf("seed contexts %s: %w", day, err)
				}
			}
		}()
	}
	for i, d := range days {
		work <- perfDay{i, d}
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for _, q := range []string{
		`insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count, mean, deviation, last)
		 select fingerprint_id, logical_source_id, 'mean_time'::rotten.fingerprint_stats_domain, count(*) + 50, avg(time / calls),
		        coalesce(stddev_samp(time / calls), 0), 0
		 from rotten.events group by fingerprint_id, logical_source_id
		 union all
		 select fingerprint_id, 0, 'mean_time'::rotten.fingerprint_stats_domain, count(*) + 50, avg(time / calls),
		        coalesce(stddev_samp(time / calls), 0), 0
		 from rotten.events group by fingerprint_id`,
		`vacuum (analyze) rotten.events, rotten.event_context, rotten.fingerprints, rotten.fingerprint_stats,
		 rotten.logical_sources, rotten.physical_sources, rotten.controllers, rotten.actions, rotten.job_tags`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("perf seed %s: %v", q, err)
		}
	}

	f := perfFixture{anchor: anchor, hotFP: 1, typicalFP: perfTypicalFP, seedTime: time.Since(started)}
	if err := conn.QueryRow(ctx, "select count(*) from rotten.events").Scan(&f.events); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "select count(*) from rotten.event_context").Scan(&f.contexts); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "show server_version").Scan(&f.pgVersion); err != nil {
		t.Fatal(err)
	}
	var populated int
	if err := conn.QueryRow(ctx, `select count(distinct tableoid) from rotten.events`).Scan(&populated); err != nil {
		t.Fatal(err)
	}
	if populated != perfDays {
		t.Fatalf("events are in %d partitions, want %d", populated, perfDays)
	}
	if f.events < int64(target)*9/10 || f.events > int64(target)*11/10 {
		t.Fatalf("seeded %d events, want about %d", f.events, target)
	}
	for _, fp := range []int64{f.hotFP, f.typicalFP} {
		var n int64
		if err := conn.QueryRow(ctx, `select count(*) from rotten.events e join rotten.logical_sources s on s.id = e.logical_source_id
			where e.fingerprint_id = $1 and s.project = $2 and s.cluster = $3`, fp, perfProject, perfCluster).Scan(&n); err != nil {
			t.Fatal(err)
		}
		t.Logf("fingerprint %d has %d events on %s cluster %s", fp, n, perfProject, perfCluster)
	}
	t.Logf("seeded %d events and %d event_context rows in %d partitions in %s (Postgres %s)",
		f.events, f.contexts, populated, f.seedTime.Round(time.Second), f.pgVersion)
	return f
}

func perfTimeList(times []time.Time) string {
	var parts []string
	for _, t := range times {
		parts = append(parts, "'"+t.Format(time.RFC3339)+"'")
	}
	return strings.Join(parts, ",")
}

// perfLiteral formats a report argument for EXECUTE. The values come from the
// test, never from input.
func perfLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return "'" + x.UTC().Format(time.RFC3339Nano) + "'"
	default:
		panic(fmt.Sprintf("perfLiteral: unsupported %T", v))
	}
}

type perfCase struct {
	name   string
	file   string
	args   []any
	start  time.Time
	end    time.Time
	budget time.Duration
	runs   int
}

type planNode struct {
	NodeType        string     `json:"Node Type"`
	RelationName    string     `json:"Relation Name"`
	IndexName       string     `json:"Index Name"`
	ActualLoops     float64    `json:"Actual Loops"`
	SubplansRemoved int        `json:"Subplans Removed"`
	SharedHit       float64    `json:"Shared Hit Blocks"`
	SharedRead      float64    `json:"Shared Read Blocks"`
	Plans           []planNode `json:"Plans"`
}

type planSummary struct {
	scanned         map[string]map[string]bool // parent -> partitions with loops > 0
	inPlan          map[string]map[string]bool // parent -> partitions anywhere in the plan
	subplansRemoved int
	accessPaths     map[string]bool // "Seq Scan", "Index Scan using events_..._idx" and so on, on the partitions
	executionMS     float64
	sharedHit       float64
	sharedRead      float64
}

func summarizePlan(t *testing.T, raw []byte, partitionsOf map[string]string) planSummary {
	t.Helper()
	var doc []struct {
		Plan          planNode `json:"Plan"`
		ExecutionTime float64  `json:"Execution Time"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse plan: %v\n%s", err, raw)
	}
	s := planSummary{
		scanned:     map[string]map[string]bool{},
		inPlan:      map[string]map[string]bool{},
		accessPaths: map[string]bool{},
		executionMS: doc[0].ExecutionTime,
	}
	s.sharedHit, s.sharedRead = doc[0].Plan.SharedHit, doc[0].Plan.SharedRead
	var walk func(n planNode)
	walk = func(n planNode) {
		s.subplansRemoved += n.SubplansRemoved
		if parent, ok := partitionsOf[n.RelationName]; ok {
			if s.inPlan[parent] == nil {
				s.inPlan[parent], s.scanned[parent] = map[string]bool{}, map[string]bool{}
			}
			s.inPlan[parent][n.RelationName] = true
			if n.ActualLoops > 0 {
				s.scanned[parent][n.RelationName] = true
				path := n.NodeType
				if n.IndexName != "" {
					path += " using " + trimPartitionIndex(n.IndexName)
				}
				if n.NodeType == "Bitmap Heap Scan" {
					var indexes []string
					var bitmapIndexes func(b planNode)
					bitmapIndexes = func(b planNode) {
						if b.IndexName != "" {
							indexes = append(indexes, trimPartitionIndex(b.IndexName))
						}
						for _, c := range b.Plans {
							bitmapIndexes(c)
						}
					}
					bitmapIndexes(n)
					path += " on " + strings.Join(indexes, " + ")
				}
				s.accessPaths[parent+": "+path] = true
			}
		}
		for _, c := range n.Plans {
			walk(c)
		}
	}
	walk(doc[0].Plan)
	return s
}

// trimPartitionIndex turns events_p20261001_fingerprint_id_observed_window_start_idx
// into events_p*_fingerprint_id_observed_window_start_idx, so plan shapes
// from different partitions read the same.
func trimPartitionIndex(name string) string {
	for _, p := range []string{"event_context_p", "events_p"} {
		if strings.HasPrefix(name, p) {
			rest := strings.TrimPrefix(name, p)
			if i := strings.Index(rest, "_"); i > 0 {
				return p + "*" + rest[i:]
			}
		}
	}
	return name
}

func perfPartitionsOf(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, parent := range []string{"rotten.events", "rotten.event_context"} {
		for _, p := range childPartitions(t, conn, parent) {
			out[p] = parent
		}
	}
	return out
}

type perfResult struct {
	median   time.Duration
	runs     []time.Duration
	summary  planSummary
	rows     int
	timedOut bool
}

// runPerfCase prepares the report on the UI role's connection, warms it up,
// times runs executions (each in its own read-only transaction with the UI's
// statement_timeout), then explains one more.
// perfConn is a *pgx.Conn, or a pgx.Tx whose Begin makes a savepoint.
type perfConn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func runPerfCase(t *testing.T, conn perfConn, c perfCase, mode string, partitionsOf map[string]string) perfResult {
	t.Helper()
	ctx := context.Background()
	query, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "set plan_cache_mode = "+mode); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "prepare perf_report as "+string(query)); err != nil {
		t.Fatalf("prepare %s: %v", c.file, err)
	}
	defer func() {
		if _, err := conn.Exec(ctx, "deallocate perf_report"); err != nil {
			t.Fatal(err)
		}
	}()
	var lits []string
	for _, a := range c.args {
		lits = append(lits, perfLiteral(a))
	}
	execute := "execute perf_report(" + strings.Join(lits, ", ") + ")"

	once := func(sql string) (time.Duration, int, []byte, bool) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "set transaction read only"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "select set_config('statement_timeout', $1, true)", perfUITimeout); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		canceled := func(err error) bool {
			var pgErr *pgconn.PgError
			return errors.As(err, &pgErr) && pgErr.Code == "57014"
		}
		rows, err := tx.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		if canceled(err) {
			return 0, 0, nil, true
		}
		if err != nil {
			t.Fatalf("%s (%s): %v", c.name, mode, err)
		}
		n := 0
		var plan []byte
		for rows.Next() {
			n++
			if plan == nil {
				vals := rows.RawValues()
				plan = slices.Clone(vals[0])
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			if canceled(err) {
				return 0, 0, nil, true
			}
			t.Fatalf("%s (%s): %v", c.name, mode, err)
		}
		elapsed := time.Since(began)
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return elapsed, n, plan, false
	}

	// A run that hits the UI's statement_timeout is what a user would see
	// as an error page. Record it and move on, so one slow report doesn't
	// hide the numbers for the rest.
	timedOut := func() perfResult {
		t.Errorf("%s (%s): canceled by the UI's %s statement_timeout", c.name, mode, perfUITimeout)
		return perfResult{timedOut: true}
	}
	_, rowCount, _, canceled := once(execute)
	if canceled {
		return timedOut()
	}
	runs := c.runs
	if runs == 0 {
		runs = 5
	}
	var r perfResult
	r.rows = rowCount
	for i := 0; i < runs; i++ {
		d, _, _, canceled := once(execute)
		if canceled {
			return timedOut()
		}
		r.runs = append(r.runs, d)
	}
	sorted := slices.Clone(r.runs)
	slices.Sort(sorted)
	r.median = sorted[len(sorted)/2]
	_, _, plan, canceled := once("explain (analyze, buffers, format json) " + execute)
	if canceled {
		return timedOut()
	}
	r.summary = summarizePlan(t, plan, partitionsOf)
	return r
}

func assertPerfPruning(t *testing.T, conn *pgx.Conn, c perfCase, r perfResult) {
	t.Helper()
	// A partition can stay in a generic plan when pruning happens per
	// execution of its node; it only costs anything if it's actually scanned.
	for parent, scanned := range r.summary.scanned {
		expected := partitionsOverlappingRange(t, conn, parent, c.start, c.end)
		for p := range scanned {
			if !expected[p] {
				t.Errorf("%s: scans out-of-range partition %s; want only %v", c.name, p, sortedKeys(expected))
			}
		}
	}
	if len(r.summary.scanned["rotten.events"]) == 0 && len(r.summary.scanned["rotten.event_context"]) == 0 {
		t.Errorf("%s: scans no events or event_context partition", c.name)
	}
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func perfCases(f perfFixture) []perfCase {
	end := f.anchor
	h3, h24, d7, d21 := end.Add(-3*time.Hour), end.Add(-24*time.Hour), end.AddDate(0, 0, -7), end.Truncate(24*time.Hour).AddDate(0, 0, -(perfDays-1))
	src := []any{perfProject, perfEnvironment, perfCluster}
	with := func(xs ...any) []any { return append(slices.Clone(src), xs...) }
	const twoS, uiTimeout = 2 * time.Second, 15 * time.Second
	var cases []perfCase
	add := func(name, file string, start time.Time, budget time.Duration, runs int, args ...any) {
		cases = append(cases, perfCase{name: name, file: file, args: args, start: start, end: end, budget: budget, runs: runs})
	}
	for _, r := range []struct {
		label  string
		start  time.Time
		budget time.Duration
		runs   int
	}{{"3h", h3, twoS, 5}, {"24h", h24, uiTimeout, 3}, {"7d", d7, uiTimeout, 3}} {
		add("top_by_calls "+r.label, "top_by_calls.sql", r.start, r.budget, r.runs, with(r.start, end, 50, nil, nil)...)
		add("top_by_calls "+r.label+" role=replica", "top_by_calls.sql", r.start, r.budget, r.runs, with(r.start, end, 50, testdb.ReportReplicaRole, nil)...)
		add("top_by_total_time "+r.label, "top_by_total_time.sql", r.start, r.budget, r.runs, with(r.start, end, 50, nil, nil)...)
		add("outliers "+r.label, "outliers.sql", r.start, r.budget, r.runs, with(r.start, end, 50, 3.0, 30, 2.0, nil, nil)...)
		add("outliers "+r.label+" role=primary", "outliers.sql", r.start, r.budget, r.runs, with(r.start, end, 50, 3.0, 30, 2.0, testdb.ReportPrimaryRole, nil)...)
		add("replica_utilization_by_controller_action "+r.label, "replica_utilization_by_controller_action.sql", r.start, r.budget, r.runs,
			with(r.start, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, nil)...)
		add("replica_utilization_by_job "+r.label, "replica_utilization_by_job.sql", r.start, r.budget, r.runs,
			with(r.start, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, nil)...)
		// A match on a few controller#action contexts and no query text, so the context
		// EXISTS does real work.
		add("top_by_calls "+r.label+" match", "top_by_calls.sql", r.start, r.budget, r.runs, with(r.start, end, 50, nil, "^controller1[0-9]#")...)
		add("outliers "+r.label+" match", "outliers.sql", r.start, r.budget, r.runs, with(r.start, end, 50, 3.0, 30, 2.0, nil, "^controller1[0-9]#")...)
		add("replica_utilization_by_controller_action "+r.label+" match", "replica_utilization_by_controller_action.sql", r.start, r.budget, r.runs,
			with(r.start, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, "^controller1[0-9]#")...)
	}
	// replica_utilization's cost grows with the cluster's events in the
	// range, so it also runs over all 21 seeded days: the longest custom range.
	for _, file := range []string{"replica_utilization_by_controller_action", "replica_utilization_by_job"} {
		add(file+" 21d", file+".sql", d21, uiTimeout, 3, with(d21, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, nil)...)
	}
	// The fingerprint page runs these four together, with the UI's automatic
	// bucket: the smallest that gives at most 200 buckets.
	for _, r := range []struct {
		label, bucket string
		start         time.Time
		budget        time.Duration
	}{
		{"3h", "1 minute", h3, twoS},
		{"7d", "1 hour", d7, twoS},
		{"21d", "1 day", d21, twoS},
	} {
		for _, fp := range []struct {
			label string
			id    int64
		}{{"hot", f.hotFP}, {"typical", f.typicalFP}} {
			add("fingerprint_timeseries "+r.label+" "+fp.label, "fingerprint_timeseries.sql", r.start, r.budget, 5, with(fp.id, r.start, end, r.bucket, nil)...)
			add("fingerprint_timeseries "+r.label+" "+fp.label+" role=replica", "fingerprint_timeseries.sql", r.start, r.budget, 5, with(fp.id, r.start, end, r.bucket, testdb.ReportReplicaRole)...)
			add("fingerprint_contexts "+r.label+" "+fp.label, "fingerprint_contexts.sql", r.start, r.budget, 5, with(fp.id, r.start, end, 10, nil)...)
			add("fingerprint_sources "+r.label+" "+fp.label, "fingerprint_sources.sql", r.start, r.budget, 5, with(fp.id, r.start, end, nil)...)
			add("fingerprint_all_sources "+r.label+" "+fp.label, "fingerprint_all_sources.sql", r.start, r.budget, 5, fp.id, r.start, end)
		}
	}
	return cases
}

func TestPerfReports(t *testing.T) {
	db := testdb.StartRotten(t)
	f := seedPerf(t, db)
	conn := db.Connect(t)
	ui := db.ConnectAs(t, testdb.UIRole)
	partitionsOf := perfPartitionsOf(t, conn)

	var hasIndex bool
	if err := conn.QueryRow(context.Background(),
		"select exists (select from pg_class where relname = $1 and relnamespace = 'rotten'::regnamespace)", perfIndex).Scan(&hasIndex); err != nil {
		t.Fatal(err)
	}
	t.Logf("index %s present: %v", perfIndex, hasIndex)

	var lines []string
	withIndex := map[string]perfResult{}
	for _, c := range perfCases(f) {
		for _, mode := range perfModes {
			r := runPerfCase(t, ui, c, mode, partitionsOf)
			if !r.timedOut {
				assertPerfPruning(t, conn, c, r)
			}
			if r.median > c.budget {
				t.Errorf("%s (%s): median %s over budget %s (runs %v)", c.name, mode, r.median, c.budget, r.runs)
			}
			withIndex[c.name+" "+mode] = r
			lines = append(lines, perfLine(c, mode, r))
		}
	}
	t.Logf("results (anchor %s):\n%s", f.anchor.Format(time.RFC3339), strings.Join(lines, "\n"))

	// The index decision. events (fingerprint_id, observed_window_start)
	// is for the per-fingerprint reports, so rerun those with it dropped in
	// a transaction that's rolled back, and require it to win clearly for a
	// typical fingerprint on the longer ranges. The hot fingerprint, which
	// has an event in nearly every window, gains less; it's logged.
	if !hasIndex {
		t.Fatalf("index %s is missing; the per-fingerprint reports read every page of the source's partitions without it", perfIndex)
	}
	ctx := context.Background()
	logPerfIndexSizes(t, conn)
	insertWith, inserted := timePerfInserts(t, conn, f.anchor)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "drop index rotten."+perfIndex); err != nil {
		t.Fatal(err)
	}
	insertWithout, _ := timePerfInserts(t, tx, f.anchor)
	t.Logf("inserting one hour of windows (%d events): %s with %s, %s without", inserted, insertWith.Round(time.Millisecond), perfIndex, insertWithout.Round(time.Millisecond))
	if _, err := tx.Exec(ctx, "set local role "+testdb.UIRole); err != nil {
		t.Fatal(err)
	}
	lines = nil
	for _, c := range perfCases(f) {
		if !strings.HasPrefix(c.name, "fingerprint_") {
			continue
		}
		for _, mode := range perfModes {
			without := runPerfCase(t, tx, c, mode, partitionsOf)
			with := withIndex[c.name+" "+mode]
			lines = append(lines, fmt.Sprintf("| %s | %s | %s | %s |", c.name, strings.TrimSuffix(strings.TrimPrefix(mode, "force_"), "_plan"),
				without.median.Round(time.Millisecond), with.median.Round(time.Millisecond)))
			if strings.Contains(c.name, " typical") && !strings.Contains(c.name, " 3h ") && with.median*2 > without.median {
				t.Errorf("%s (%s): with %s %s, without %s; want at least twice as fast", c.name, mode, perfIndex, with.median, without.median)
			}
		}
	}
	t.Logf("per-fingerprint reports without and with %s:\n| case | plan | without | with |\n%s", perfIndex, strings.Join(lines, "\n"))

	// What migration 0008 costs on a database this size: rebuild the index
	// with the migration's own statement, still inside the transaction.
	if _, err := tx.Exec(ctx, "reset role"); err != nil {
		t.Fatal(err)
	}
	build := perfMigrationUp(t, "../migrations/0008_events_fingerprint_window.sql")
	began := time.Now()
	if _, err := tx.Exec(ctx, build); err != nil {
		t.Fatalf("rebuild %s: %v", perfIndex, err)
	}
	t.Logf("building %s (migration 0008) on %d events took %s, holding a SHARE lock on rotten.events and its partitions throughout",
		perfIndex, f.events, time.Since(began).Round(time.Millisecond))
}

// perfMigrationUp returns a goose migration's Up section.
func perfMigrationUp(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	up, _, ok := strings.Cut(string(b), "-- +goose Down")
	if !ok || !strings.Contains(up, "-- +goose Up") {
		t.Fatalf("%s: no goose Up/Down sections", path)
	}
	return up
}

var perfModes = []string{"force_custom_plan", "force_generic_plan"}

// logPerfIndexSizes logs the events heap and each of its indexes, summed
// over the partitions.
func logPerfIndexSizes(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		select 'heap', pg_size_pretty(sum(pg_relation_size(relid))) from pg_partition_tree('rotten.events')
		union all
		select c.relname::text, pg_size_pretty((select sum(pg_relation_size(relid)) from pg_partition_tree(c.oid)))
		from pg_index x join pg_class c on c.oid = x.indexrelid
		where x.indrelid = 'rotten.events'::regclass`)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		var name, size string
		if err := rows.Scan(&name, &size); err != nil {
			t.Fatal(err)
		}
		out = append(out, name+" "+size)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("rotten.events sizes: %s", strings.Join(out, ", "))
}

// timePerfInserts times inserting the next hour of windows for every
// stream, the seed's way, rolled back each time. It's a rough measure of
// what one more index costs ingest. It returns the median and the row count.
func timePerfInserts(t *testing.T, conn perfConn, anchor time.Time) (time.Duration, int64) {
	t.Helper()
	ctx := context.Background()
	var runs []time.Duration
	var n int64
	for i := 0; i < 5; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "set local enable_memoize = off"); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		tag, err := tx.Exec(ctx, perfSeedDaySQL, anchor, anchor.Add(time.Hour), anchor)
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, time.Since(began))
		n = tag.RowsAffected()
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(runs)
	return runs[len(runs)/2], n
}

func perfLine(c perfCase, mode string, r perfResult) string {
	short := strings.TrimSuffix(strings.TrimPrefix(mode, "force_"), "_plan")
	if r.timedOut {
		return fmt.Sprintf("| %s | %s | timed out (%s) |", c.name, short, perfUITimeout)
	}
	var parts []string
	for _, parent := range []string{"rotten.events", "rotten.event_context"} {
		if r.summary.inPlan[parent] != nil {
			parts = append(parts, fmt.Sprintf("%s %d/%d", strings.TrimPrefix(parent, "rotten."), len(r.summary.scanned[parent]), len(r.summary.inPlan[parent])))
		}
	}
	return fmt.Sprintf("| %s | %s | %s | %d | %s | removed %d | hit %.0f read %.0f | %s |",
		c.name, short, r.median.Round(time.Millisecond), r.rows, strings.Join(parts, ", "),
		r.summary.subplansRemoved, r.summary.sharedHit, r.summary.sharedRead,
		strings.Join(sortedKeys(r.summary.accessPaths), "; "))
}
