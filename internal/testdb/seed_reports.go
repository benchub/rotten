package testdb

import (
	"context"
	"sort"
	"testing"
	"time"
)

// The report fixture. Reports (tasks -43 through -47) filter on windows
// relative to now(), like "the last 3 hours", so the fixture can't use a fixed
// calendar anchor: pg_partman only made partitions around the time migrate
// ran, and now() in the report would drift away from it. Instead SeedReports
// takes Anchor = now() truncated to the minute, at seed time, and places every
// window at an explicit offset before it. Windows meant to be "recent" sit
// well inside the last 3 hours (ending at least 10 minutes before Anchor and
// starting at least 30 minutes after Anchor-3h), and "old" windows sit well
// outside it (4 hours or more before Anchor). A report test that runs within
// a few minutes of seeding sees the same split, and can also pass
// Anchor-RecentRange to a parameterized report for an exact boundary.

// ReportPrimaryRole and ReportReplicaRole are the role names the fixture
// uses. They deliberately differ from the legacy example queries' 'master'
// and 'slave', so a report that hard-codes those finds nothing.
const (
	ReportPrimaryRole = "primary"
	ReportReplicaRole = "replica"
	ReportEnvironment = "production"
)

// RecentRange is the "last 3 hours" the reports filter on.
const RecentRange = 3 * time.Hour

// WindowLength is the length of every seeded window.
const WindowLength = 10 * time.Minute

// SeedSource is one logical source in the fixture.
type SeedSource struct {
	Key, Project, Cluster, Role string
}

// SeedFingerprint is one fingerprint in the fixture.
type SeedFingerprint struct {
	Key, Fingerprint, Normalized string
}

// SeedContext is one event_context row for an event. Empty names are NULL.
type SeedContext struct {
	Controller, Action, JobTag string
	C                          int
}

// SeedEvent is one events row. StartAgo is how long before Anchor the window
// starts; the window ends WindowLength later.
type SeedEvent struct {
	Source, Fingerprint string
	StartAgo            time.Duration
	Calls, Time         float64
	Contexts            []SeedContext
}

// Recent reports whether the event's window is inside the last RecentRange.
func (e SeedEvent) Recent() bool {
	return e.StartAgo <= RecentRange && e.StartAgo-WindowLength >= 0
}

// SeedStat is one fingerprint_stats row. Source "" means logical source 0
// (all sources).
type SeedStat struct {
	Fingerprint, Source, Type string
	Count                     int64
	Mean, Deviation           float64
	Last                      int64
}

// ReportSources: two projects, canvas on clusters 13 and 7 with a primary and
// a replica each, and bridge on cluster 13 with a primary only.
var ReportSources = []SeedSource{
	{"canvas13p", "canvas", "13", ReportPrimaryRole},
	{"canvas13r", "canvas", "13", ReportReplicaRole},
	{"canvas7p", "canvas", "7", ReportPrimaryRole},
	{"canvas7r", "canvas", "7", ReportReplicaRole},
	{"bridge13p", "bridge", "13", ReportPrimaryRole},
}

var ReportFingerprints = []SeedFingerprint{
	{"users", "select * from users where id = 1", "select * from users where id = $1"},
	{"courses", "select * from courses where account_id = 1", "select * from courses where account_id = $1"},
	{"jobs", "update delayed_jobs set locked_by = 'w' where id = 1", "update delayed_jobs set locked_by = $1 where id = $2"},
	{"slow", "select * from submissions where assignment_id = 1", "select * from submissions where assignment_id = $1"},
}

const (
	m = time.Minute
	h = time.Hour
)

// ReportEvents is the fixture's events and contexts. Notes for report tests:
//   - canvas cluster 13, recent: users has the most calls, jobs the most time.
//   - users on canvas cluster 13 has seven distinct recent contexts across
//     both roles, so a top-five list drops two. RecentTopContexts computes
//     the expected lists from this table.
//   - Replica utilization (-46) attributes calls by event_context.c and splits
//     event time across all contexts by c / sum(c) for that event. Each job
//     event has one context whose c equals the event's calls. On canvas 13:
//     "SendEmail" runs
//     only on the primary (30 calls), "Reindex" splits 30:10 primary:replica
//     (75% and 25%), and "ReplicaReport" runs only on the replica (15 calls).
//   - controller "grades"#"show" runs only on replicas.
//   - slow on canvas7p is the planted outlier: 40 ms/call recent vs a
//     history (slowHistory) of 40 windows before the last 3 hours with
//     median 8 ms/call and MAD 0.5.
//   - users on canvas13p (for -47): with 10-minute buckets aligned to Anchor,
//     recent buckets start 30, 50, and 90 minutes before Anchor, the bucket
//     70 minutes before is empty, and an older one sits 4 hours back.
//   - old windows (4h, 5h, and 26h ago) must drop out of "last 3 hours".
var ReportEvents = append([]SeedEvent{
	// canvas 13 primary, recent.
	{"canvas13p", "users", 30 * m, 500, 250, []SeedContext{
		{"users", "show", "", 200}, {"users", "index", "", 120}, {"courses", "show", "", 80},
		{"grades", "index", "", 50}, {"api", "list", "", 40}, {"login", "new", "", 1},
	}},
	{"canvas13p", "users", 50 * m, 100, 50, []SeedContext{{"users", "show", "", 100}}},
	{"canvas13p", "users", 90 * m, 300, 150, []SeedContext{{"users", "show", "", 300}}},
	{"canvas13p", "jobs", 60 * m, 30, 3000, []SeedContext{{"", "", "SendEmail", 30}}},
	{"canvas13p", "jobs", 80 * m, 30, 1000, []SeedContext{{"", "", "Reindex", 30}}},
	{"canvas13p", "courses", 120 * m, 100, 300, []SeedContext{{"courses", "index", "", 100}}},
	// canvas 13 replica, recent.
	{"canvas13r", "users", 45 * m, 200, 80, []SeedContext{{"grades", "show", "", 200}}},
	{"canvas13r", "jobs", 60 * m, 10, 900, []SeedContext{{"", "", "Reindex", 10}}},
	{"canvas13r", "jobs", 110 * m, 15, 300, []SeedContext{{"", "", "ReplicaReport", 15}}},
	// canvas 13, old.
	{"canvas13p", "users", 4 * h, 800, 400, []SeedContext{{"users", "show", "", 800}}},
	{"canvas13p", "courses", 4 * h, 9000, 90000, []SeedContext{{"courses", "index", "", 9000}}},
	{"canvas13r", "users", 26 * h, 7000, 7000, []SeedContext{{"grades", "show", "", 7000}}},
	// canvas 7, recent.
	{"canvas7p", "slow", 40 * m, 20, 800, []SeedContext{{"submissions", "index", "", 20}}},
	{"canvas7p", "users", 50 * m, 60, 30, []SeedContext{{"users", "show", "", 60}}},
	{"canvas7r", "users", 50 * m, 140, 70, []SeedContext{{"users", "show", "", 140}}},
	// bridge 13, recent and old.
	{"bridge13p", "courses", 20 * m, 75, 150, []SeedContext{{"programs", "show", "", 75}}},
	{"bridge13p", "users", 100 * m, 25, 10, []SeedContext{{"", "", "SyncLearners", 25}}},
	{"bridge13p", "users", 5 * h, 1000, 400, nil},
}, slowHistory()...)

// slowHistory is slow's history on canvas7p for the outliers report: 40
// windows ending at or before Anchor-3h, cycling 7, 7.5, 8, 8.5 and 9
// ms/call, so the median is 8 and the MAD 0.5.
func slowHistory() []SeedEvent {
	var out []SeedEvent
	for i := 0; i < 40; i++ {
		ms := []float64{7, 7.5, 8, 8.5, 9}[i%5]
		out = append(out, SeedEvent{"canvas7p", "slow", RecentRange + WindowLength + time.Duration(i)*WindowLength, 20, 20 * ms, nil})
	}
	return out
}

// ReportStats is the fixture's fingerprint_stats. The stored slow mean_time
// rows include the recent 40 ms sample, the way real ingest keeps current
// fingerprint_stats. Removing the in-range sample leaves source 0 at mean 5,
// deviation 1, and canvas7p at mean 8, deviation 0.9. users is a non-outlier
// control.
var ReportStats = []SeedStat{
	{"slow", "", "mean_time", 1001, 5.034965034965035, 1.4908977911903367, 5},
	{"slow", "canvas7p", "mean_time", 801, 8.039950062421973, 1.4447800862079763, 8},
	{"users", "", "mean_time", 50000, 0.5, 0.2, 0},
	{"users", "canvas13p", "mean_time", 30000, 0.5, 0.1, 0},
	{"slow", "", "calls", 1000, 20, 3, 20},
}

// ReportProjects returns the distinct projects in ReportSources, sorted.
func ReportProjects() []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range ReportSources {
		if !seen[s.Project] {
			seen[s.Project] = true
			out = append(out, s.Project)
		}
	}
	sort.Strings(out)
	return out
}

func reportSource(key string) SeedSource {
	for _, s := range ReportSources {
		if s.Key == key {
			return s
		}
	}
	panic("testdb: unknown source " + key)
}

// GroupKey is a (project, cluster, fingerprint) group, across both roles.
type GroupKey struct{ Project, Cluster, Fingerprint string }

func (e SeedEvent) group() GroupKey {
	s := reportSource(e.Source)
	return GroupKey{s.Project, s.Cluster, e.Fingerprint}
}

// RecentTotals sums calls and time of recent events per group, straight
// from ReportEvents.
func RecentTotals() map[GroupKey]SeedTotals {
	out := map[GroupKey]SeedTotals{}
	for _, e := range ReportEvents {
		if !e.Recent() {
			continue
		}
		t := out[e.group()]
		t.Calls += e.Calls
		t.Time += e.Time
		out[e.group()] = t
	}
	return out
}

// RecentTopContexts returns a group's top n contexts over recent events,
// with c summed per (controller, action, job tag). Ties sort by controller,
// action, then job tag.
func RecentTopContexts(k GroupKey, n int) []SeedContext {
	sum := map[SeedContext]int{}
	for _, e := range ReportEvents {
		if !e.Recent() || e.group() != k {
			continue
		}
		for _, c := range e.Contexts {
			sum[SeedContext{c.Controller, c.Action, c.JobTag, 0}] += c.C
		}
	}
	var out []SeedContext
	for c, total := range sum {
		c.C = total
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.C != b.C {
			return a.C > b.C
		}
		if a.Controller != b.Controller {
			return a.Controller < b.Controller
		}
		if a.Action != b.Action {
			return a.Action < b.Action
		}
		return a.JobTag < b.JobTag
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// SeedTotals sums calls and time.
type SeedTotals struct{ Calls, Time float64 }

// Reports is what SeedReports returns.
type Reports struct {
	Anchor        time.Time
	SourceIDs     map[string]int   // SeedSource.Key -> logical_sources.id
	PhysicalIDs   map[string]int   // SeedSource.Key -> physical_sources.id
	FingerprintID map[string]int64 // SeedFingerprint.Key -> fingerprints.id
	EventIDs      []int64          // parallel to ReportEvents
	ControllerIDs map[string]int
	ActionIDs     map[string]int
	JobTagIDs     map[string]int

	// Totals per source key, over all events and over recent events only.
	TotalsBySource       map[string]SeedTotals
	RecentTotalsBySource map[string]SeedTotals
}

// WindowStart returns event i's observed_window_start.
func (r *Reports) WindowStart(i int) time.Time {
	return r.Anchor.Add(-ReportEvents[i].StartAgo)
}

// SeedReports loads the report fixture into db, which StartRotten made.
func SeedReports(t testing.TB, db *DB) *Reports {
	t.Helper()
	ctx := context.Background()
	conn := db.Connect(t)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("seed: begin: %v", err)
	}
	defer tx.Rollback(ctx)

	r := &Reports{
		SourceIDs:            map[string]int{},
		PhysicalIDs:          map[string]int{},
		FingerprintID:        map[string]int64{},
		ControllerIDs:        map[string]int{},
		ActionIDs:            map[string]int{},
		JobTagIDs:            map[string]int{},
		TotalsBySource:       map[string]SeedTotals{},
		RecentTotalsBySource: map[string]SeedTotals{},
	}
	if err := tx.QueryRow(ctx, "select date_trunc('minute', now())").Scan(&r.Anchor); err != nil {
		t.Fatalf("seed: anchor: %v", err)
	}

	physical := map[string]int{}
	for _, s := range ReportSources {
		var id int
		if err := tx.QueryRow(ctx,
			"insert into rotten.logical_sources (project, environment, cluster, role) values ($1,$2,$3,$4) returning id",
			s.Project, ReportEnvironment, s.Cluster, s.Role).Scan(&id); err != nil {
			t.Fatalf("seed: logical source %s: %v", s.Key, err)
		}
		r.SourceIDs[s.Key] = id
		var pid int
		if err := tx.QueryRow(ctx, "insert into rotten.physical_sources (fqdn) values ($1) returning id",
			s.Key+".db.example").Scan(&pid); err != nil {
			t.Fatalf("seed: physical source %s: %v", s.Key, err)
		}
		physical[s.Key] = pid
		r.PhysicalIDs[s.Key] = pid
	}
	for _, f := range ReportFingerprints {
		var id int64
		if err := tx.QueryRow(ctx,
			"insert into rotten.fingerprints (fingerprint, normalized) values ($1,$2) returning id",
			f.Fingerprint, f.Normalized).Scan(&id); err != nil {
			t.Fatalf("seed: fingerprint %s: %v", f.Key, err)
		}
		r.FingerprintID[f.Key] = id
	}
	dim := func(table, col, name string, ids map[string]int) *int {
		if name == "" {
			return nil
		}
		if id, ok := ids[name]; ok {
			return &id
		}
		var id int
		if err := tx.QueryRow(ctx, "insert into rotten."+table+" ("+col+") values ($1) returning id", name).Scan(&id); err != nil {
			t.Fatalf("seed: %s %s: %v", table, name, err)
		}
		ids[name] = id
		return &id
	}

	for i, e := range ReportEvents {
		start := r.WindowStart(i)
		end := start.Add(WindowLength)
		var id int64
		if err := tx.QueryRow(ctx, `insert into rotten.events
			(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			values ($1,$2,$3,$4,$5,$6,$7) returning id`,
			r.FingerprintID[e.Fingerprint], r.SourceIDs[e.Source], physical[e.Source], start, end, e.Calls, e.Time).Scan(&id); err != nil {
			t.Fatalf("seed: event %d: %v", i, err)
		}
		r.EventIDs = append(r.EventIDs, id)
		for _, c := range e.Contexts {
			if _, err := tx.Exec(ctx, `insert into rotten.event_context
				(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
				values ($1,$2,$3,$4,$5,$6,$7)`,
				id, start, end,
				dim("controllers", "controller", c.Controller, r.ControllerIDs),
				dim("actions", "action", c.Action, r.ActionIDs),
				dim("job_tags", "job_tag", c.JobTag, r.JobTagIDs), c.C); err != nil {
				t.Fatalf("seed: event %d context: %v", i, err)
			}
		}
		add := func(m map[string]SeedTotals) {
			s := m[e.Source]
			s.Calls += e.Calls
			s.Time += e.Time
			m[e.Source] = s
		}
		add(r.TotalsBySource)
		if e.Recent() {
			add(r.RecentTotalsBySource)
		}
	}

	for _, s := range ReportStats {
		src := 0
		if s.Source != "" {
			src = r.SourceIDs[s.Source]
		}
		if _, err := tx.Exec(ctx, `insert into rotten.fingerprint_stats
			(fingerprint_id, logical_source_id, type, count, mean, deviation, last)
			values ($1,$2,$3,$4,$5,$6,$7)`,
			r.FingerprintID[s.Fingerprint], src, s.Type, s.Count, s.Mean, s.Deviation, s.Last); err != nil {
			t.Fatalf("seed: stat %+v: %v", s, err)
		}
	}
	if _, err := tx.Exec(ctx, FillContextUtilizationSQL); err != nil {
		t.Fatalf("seed: context utilization: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("seed: commit: %v", err)
	}
	return r
}

// FillContextUtilizationSQL sets event_context.logical_source_id and
// attributed_time the way ingest and migration 0011 do, on rows a fixture
// inserted directly. Replica utilization reads only those columns, so it
// skips context rows without them.
const FillContextUtilizationSQL = `
update rotten.event_context ec
set logical_source_id = e.logical_source_id,
    attributed_time = e.time * ec.c::double precision / t.total::double precision
from rotten.events e,
     (select event_id, observed_window_start, sum(c) as total
      from rotten.event_context
      group by event_id, observed_window_start) t
where ec.attributed_time is null
  and e.id = ec.event_id
  and e.observed_window_start = ec.observed_window_start
  and t.event_id = ec.event_id
  and t.observed_window_start = ec.observed_window_start`
