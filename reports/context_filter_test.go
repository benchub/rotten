package reports_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// A context filter (text[] of "controller#action#job_tag", empty parts for
// none) makes a report count only the calls and time event_context
// attributes to those contexts. Ignoring untagged leaves out the untagged
// context's calls and time, and its row in context lists.

type contextTotals struct {
	Calls, TotalMS float64
}

// contextSums reads the expected totals straight from event_context: canvas
// 13, the recent range, for each fingerprint key, over the rows keep allows.
func contextSums(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, keep string) map[string]contextTotals {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		select e.fingerprint_id, sum(ec.c)::double precision, sum(ec.attributed_time)::double precision
		from rotten.event_context ec
		join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start
		join rotten.logical_sources s on s.id = e.logical_source_id
		left join rotten.controllers c on c.id = ec.controller_id
		left join rotten.actions a on a.id = ec.action_id
		left join rotten.job_tags j on j.id = ec.job_tag_id
		where s.project = 'canvas' and s.cluster = '13'
		  and ec.observed_window_start >= $1 and ec.observed_window_start < $2 and ec.observed_window_end <= $2
		  and `+keep+`
		group by e.fingerprint_id`, fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	keys := fingerprintKeys(fixture)
	out := map[string]contextTotals{}
	for rows.Next() {
		var id int64
		var c contextTotals
		if err := rows.Scan(&id, &c.Calls, &c.TotalMS); err != nil {
			t.Fatal(err)
		}
		out[keys[id]] = c
	}
	return out
}

func topTotals(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, report string, contexts any, ignoreUntagged bool) map[string]contextTotals {
	t.Helper()
	keys := fingerprintKeys(fixture)
	out := map[string]contextTotals{}
	for _, row := range readReportMaps(t, conn, report,
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 50, nil, nil, nil, contexts, ignoreUntagged) {
		out[keys[row["fingerprint_id"].(int64)]] = contextTotals{row["calls"].(float64), row["total_ms"].(float64)}
	}
	return out
}

func assertTotals(t *testing.T, name string, got, want map[string]contextTotals) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %v, want %v", name, got, want)
		return
	}
	for key, w := range want {
		g, ok := got[key]
		if !ok || g.Calls != w.Calls || !floatsClose(g.TotalMS, w.TotalMS) {
			t.Errorf("%s: %s = %+v, want %+v", name, key, g, w)
		}
	}
}

func floatsClose(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}

func TestTopReportsCountOnlyTheFilteredContexts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		assertTotals(t, report+" users#show", topTotals(t, conn, fixture, report, []string{"users#show#"}, false),
			contextSums(t, conn, fixture, "c.controller = 'users' and a.action = 'show' and ec.job_tag_id is null"))
		assertTotals(t, report+" users#show and SendEmail",
			topTotals(t, conn, fixture, report, []string{"users#show#", "##SendEmail"}, false),
			contextSums(t, conn, fixture, "((c.controller = 'users' and a.action = 'show' and ec.job_tag_id is null) or (ec.controller_id is null and ec.action_id is null and j.job_tag = 'SendEmail'))"))
		if got := topTotals(t, conn, fixture, report, []string{"nosuch#context#"}, false); len(got) != 0 {
			t.Errorf("%s: a context no row has = %v, want none", report, got)
		}
	}
	// The filtered contexts are the only ones listed.
	for _, row := range readReportMaps(t, conn, "top_by_calls.sql",
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 50, nil, nil, nil, []string{"users#show#"}, false) {
		contexts := row["context"].([]any)
		if len(contexts) != 1 || contexts[0].(map[string]any)["controller"] != "users" || contexts[0].(map[string]any)["action"] != "show" {
			t.Errorf("contexts = %v, want only users#show", contexts)
		}
	}
}

func TestTopReportsIgnoreUntagged(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	// A fingerprint whose only calls are untagged, and untagged calls on users.
	var fp, event int64
	start := fixture.Anchor.Add(-35 * time.Minute)
	if err := conn.QueryRow(ctx, `insert into rotten.fingerprints (fingerprint, normalized) values ('select 1', 'select $1') returning id`).Scan(&fp); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `insert into rotten.events
		(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
		values ($1, $2, $3, $4, $5, 40, 4) returning id`,
		fp, fixture.SourceIDs["canvas13p"], fixture.PhysicalIDs["canvas13p"], start, start.Add(testdb.WindowLength)).Scan(&event); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `insert into rotten.event_context
		(event_id, observed_window_start, observed_window_end, c, logical_source_id, attributed_time)
		values ($1, $2, $3, 40, $4, 4)`, event, start, start.Add(testdb.WindowLength), fixture.SourceIDs["canvas13p"]); err != nil {
		t.Fatal(err)
	}
	addUntaggedContext(t, conn, fixture, "canvas13p", "users", 30*time.Minute, 9)
	fixture.FingerprintID["only_untagged"] = fp

	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		all := topTotals(t, conn, fixture, report, nil, false)
		if all["only_untagged"].Calls != 40 {
			t.Errorf("%s: without the filter, only_untagged = %+v, want 40 calls", report, all["only_untagged"])
		}
		assertTotals(t, report+" ignoring untagged", topTotals(t, conn, fixture, report, nil, true),
			contextSums(t, conn, fixture, "(ec.controller_id is not null or ec.action_id is not null or ec.job_tag_id is not null)"))
	}
	for _, row := range readReportMaps(t, conn, "top_by_calls.sql",
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 50, nil, nil, nil, nil, true) {
		for _, c := range row["context"].([]any) {
			m := c.(map[string]any)
			if m["controller"] == nil && m["action"] == nil && m["job_tag"] == nil {
				t.Errorf("fingerprint %v lists the untagged context while ignoring it", row["fingerprint_id"])
			}
		}
	}
}

func TestReplicaUtilizationContextFilterAndIgnoreUntagged(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	addUntaggedContext(t, conn, fixture, "canvas13p", "users", 30*time.Minute, 9)
	run := func(file string, contexts any, ignore bool) map[string]float64 {
		t.Helper()
		out := map[string]float64{}
		for _, row := range readReplicaUtilization(t, conn, file,
			[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, testdb.ReportPrimaryRole, testdb.ReportReplicaRole,
			nil, nil, contexts, ignore) {
			out[row.Name] = row.TotalCalls
		}
		return out
	}
	if got, want := run("replica_utilization_by_controller_action.sql", []string{"users#show#", "grades#show#"}, false),
		map[string]float64{"users#show": 600, "grades#show": 200}; !reflect.DeepEqual(got, want) {
		t.Errorf("controller actions in users#show and grades#show = %v, want %v", got, want)
	}
	if got := run("replica_utilization_by_job.sql", nil, false); got[untaggedName] == 0 {
		t.Fatalf("jobs = %v, want an untagged row", got)
	}
	if got := run("replica_utilization_by_job.sql", nil, true); got[untaggedName] != 0 {
		t.Errorf("ignoring untagged, jobs = %v, want no untagged row", got)
	}
	if got, want := run("replica_utilization_by_job.sql", []string{"##SendEmail"}, false),
		map[string]float64{"SendEmail": 30}; !reflect.DeepEqual(got, want) {
		t.Errorf("jobs in SendEmail = %v, want %v", got, want)
	}
}

func TestFingerprintReportsContextFilterAndIgnoreUntagged(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	addUntaggedContext(t, conn, fixture, "canvas13p", "users", 30*time.Minute, 9)
	src := []any{[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}}
	users := fixture.FingerprintID["users"]
	start, end := fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor
	args := func(xs ...any) []any { return append(append([]any{}, src...), xs...) }
	sum := func(rows []map[string]any, key string) float64 {
		total := 0.0
		for _, row := range rows {
			switch v := row[key].(type) {
			case int64:
				total += float64(v)
			case float64:
				total += v
			}
		}
		return total
	}

	// The time series in users#show and grades#show.
	series := readReportMaps(t, conn, "fingerprint_context_timeseries.sql", args(users, start, end, "3 hours", nil, []string{"users#show#", "grades#show#"}, false)...)
	if got := sum(series, "calls"); got != 800 {
		t.Errorf("time series calls in users#show and grades#show = %v, want 800", got)
	}
	// Every tagged context, with the untagged one ignored.
	all := sum(readReportMaps(t, conn, "fingerprint_timeseries.sql", args(users, start, end, "3 hours", nil)...), "calls")
	tagged := sum(readReportMaps(t, conn, "fingerprint_context_timeseries.sql", args(users, start, end, "3 hours", nil, nil, true)...), "calls")
	untagged := sum(readReportMaps(t, conn, "fingerprint_context_timeseries.sql", args(users, start, end, "3 hours", nil, []string{"##"}, false)...), "calls")
	if untagged != 9 || tagged >= all {
		t.Errorf("tagged = %v, untagged = %v, all = %v; want untagged 9 and tagged below all", tagged, untagged, all)
	}

	// Contexts: the untagged row goes when it's ignored.
	hasUntaggedRow := func(ignore bool) bool {
		for _, row := range readReportMaps(t, conn, "fingerprint_contexts.sql", args(users, start, end, 10, nil, ignore)...) {
			if row["controller"] == nil && row["action"] == nil && row["job_tag"] == nil {
				return true
			}
		}
		return false
	}
	if !hasUntaggedRow(false) || hasUntaggedRow(true) {
		t.Errorf("untagged context row: %v without ignoring, %v ignoring; want true, false", hasUntaggedRow(false), hasUntaggedRow(true))
	}

	// Stats for each source count only the context's calls.
	sources := readReportMaps(t, conn, "fingerprint_sources.sql", args(users, start, end, nil, []string{"grades#show#"}, false)...)
	if got := sum(sources, "calls"); got != 200 {
		t.Errorf("source calls in grades#show = %v, want 200", got)
	}

	// The unparsed summary counts only unparsed calls in the contexts.
	markUnparsed(t, conn, users)
	summary := readReportMaps(t, conn, "unparsed_summary.sql", args(start, end, nil, []string{"users#show#"}, false)...)
	if got := sum(summary, "calls"); got != 600 {
		t.Errorf("unparsed calls in users#show = %v, want 600", got)
	}
}

// With a context filter, Match and Exclude look only at the filtered
// contexts (and the query text): users also ran in grades#show, but with
// the filter on users#show an exclude of grades doesn't drop it.
func TestContextFilterLimitsWhichContextsMatchAndExcludeSee(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	keys := fingerprintKeys(fixture)
	run := func(report string, match, exclude, contexts any) []string {
		t.Helper()
		got := []string{}
		for _, row := range readReportMaps(t, conn, report,
			[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 50, nil, match, exclude, contexts, false) {
			got = append(got, keys[row["fingerprint_id"].(int64)])
		}
		return got
	}
	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		if got := run(report, nil, []string{"grades"}, nil); sameSet(got, []string{"courses", "jobs"}) == false {
			t.Errorf("%s: unfiltered exclude grades = %v, want courses and jobs", report, got)
		}
		if got := run(report, nil, []string{"grades"}, []string{"users#show#"}); !reflect.DeepEqual(got, []string{"users"}) {
			t.Errorf("%s: users#show, exclude grades = %v, want users", report, got)
		}
		if got := run(report, []string{"grades"}, nil, []string{"users#show#"}); len(got) != 0 {
			t.Errorf("%s: users#show, match grades = %v, want none", report, got)
		}
	}
}
