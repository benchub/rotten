package reports_test

import (
	"context"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// topFingerprints runs a top report on canvas 13, every role, the recent
// range, and returns its fingerprints by fixture key, in order.
func topFingerprints(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, report string, limit int, match any) []string {
	t.Helper()
	query, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query),
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, limit, nil, patterns(match), nil, nil, false)
	if err != nil {
		t.Fatalf("%s match %v: %v", report, match, err)
	}
	defer rows.Close()
	keys := fingerprintKeys(fixture)
	got := []string{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, keys[values[0].(int64)])
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func fingerprintKeys(fixture *testdb.Reports) map[int64]string {
	keys := map[int64]string{}
	for key, id := range fixture.FingerprintID {
		keys[id] = key
	}
	return keys
}

func TestTopReportsMatch(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	cases := []struct {
		report string
		limit  int
		match  any
		want   []string
	}{
		{"top_by_calls.sql", 10, nil, []string{"users", "courses", "jobs"}},
		// Query text, case-insensitively.
		{"top_by_calls.sql", 10, "USERS", []string{"users"}},
		// users ran in courses#show, so it matches on that context.
		{"top_by_calls.sql", 10, "courses", []string{"users", "courses"}},
		{"top_by_total_time.sql", 10, "courses", []string{"users", "courses"}},
		// A job tag.
		{"top_by_calls.sql", 10, "reindex", []string{"jobs"}},
		// controller#action is matched as one string.
		{"top_by_calls.sql", 10, "s#sh", []string{"users"}},
		// A job-only context has no controller#action to match.
		{"top_by_calls.sql", 10, "^#$", []string{}},
		// The filter runs before the limit.
		{"top_by_calls.sql", 1, "^update", []string{"jobs"}},
		{"top_by_total_time.sql", 1, "^select \\* from courses", []string{"courses"}},
		// Contexts from another project, or outside the range, don't count.
		{"top_by_calls.sql", 10, "SyncLearners|programs", []string{}},
		{"top_by_calls.sql", 10, "no such thing", []string{}},
	}
	for _, c := range cases {
		if got := topFingerprints(t, conn, fixture, c.report, c.limit, c.match); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s limit %d match %v = %v, want %v", c.report, c.limit, c.match, got, c.want)
		}
	}
}

func TestTopReportsMatchKeepsWholeFingerprintTotals(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	query, err := os.ReadFile("top_by_calls.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Only the replica's grades#show context matches, but users' totals are
	// all its calls on both roles, and its top contexts are all of them.
	var calls float64
	var contexts []byte
	if err := conn.QueryRow(context.Background(), "select calls, context from ("+trimSemicolon(string(query))+") r",
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10, nil, []string{"grades#show"}, nil, nil, false).Scan(&calls, &contexts); err != nil {
		t.Fatal(err)
	}
	want := testdb.RecentTotals()[testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: "users"}]
	if calls != want.Calls {
		t.Errorf("calls = %v, want %v", calls, want.Calls)
	}
	var unfiltered []byte
	if err := conn.QueryRow(context.Background(), "select context from ("+trimSemicolon(string(query))+") r where fingerprint_id = $12",
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10, nil, nil, nil, nil, false, fixture.FingerprintID["users"]).Scan(&unfiltered); err != nil {
		t.Fatal(err)
	}
	if string(contexts) != string(unfiltered) {
		t.Errorf("contexts = %s, want %s", contexts, unfiltered)
	}
}

func trimSemicolon(sql string) string {
	for len(sql) > 0 && (sql[len(sql)-1] == ';' || sql[len(sql)-1] == '\n' || sql[len(sql)-1] == ' ') {
		sql = sql[:len(sql)-1]
	}
	return sql
}

func TestOutliersMatch(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	keys := fingerprintKeys(fixture)

	for _, c := range []struct {
		match any
		want  []string
	}{
		{nil, []string{"slow"}},
		{"SUBMISSIONS", []string{"slow"}},
		{"#index$", []string{"slow"}},
		{"users", []string{}},
	} {
		got := []string{}
		for _, row := range readOutliers(t, conn,
			[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"7"},
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
			10, defaultSigma, defaultMinHistory, defaultRatio, nil, c.match) {
			got = append(got, keys[row.FingerprintID])
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("match %v = %v, want %v", c.match, got, c.want)
		}
	}
}

func TestReplicaUtilizationMatch(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	for _, c := range []struct {
		report string
		match  any
		want   []string
	}{
		{"replica_utilization_by_controller_action.sql", "^USERS#", []string{"users#index", "users#show"}},
		{"replica_utilization_by_controller_action.sql", "s#sh", []string{"courses#show", "grades#show", "users#show"}},
		{"replica_utilization_by_controller_action.sql", "SendEmail", []string{}},
		{"replica_utilization_by_job.sql", "^re", []string{"Reindex", "ReplicaReport"}},
		{"replica_utilization_by_job.sql", "#", []string{}},
	} {
		got := []string{}
		for _, row := range readReplicaUtilization(t, conn, c.report,
			[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
			testdb.ReportPrimaryRole, testdb.ReportReplicaRole, c.match) {
			got = append(got, row.Name)
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s match %v = %v, want %v", c.report, c.match, got, c.want)
		}
	}
}

func TestMatchPrunesEventContextPartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	expected := partitionsOverlappingRange(t, conn, "rotten.event_context", start, end)
	if len(expected) != 2 {
		t.Fatalf("fixed range touches %d partitions, want 2", len(expected))
	}
	for report, args := range map[string][]any{
		"top_by_calls.sql":      {[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}, start, end, 50, nil, "users"},
		"top_by_total_time.sql": {[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}, start, end, 50, nil, "users"},
		"outliers.sql":          {[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}, start, end, 50, defaultSigma, defaultMinHistory, defaultRatio, nil, "users"},
	} {
		t.Run(report, func(t *testing.T) {
			plan := explainReport(t, conn, report, args...)
			assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.event_context", expected)
		})
	}
}

// patterns turns a single match pattern into the text[] the reports take,
// and leaves nil or a slice as it is.
func patterns(v any) any {
	if s, ok := v.(string); ok {
		return []string{s}
	}
	return v
}

// reportShape is a report's argument count, and the defaults for its
// trailing arguments: exclude, contexts and ignore-untagged, added after the
// tests that leave them off were written. match is the index of its match
// patterns, or -1.
type reportShape struct {
	match    int
	defaults []any
	full     int
}

var reportShapes = map[string]reportShape{
	"top_by_calls.sql":      {7, []any{nil, nil, false}, 11},
	"top_by_total_time.sql": {7, []any{nil, nil, false}, 11},
	"outliers.sql":          {10, []any{nil}, 12},
	"replica_utilization_by_controller_action.sql": {7, []any{nil, nil, false}, 11},
	"replica_utilization_by_job.sql":               {7, []any{nil, nil, false}, 11},
	"fingerprint_contexts.sql":                     {-1, []any{false}, 9},
	"fingerprint_sources.sql":                      {-1, []any{nil, false}, 9},
	"unparsed_summary.sql":                         {-1, []any{nil, false}, 8},
}

// fillReportArgs completes arguments that stop before a report's newer
// trailing parameters: a single match pattern becomes a text[], and each
// missing parameter gets its default (no exclude, no context filter,
// untagged kept).
func fillReportArgs(file string, args []any) []any {
	shape, ok := reportShapes[file]
	if !ok || len(args) >= shape.full || len(args) < shape.full-len(shape.defaults) {
		return args
	}
	out := append([]any{}, args...)
	if shape.match >= 0 && shape.match < len(out) {
		out[shape.match] = patterns(out[shape.match])
	}
	return append(out, shape.defaults[len(args)-(shape.full-len(shape.defaults)):]...)
}
