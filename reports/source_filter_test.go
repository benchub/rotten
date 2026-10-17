package reports_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

// Every report's project, environment, cluster and role are text arrays: a
// source is in the filter when each of its fields is one of that array's
// values, and a NULL array means every value.

func TestTopByCallsFiltersOnSourceArrays(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	projectOf := map[string]string{}
	clusterOf := map[string]string{}
	for _, s := range testdb.ReportSources {
		projectOf[s.Key] = s.Project
		clusterOf[s.Key] = s.Cluster
	}
	want := func(keep func(source string) bool) map[int64]float64 {
		out := map[int64]float64{}
		for _, e := range testdb.ReportEvents {
			if e.Recent() && keep(e.Source) {
				out[fixture.FingerprintID[e.Fingerprint]] += e.Calls
			}
		}
		return out
	}
	got := func(project, cluster any) map[int64]float64 {
		out := map[int64]float64{}
		for _, row := range readReportMaps(t, conn, "top_by_calls.sql",
			project, []string{testdb.ReportEnvironment}, cluster,
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 50, nil, nil) {
			out[row["fingerprint_id"].(int64)] = row["calls"].(float64)
		}
		return out
	}

	cases := []struct {
		name             string
		project, cluster any
		keep             func(string) bool
	}{
		{"canvas, clusters 13 and 7", []string{"canvas"}, []string{"13", "7"},
			func(s string) bool { return projectOf[s] == "canvas" }},
		{"canvas, cluster 7", []string{"canvas"}, []string{"7"},
			func(s string) bool { return projectOf[s] == "canvas" && clusterOf[s] == "7" }},
		{"every project, cluster 13", nil, []string{"13"},
			func(s string) bool { return clusterOf[s] == "13" }},
		{"bridge and canvas, every cluster", []string{"bridge", "canvas"}, nil,
			func(string) bool { return true }},
	}
	for _, c := range cases {
		if g, w := got(c.project, c.cluster), want(c.keep); !reflect.DeepEqual(g, w) {
			t.Errorf("%s: calls by fingerprint = %v, want %v", c.name, g, w)
		}
	}
}

func TestEveryReportTakesSourceArrays(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	start, end := fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor
	src := []any{[]string{"canvas", "bridge"}, []string{testdb.ReportEnvironment}, []string{"13", "7"}}
	roles := []string{testdb.ReportPrimaryRole, testdb.ReportReplicaRole}
	users := fixture.FingerprintID["users"]
	cases := map[string][]any{
		"top_by_calls.sql":                             append(append([]any{}, src...), start, end, 50, roles, nil),
		"top_by_total_time.sql":                        append(append([]any{}, src...), start, end, 50, roles, nil),
		"outliers.sql":                                 append(append([]any{}, src...), start, end, 50, defaultSigma, defaultMinHistory, defaultRatio, roles, nil),
		"replica_utilization_by_controller_action.sql": append(append([]any{}, src...), start, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, nil),
		"replica_utilization_by_job.sql":               append(append([]any{}, src...), start, end, testdb.ReportPrimaryRole, testdb.ReportReplicaRole, nil),
		"fingerprint_timeseries.sql":                   append(append([]any{}, src...), users, start, end, "30 minutes", roles),
		"fingerprint_context_timeseries.sql":           append(append([]any{}, src...), users, start, end, "30 minutes", roles, []string{"users#show#"}, false),
		"fingerprint_contexts.sql":                     append(append([]any{}, src...), users, start, end, 10, roles),
		"fingerprint_sources.sql":                      append(append([]any{}, src...), users, start, end, roles),
		"unparsed_summary.sql":                         append(append([]any{}, src...), start, end, roles),
	}
	files := make([]string, 0, len(cases))
	for file := range cases {
		files = append(files, file)
	}
	sort.Strings(files)
	for _, file := range files {
		if rows := readReportMaps(t, conn, file, cases[file]...); len(rows) == 0 {
			t.Errorf("%s returned no rows for both projects and clusters", file)
		}
	}
}
