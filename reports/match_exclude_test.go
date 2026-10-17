package reports_test

import (
	"reflect"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

// Match takes several patterns, OR'd, and exclude takes several too: a row
// any exclude pattern matches is dropped first, then match filters the rest.
// Both are text[] binds, NULL for no filter.

func TestTopReportsMatchSeveralAndExclude(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	start, end := fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor
	keys := fingerprintKeys(fixture)

	run := func(report string, match, exclude any) []string {
		t.Helper()
		got := []string{}
		for _, row := range readReportMaps(t, conn, report,
			[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}, start, end, 50, nil, match, exclude) {
			got = append(got, keys[row["fingerprint_id"].(int64)])
		}
		return got
	}
	// users, courses and jobs ran on canvas 13 in the range. Neither pattern
	// matches a context.
	both := []string{"from users", "delayed_jobs"}
	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		if got := run(report, both, nil); !sameSet(got, []string{"users", "jobs"}) {
			t.Errorf("%s: matches %v = %v, want users and jobs", report, both, got)
		}
		if got := run(report, both, []string{"delayed_jobs"}); !reflect.DeepEqual(got, []string{"users"}) {
			t.Errorf("%s: matches %v, exclude delayed_jobs = %v, want only users", report, both, got)
		}
		// Exclude alone drops what it matches. users ran in courses#show,
		// so excluding courses drops it too.
		if got := run(report, nil, []string{"delayed_jobs", "courses"}); len(got) != 0 {
			t.Errorf("%s: exclude delayed_jobs and courses = %v, want none", report, got)
		}
		if got := run(report, nil, []string{"delayed_jobs"}); !sameSet(got, []string{"users", "courses"}) {
			t.Errorf("%s: exclude delayed_jobs = %v, want users and courses", report, got)
		}
		// A context exclude drops a query that ran in that context: jobs ran as SendEmail.
		if got := run(report, nil, []string{"^SendEmail$"}); !sameSet(got, []string{"users", "courses"}) {
			t.Errorf("%s: exclude ^SendEmail$ = %v, want users and courses", report, got)
		}
	}
}

func TestOutliersMatchSeveralAndExclude(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	run := func(match, exclude any) int {
		t.Helper()
		return len(readOutliers(t, conn, []string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"7"},
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10, defaultSigma, defaultMinHistory, defaultRatio, nil, match, exclude))
	}
	// slow, on submissions, is canvas 7's only outlier.
	if n := run([]string{"from users", "submissions"}, nil); n != 1 {
		t.Errorf("matches users and submissions: %d outliers, want 1", n)
	}
	if n := run([]string{"from users", "submissions"}, []string{"submissions"}); n != 0 {
		t.Errorf("exclude submissions: %d outliers, want 0", n)
	}
}

func TestReplicaUtilizationMatchSeveralAndExclude(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	got := map[string]bool{}
	for _, row := range readReplicaUtilization(t, conn, "replica_utilization_by_job.sql",
		[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"},
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, testdb.ReportPrimaryRole, testdb.ReportReplicaRole,
		[]string{"^Send", "^Re"}, []string{"^Reindex$"}) {
		got[row.Name] = true
	}
	if want := map[string]bool{"SendEmail": true, "ReplicaReport": true}; !reflect.DeepEqual(got, want) {
		t.Errorf("jobs = %v, want %v", got, want)
	}
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			return false
		}
		seen[w]--
	}
	return true
}
