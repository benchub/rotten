package reports_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// Untagged calls are stored as event_context rows with controller_id,
// action_id and job_tag_id all NULL: calls pg_stat_statement_context didn't
// attribute, or every call from a database without it. The reports show them
// as their own context.

// addUntaggedContext adds an untagged context row with c calls to the
// fixture's recent event for source and fingerprint, startAgo before Anchor.
func addUntaggedContext(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, source, fingerprint string, startAgo time.Duration, c int) {
	t.Helper()
	start := fixture.Anchor.Add(-startAgo)
	tag, err := conn.Exec(context.Background(), `insert into rotten.event_context
		(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c, logical_source_id, attributed_time)
		select e.id, e.observed_window_start, e.observed_window_end, null, null, null, $4, e.logical_source_id, 1
		from rotten.events e
		where e.logical_source_id = $1 and e.fingerprint_id = $2 and e.observed_window_start = $3`,
		fixture.SourceIDs[source], fixture.FingerprintID[fingerprint], start, c)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("added untagged context to %d events, want 1", tag.RowsAffected())
	}
}

func hasUntagged(contexts []topByCallsContext, times float64) bool {
	for _, c := range contexts {
		if c.Controller == nil && c.Action == nil && c.JobTag == nil && c.Times == times {
			return true
		}
	}
	return false
}

func TestTopReportsShowUntaggedContexts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	// More than any other context of jobs, so it's in the top five.
	addUntaggedContext(t, conn, fixture, "canvas13p", "jobs", 60*time.Minute, 500)

	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		t.Run(report, func(t *testing.T) {
			query, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := conn.Query(context.Background(), string(query), "canvas", testdb.ReportEnvironment, "13",
				fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			found := false
			for rows.Next() {
				var (
					id          int64
					f1, f2, f3  float64
					example     string
					contextJSON []byte
				)
				if err := rows.Scan(&id, &f1, &f2, &f3, &example, &contextJSON, new(bool)); err != nil {
					t.Fatal(err)
				}
				if id != fixture.FingerprintID["jobs"] {
					continue
				}
				var contexts []topByCallsContext
				if err := json.Unmarshal(contextJSON, &contexts); err != nil {
					t.Fatal(err)
				}
				if !hasUntagged(contexts, 500) {
					t.Errorf("jobs contexts = %s, want an untagged context with 500 calls", contextJSON)
				}
				found = true
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("jobs not in the report")
			}
		})
	}
}

func TestOutliersShowUntaggedContexts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	addUntaggedContext(t, conn, fixture, "canvas7p", "slow", 40*time.Minute, 7)

	rows := readFixtureOutliers(t, conn, fixture, "7", 10)
	for _, row := range rows {
		if row.FingerprintID != fixture.FingerprintID["slow"] {
			continue
		}
		var contexts []topByCallsContext
		if err := json.Unmarshal(row.ContextJSON, &contexts); err != nil {
			t.Fatal(err)
		}
		if !hasUntagged(contexts, 7) {
			t.Fatalf("slow contexts = %s, want an untagged context with 7 calls", row.ContextJSON)
		}
		return
	}
	t.Fatalf("slow not an outlier: %+v", rows)
}

// A match pattern never matches the untagged context, even "untagged": it has
// no controller, action or job tag to match. Its query text still can.
func TestTopReportsMatchIgnoresUntaggedContexts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	addUntaggedContext(t, conn, fixture, "canvas13p", "jobs", 60*time.Minute, 500)

	for _, report := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		if got := topFingerprints(t, conn, fixture, report, 10, "untagged"); len(got) != 0 {
			t.Errorf("%s match untagged = %v, want none", report, got)
		}
	}

	addUntaggedContext(t, conn, fixture, "canvas7p", "slow", 40*time.Minute, 7)
	if got := readOutliers(t, conn, "canvas", testdb.ReportEnvironment, "7",
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10,
		defaultSigma, defaultMinHistory, defaultRatio, nil, "untagged"); len(got) != 0 {
		t.Errorf("outliers match untagged = %+v, want none", got)
	}
	// The same call without a match does list slow, so the empty result is the
	// match's doing.
	found := false
	for _, row := range readFixtureOutliers(t, conn, fixture, "7", 10) {
		found = found || row.FingerprintID == fixture.FingerprintID["slow"]
	}
	if !found {
		t.Error("outliers without a match don't list slow")
	}
}

func TestReplicaUtilizationShowsUntaggedRow(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	at := fixture.Anchor.Add(-15 * time.Minute)
	insertUtilizationEvent(t, conn, fixture, "canvas13p", at, 40, 400, []utilizationContext{{Count: 40}})
	insertUtilizationEvent(t, conn, fixture, "canvas13r", at, 10, 100, []utilizationContext{{Count: 10}})
	// Job-only calls are not untagged; they stay out of the controller report.
	insertUtilizationEvent(t, conn, fixture, "canvas13p", at, 5, 50, []utilizationContext{{JobTag: "JobOnly", Count: 5}})

	for _, report := range []string{"replica_utilization_by_controller_action.sql", "replica_utilization_by_job.sql"} {
		t.Run(report, func(t *testing.T) {
			args := []any{"canvas", testdb.ReportEnvironment, "13", fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
				testdb.ReportPrimaryRole, testdb.ReportReplicaRole}
			got := readReplicaUtilization(t, conn, report, append(args, nil)...)
			row := findUtilizationRow(t, got, untaggedName)
			want := replicaUtilizationRow{untaggedName, "13", 40, 10, 50, 80, 20, 400, 100, 500, 80, 20}
			if row != want {
				t.Fatalf("untagged row = %+v, want %+v", row, want)
			}
			if report == "replica_utilization_by_controller_action.sql" {
				for _, r := range got {
					if r.Name == "#" {
						t.Fatalf("job-only calls showed as a controller row: %+v", got)
					}
				}
			}
			for _, match := range []string{"untagged", ".*"} {
				for _, r := range readReplicaUtilization(t, conn, report, append(args, match)...) {
					if r.Name == untaggedName {
						t.Fatalf("match %q kept the untagged row", match)
					}
				}
			}
		})
	}
}
