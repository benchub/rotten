package reports_test

import (
	"context"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

type replicaUtilizationRow struct {
	Name               string
	Cluster            string
	PrimaryCalls       int64
	ReplicaCalls       int64
	TotalCalls         int64
	PrimaryCallPercent float64
	ReplicaCallPercent float64
	PrimaryTotalMS     float64
	ReplicaTotalMS     float64
	TotalMS            float64
	PrimaryTimePercent float64
	ReplicaTimePercent float64
}

func readReplicaUtilization(t *testing.T, conn *pgx.Conn, path string, args ...any) []replicaUtilizationRow {
	t.Helper()
	query, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []replicaUtilizationRow
	for rows.Next() {
		var r replicaUtilizationRow
		if err := rows.Scan(
			&r.Name,
			&r.Cluster,
			&r.PrimaryCalls,
			&r.ReplicaCalls,
			&r.TotalCalls,
			&r.PrimaryCallPercent,
			&r.ReplicaCallPercent,
			&r.PrimaryTotalMS,
			&r.ReplicaTotalMS,
			&r.TotalMS,
			&r.PrimaryTimePercent,
			&r.ReplicaTimePercent,
		); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReplicaUtilizationByJob(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readReplicaUtilization(t, conn, "replica_utilization_by_job.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		testdb.ReportPrimaryRole,
		testdb.ReportReplicaRole,
	)

	want := []replicaUtilizationRow{
		{"Reindex", "13", 30, 10, 40, 75, 25, 1000, 900, 1900, 52.63, 47.37},
		{"SendEmail", "13", 30, 0, 30, 100, 0, 3000, 0, 3000, 100, 0},
		{"ReplicaReport", "13", 0, 15, 15, 0, 100, 0, 300, 300, 0, 100},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replica utilization by job = %+v, want %+v", got, want)
	}
}

func TestReplicaUtilizationByControllerAction(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readReplicaUtilization(t, conn, "replica_utilization_by_controller_action.sql",
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		testdb.ReportPrimaryRole,
		testdb.ReportReplicaRole,
	)

	want := []replicaUtilizationRow{
		{"users#show", "7", 60, 140, 200, 30, 70, 30, 70, 100, 30, 70},
		{"submissions#index", "7", 20, 0, 20, 100, 0, 800, 0, 800, 100, 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replica utilization by controller/action = %+v, want %+v", got, want)
	}
}

func TestReplicaUtilizationByControllerActionSplitsEventTimeByContextCounts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readReplicaUtilization(t, conn, "replica_utilization_by_controller_action.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		testdb.ReportPrimaryRole,
		testdb.ReportReplicaRole,
	)

	byName := map[string]replicaUtilizationRow{}
	for _, row := range got {
		byName[row.Name] = row
	}

	usersShow := byName["users#show"]
	if usersShow.PrimaryCalls != 600 || usersShow.ReplicaCalls != 0 || usersShow.TotalCalls != 600 {
		t.Fatalf("users#show calls = %d/%d/%d, want 600/0/600; all rows %+v", usersShow.PrimaryCalls, usersShow.ReplicaCalls, usersShow.TotalCalls, got)
	}
	assertFloat(t, usersShow.PrimaryTotalMS, 250.0*200.0/491.0+50.0+150.0, "users#show primary time")
	assertFloat(t, usersShow.PrimaryTimePercent, 100, "users#show primary time percent")

	loginNew := byName["login#new"]
	if loginNew.PrimaryCalls != 1 || loginNew.ReplicaCalls != 0 || loginNew.TotalCalls != 1 {
		t.Fatalf("login#new calls = %d/%d/%d, want 1/0/1; all rows %+v", loginNew.PrimaryCalls, loginNew.ReplicaCalls, loginNew.TotalCalls, got)
	}
	assertFloat(t, loginNew.PrimaryTotalMS, 250.0/491.0, "login#new primary time")
}

func TestReplicaUtilizationByJobDoesNotDoubleCountRepeatedJobContexts(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	insertUtilizationEvent(t, conn, fixture, "canvas13p", fixture.Anchor.Add(-15*time.Minute), 100, 1000, []utilizationContext{
		{Controller: "jobs", Action: "first", JobTag: "Fanout", Count: 20},
		{Controller: "jobs", Action: "second", JobTag: "Fanout", Count: 30},
	})

	got := readReplicaUtilization(t, conn, "replica_utilization_by_job.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		testdb.ReportPrimaryRole,
		testdb.ReportReplicaRole,
	)
	row := findUtilizationRow(t, got, "Fanout")
	if row.PrimaryCalls != 50 || row.ReplicaCalls != 0 || row.TotalCalls != 50 {
		t.Fatalf("Fanout calls = %d/%d/%d, want 50/0/50; all rows %+v", row.PrimaryCalls, row.ReplicaCalls, row.TotalCalls, got)
	}
	assertFloat(t, row.PrimaryTotalMS, 1000, "Fanout primary time")
}

func TestReplicaUtilizationPercentagesSumToOneHundredAfterRounding(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	insertUtilizationEvent(t, conn, fixture, "canvas13p", fixture.Anchor.Add(-15*time.Minute), 1, 1, []utilizationContext{
		{JobTag: "RoundingCase", Count: 1},
	})
	insertUtilizationEvent(t, conn, fixture, "canvas13r", fixture.Anchor.Add(-15*time.Minute), 799, 799, []utilizationContext{
		{JobTag: "RoundingCase", Count: 799},
	})

	got := readReplicaUtilization(t, conn, "replica_utilization_by_job.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		testdb.ReportPrimaryRole,
		testdb.ReportReplicaRole,
	)
	row := findUtilizationRow(t, got, "RoundingCase")
	if row.PrimaryCallPercent != 0.13 || row.ReplicaCallPercent != 99.87 {
		t.Fatalf("call percentages = %v/%v, want 0.13/99.87", row.PrimaryCallPercent, row.ReplicaCallPercent)
	}
	if row.PrimaryTimePercent != 0.13 || row.ReplicaTimePercent != 99.87 {
		t.Fatalf("time percentages = %v/%v, want 0.13/99.87", row.PrimaryTimePercent, row.ReplicaTimePercent)
	}
}

func TestReplicaUtilizationPrunesEventContextPartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start := fixture.Anchor.Add(-testdb.RecentRange)
	end := fixture.Anchor
	expectedPartitions := partitionsOverlappingRange(t, conn, "rotten.event_context", start, end)

	for _, report := range []string{
		"replica_utilization_by_job.sql",
		"replica_utilization_by_controller_action.sql",
	} {
		t.Run(report, func(t *testing.T) {
			plan := explainReport(t, conn, report,
				"canvas",
				testdb.ReportEnvironment,
				"13",
				start,
				end,
				testdb.ReportPrimaryRole,
				testdb.ReportReplicaRole,
			)
			assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.event_context", expectedPartitions)
		})
	}
}

func TestReplicaUtilizationPrunesFixedMidnightCrossingRange(t *testing.T) {
	db := testdb.StartRotten(t)
	testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	expectedPartitions := partitionsOverlappingRange(t, conn, "rotten.event_context", start, end)
	if len(expectedPartitions) != 2 {
		t.Fatalf("fixed range touches %d partitions, want 2", len(expectedPartitions))
	}

	for _, report := range []string{
		"replica_utilization_by_job.sql",
		"replica_utilization_by_controller_action.sql",
	} {
		t.Run(report, func(t *testing.T) {
			plan := explainReport(t, conn, report,
				"canvas",
				testdb.ReportEnvironment,
				"13",
				start,
				end,
				testdb.ReportPrimaryRole,
				testdb.ReportReplicaRole,
			)
			assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.event_context", expectedPartitions)
		})
	}
}

type utilizationContext struct {
	Controller string
	Action     string
	JobTag     string
	Count      int
}

func insertUtilizationEvent(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, sourceKey string, start time.Time, calls float64, totalMS float64, contexts []utilizationContext) {
	t.Helper()
	ctx := context.Background()
	var eventID int64
	if err := conn.QueryRow(ctx, `insert into rotten.events
		(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
		values ($1,$2,$3,$4,$5,$6,$7) returning id`,
		fixture.FingerprintID["jobs"], fixture.SourceIDs[sourceKey], fixture.PhysicalIDs[sourceKey], start, start.Add(testdb.WindowLength), calls, totalMS).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	for _, c := range contexts {
		if _, err := conn.Exec(ctx, `insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
			values ($1,$2,$3,$4,$5,$6,$7)`,
			eventID, start, start.Add(testdb.WindowLength),
			ensureControllerID(t, conn, c.Controller),
			ensureActionID(t, conn, c.Action),
			ensureJobTagID(t, conn, c.JobTag),
			c.Count); err != nil {
			t.Fatal(err)
		}
	}
}

func ensureControllerID(t *testing.T, conn *pgx.Conn, controller string) *int {
	t.Helper()
	if controller == "" {
		return nil
	}
	var id int
	if err := conn.QueryRow(context.Background(), `insert into rotten.controllers (controller)
		values ($1) on conflict (controller) do update set controller = excluded.controller returning id`, controller).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return &id
}

func ensureActionID(t *testing.T, conn *pgx.Conn, action string) *int {
	t.Helper()
	if action == "" {
		return nil
	}
	var id int
	if err := conn.QueryRow(context.Background(), `insert into rotten.actions (action)
		values ($1) on conflict (action) do update set action = excluded.action returning id`, action).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return &id
}

func ensureJobTagID(t *testing.T, conn *pgx.Conn, jobTag string) *int {
	t.Helper()
	if jobTag == "" {
		return nil
	}
	var id int
	if err := conn.QueryRow(context.Background(), `insert into rotten.job_tags (job_tag)
		values ($1) on conflict (job_tag) do update set job_tag = excluded.job_tag returning id`, jobTag).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return &id
}

func findUtilizationRow(t *testing.T, rows []replicaUtilizationRow, name string) replicaUtilizationRow {
	t.Helper()
	for _, row := range rows {
		if row.Name == name {
			return row
		}
	}
	t.Fatalf("row %q not found in %+v", name, rows)
	return replicaUtilizationRow{}
}

func assertFloat(t *testing.T, got float64, want float64, label string) {
	t.Helper()
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func explainReport(t *testing.T, conn *pgx.Conn, path string, args ...any) string {
	t.Helper()
	query, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), "explain "+string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}
