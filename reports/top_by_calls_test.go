package reports_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/harvestlimits"
	"github.com/benchub/rotten/internal/testdb"
)

type topByCallsContext struct {
	Times      float64 `json:"times"`
	Controller *string `json:"controller"`
	Action     *string `json:"action"`
	JobTag     *string `json:"job_tag"`
}

func TestTopByCalls(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	query, err := os.ReadFile("top_by_calls.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	fpByKey := map[string]testdb.SeedFingerprint{}
	for _, fp := range testdb.ReportFingerprints {
		fpByKey[fp.Key] = fp
	}
	wantOrder := []string{"users", "courses", "jobs"}
	totals := testdb.RecentTotals()

	i := 0
	for rows.Next() {
		if i >= len(wantOrder) {
			t.Fatalf("got more rows than expected")
		}
		key := wantOrder[i]
		wantGroup := testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: key}
		wantTotals := totals[wantGroup]

		var (
			fingerprintID int64
			calls         float64
			totalMS       float64
			avgMSPerCall  float64
			example       string
			contextJSON   []byte
		)
		if err := rows.Scan(&fingerprintID, &calls, &totalMS, &avgMSPerCall, &example, &contextJSON); err != nil {
			t.Fatal(err)
		}

		if fingerprintID != fixture.FingerprintID[key] {
			t.Errorf("row %d fingerprint id = %d, want %d", i, fingerprintID, fixture.FingerprintID[key])
		}
		if calls != wantTotals.Calls {
			t.Errorf("row %d calls = %v, want %v", i, calls, wantTotals.Calls)
		}
		if totalMS != wantTotals.Time {
			t.Errorf("row %d total_ms = %v, want %v", i, totalMS, wantTotals.Time)
		}
		if wantAvg := wantTotals.Time / wantTotals.Calls; math.Abs(avgMSPerCall-wantAvg) > 0.000001 {
			t.Errorf("row %d avg_ms_per_call = %v, want %v", i, avgMSPerCall, wantAvg)
		}
		if example != fpByKey[key].Normalized {
			t.Errorf("row %d example = %q, want %q", i, example, fpByKey[key].Normalized)
		}

		var contexts []topByCallsContext
		if err := json.Unmarshal(contextJSON, &contexts); err != nil {
			t.Fatalf("row %d contexts json %s: %v", i, contextJSON, err)
		}
		if got, want := fromReportContexts(contexts), testdb.RecentTopContexts(wantGroup, 5); !reflect.DeepEqual(got, want) {
			t.Errorf("row %d contexts = %+v, want %+v", i, got, want)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(wantOrder) {
		t.Fatalf("got %d rows, want %d", i, len(wantOrder))
	}
}

func TestTopByCallsHonorsLimit(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	query, err := os.ReadFile("top_by_calls.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []int64
	for rows.Next() {
		var (
			fingerprintID int64
			calls         float64
			totalMS       float64
			avgMSPerCall  float64
			example       string
			contextJSON   []byte
		)
		if err := rows.Scan(&fingerprintID, &calls, &totalMS, &avgMSPerCall, &example, &contextJSON); err != nil {
			t.Fatal(err)
		}
		got = append(got, fingerprintID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int64{fixture.FingerprintID["users"], fixture.FingerprintID["courses"]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fingerprints = %v, want %v", got, want)
	}
	for _, id := range got {
		if id == fixture.FingerprintID["jobs"] {
			t.Fatalf("limit 2 returned jobs fingerprint %d", id)
		}
	}
}

func TestTopByCallsContextCountsAreBigint(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	var controllerID int
	if err := conn.QueryRow(ctx, "insert into rotten.controllers (controller) values ($1) returning id", "overflow").Scan(&controllerID); err != nil {
		t.Fatal(err)
	}
	var actionID int
	if err := conn.QueryRow(ctx, "insert into rotten.actions (action) values ($1) returning id", "big").Scan(&actionID); err != nil {
		t.Fatal(err)
	}
	const bigCount = int64(1)<<32 + 5
	for _, eventIndex := range []int{0, 1} {
		start := fixture.WindowStart(eventIndex)
		if _, err := conn.Exec(ctx, `insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, c)
			values ($1, $2, $3, $4, $5, $6::bigint)`,
			fixture.EventIDs[eventIndex], start, start.Add(testdb.WindowLength), controllerID, actionID, bigCount); err != nil {
			t.Fatal(err)
		}
	}

	query, err := os.ReadFile("top_by_calls.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("got no rows")
	}
	var (
		fingerprintID int64
		calls         float64
		totalMS       float64
		avgMSPerCall  float64
		example       string
		contextJSON   []byte
	)
	if err := rows.Scan(&fingerprintID, &calls, &totalMS, &avgMSPerCall, &example, &contextJSON); err != nil {
		t.Fatal(err)
	}
	if fingerprintID != fixture.FingerprintID["users"] {
		t.Fatalf("fingerprint id = %d, want users %d", fingerprintID, fixture.FingerprintID["users"])
	}
	var contexts []topByCallsContext
	if err := json.Unmarshal(contextJSON, &contexts); err != nil {
		t.Fatal(err)
	}
	if len(contexts) == 0 {
		t.Fatal("got no contexts")
	}
	got := contexts[0]
	if stringValue(got.Controller) != "overflow" || stringValue(got.Action) != "big" || got.Times != float64(2*bigCount) {
		t.Fatalf("top context = %+v, want overflow big %d", got, 2*bigCount)
	}
}

func TestReportsHandleContextSumsAboveBigint(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	var controllerID int
	if err := conn.QueryRow(ctx, "insert into rotten.controllers (controller) values ($1) returning id", "max").Scan(&controllerID); err != nil {
		t.Fatal(err)
	}
	var actionID int
	if err := conn.QueryRow(ctx, "insert into rotten.actions (action) values ($1) returning id", "count").Scan(&actionID); err != nil {
		t.Fatal(err)
	}
	var jobTagID int
	if err := conn.QueryRow(ctx, "insert into rotten.job_tags (job_tag) values ($1) returning id", "MaxCountJob").Scan(&jobTagID); err != nil {
		t.Fatal(err)
	}
	maxCount := int64(harvestlimits.MaxContextCount)
	const largeRows = 1100
	if _, err := conn.Exec(ctx, `
		with inserted_events as (
			insert into rotten.events
				(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			select $1, $2, $3, $4::timestamptz, $5::timestamptz, $6::double precision, $7::double precision
			from generate_series(1, $8)
			returning id, observed_window_start, observed_window_end
		)
		insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
		select id, observed_window_start, observed_window_end, $9, $10, $11, $6::bigint
		from inserted_events`,
		fixture.FingerprintID["users"],
		fixture.SourceIDs["canvas13p"],
		fixture.PhysicalIDs["canvas13p"],
		fixture.Anchor.Add(-15*time.Minute),
		fixture.Anchor.Add(-15*time.Minute).Add(testdb.WindowLength),
		maxCount,
		float64(maxCount)*100,
		largeRows,
		controllerID,
		actionID,
		jobTagID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
		with inserted_events as (
			insert into rotten.events
				(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			select $1, $2, $3, $4::timestamptz, $5::timestamptz, $6::double precision, $7::double precision
			from generate_series(1, $8)
			returning id, observed_window_start, observed_window_end
		)
		insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
		select id, observed_window_start, observed_window_end, $9, $10, null, $6::bigint
		from inserted_events`,
		fixture.FingerprintID["slow"],
		fixture.SourceIDs["canvas7p"],
		fixture.PhysicalIDs["canvas7p"],
		fixture.Anchor.Add(-15*time.Minute),
		fixture.Anchor.Add(-15*time.Minute).Add(testdb.WindowLength),
		maxCount,
		float64(maxCount)*100,
		largeRows,
		controllerID,
		actionID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `update rotten.fingerprint_stats
		set count = $1, mean = 1, deviation = 1
		where fingerprint_id = $2 and type = 'mean_time' and logical_source_id in (0, $3)`,
		int64(largeRows+1000), fixture.FingerprintID["slow"], fixture.SourceIDs["canvas7p"]); err != nil {
		t.Fatal(err)
	}
	want := float64(largeRows) * float64(maxCount)

	query, err := os.ReadFile("top_by_calls.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		1,
	)
	if err != nil {
		t.Fatalf("top_by_calls with large context sum: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("got no rows")
	}
	var (
		fingerprintID int64
		calls         float64
		totalMS       float64
		avgMSPerCall  float64
		example       string
		contextJSON   []byte
	)
	if err := rows.Scan(&fingerprintID, &calls, &totalMS, &avgMSPerCall, &example, &contextJSON); err != nil {
		t.Fatal(err)
	}
	var contexts []topByCallsContext
	if err := json.Unmarshal(contextJSON, &contexts); err != nil {
		t.Fatal(err)
	}
	if len(contexts) == 0 {
		t.Fatal("got no contexts")
	}
	got := contexts[0]
	if stringValue(got.Controller) != "max" || stringValue(got.Action) != "count" || got.Times != want {
		t.Fatalf("top_by_calls context = %+v, want max count %.0f", got, want)
	}
	rows.Close()

	for _, report := range []string{"top_by_total_time.sql"} {
		query, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		args := []any{"canvas", testdb.ReportEnvironment, "13", fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 1}
		reportRows, err := conn.Query(ctx, string(query), args...)
		if err != nil {
			t.Fatalf("%s with large context sum: %v", report, err)
		}
		if !reportRows.Next() {
			if err := reportRows.Err(); err != nil {
				reportRows.Close()
				t.Fatal(err)
			}
			reportRows.Close()
			t.Fatalf("%s got no rows", report)
		}
		var (
			fingerprintID int64
			calls         float64
			totalMS       float64
			avgMSPerCall  float64
			example       string
			contextJSON   []byte
		)
		if err := reportRows.Scan(&fingerprintID, &calls, &totalMS, &avgMSPerCall, &example, &contextJSON); err != nil {
			reportRows.Close()
			t.Fatal(err)
		}
		var contexts []topByCallsContext
		if err := json.Unmarshal(contextJSON, &contexts); err != nil {
			reportRows.Close()
			t.Fatal(err)
		}
		if len(contexts) == 0 || contexts[0].Times != want {
			reportRows.Close()
			t.Fatalf("%s contexts = %+v, want top count %.0f", report, contexts, want)
		}
		reportRows.Close()
	}

	outliers := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		1,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
	)
	if len(outliers) == 0 {
		t.Fatal("outliers got no rows")
	}
	var outlierContexts []topByCallsContext
	if err := json.Unmarshal(outliers[0].ContextJSON, &outlierContexts); err != nil {
		t.Fatal(err)
	}
	if len(outlierContexts) == 0 || outlierContexts[0].Times != want {
		t.Fatalf("outliers contexts = %+v, want top count %.0f", outlierContexts, want)
	}

	for _, report := range []string{"replica_utilization_by_controller_action.sql", "replica_utilization_by_job.sql"} {
		got := readReplicaUtilization(t, conn, report,
			"canvas",
			testdb.ReportEnvironment,
			"13",
			fixture.Anchor.Add(-testdb.RecentRange),
			fixture.Anchor,
			testdb.ReportPrimaryRole,
			testdb.ReportReplicaRole,
		)
		if len(got) == 0 {
			t.Fatalf("%s got no rows", report)
		}
		row := got[0]
		if row.PrimaryCalls != want || row.TotalCalls != want {
			t.Fatalf("%s calls = %.0f/%.0f, want %.0f; row %+v", report, row.PrimaryCalls, row.TotalCalls, want, row)
		}
	}
}

func fromReportContexts(in []topByCallsContext) []testdb.SeedContext {
	out := make([]testdb.SeedContext, 0, len(in))
	for _, c := range in {
		out = append(out, testdb.SeedContext{
			Controller: stringValue(c.Controller),
			Action:     stringValue(c.Action),
			JobTag:     stringValue(c.JobTag),
			C:          int(c.Times),
		})
	}
	return out
}

func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
