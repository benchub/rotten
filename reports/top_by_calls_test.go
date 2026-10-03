package reports_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

type topByCallsContext struct {
	Times      int64   `json:"times"`
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
	const nearIntMax = int64(2_000_000_000)
	for _, eventIndex := range []int{0, 1} {
		start := fixture.WindowStart(eventIndex)
		if _, err := conn.Exec(ctx, `insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, c)
			values ($1, $2, $3, $4, $5, $6::integer)`,
			fixture.EventIDs[eventIndex], start, start.Add(testdb.WindowLength), controllerID, actionID, nearIntMax); err != nil {
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
	if stringValue(got.Controller) != "overflow" || stringValue(got.Action) != "big" || got.Times != 2*nearIntMax {
		t.Fatalf("top context = %+v, want overflow big %d", got, 2*nearIntMax)
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
