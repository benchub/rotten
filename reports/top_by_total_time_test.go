package reports_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

func TestTopByTotalTime(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	query, err := os.ReadFile("top_by_total_time.sql")
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
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	fpByKey := map[string]testdb.SeedFingerprint{}
	for _, fp := range testdb.ReportFingerprints {
		fpByKey[fp.Key] = fp
	}
	totals := testdb.RecentTotals()
	fingerprintKeys := []string{"users", "courses", "jobs"}
	timeOrder := append([]string(nil), fingerprintKeys...)
	sort.Slice(timeOrder, func(i, j int) bool {
		a, b := timeOrder[i], timeOrder[j]
		aTotals := totals[testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: a}]
		bTotals := totals[testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: b}]
		if aTotals.Time != bTotals.Time {
			return aTotals.Time > bTotals.Time
		}
		if aTotals.Calls != bTotals.Calls {
			return aTotals.Calls > bTotals.Calls
		}
		return fixture.FingerprintID[a] < fixture.FingerprintID[b]
	})
	callOrder := append([]string(nil), fingerprintKeys...)
	sort.Slice(callOrder, func(i, j int) bool {
		a, b := callOrder[i], callOrder[j]
		aTotals := totals[testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: a}]
		bTotals := totals[testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: b}]
		if aTotals.Calls != bTotals.Calls {
			return aTotals.Calls > bTotals.Calls
		}
		if aTotals.Time != bTotals.Time {
			return aTotals.Time > bTotals.Time
		}
		return fixture.FingerprintID[a] < fixture.FingerprintID[b]
	})
	if reflect.DeepEqual(timeOrder, callOrder) {
		t.Fatal("fixture does not distinguish total-time order from call-count order")
	}
	wantOrder := []string{"jobs", "users", "courses"}
	if !reflect.DeepEqual(wantOrder, timeOrder) {
		t.Fatalf("expected order fixture = %v, want %v", timeOrder, wantOrder)
	}

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

func TestTopByTotalTimeHonorsLimit(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	query, err := os.ReadFile("top_by_total_time.sql")
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
		nil,
		nil,
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
	want := []int64{fixture.FingerprintID["jobs"], fixture.FingerprintID["users"]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fingerprints = %v, want %v", got, want)
	}
	for _, id := range got {
		if id == fixture.FingerprintID["courses"] {
			t.Fatalf("limit 2 returned courses fingerprint %d", id)
		}
	}
}
