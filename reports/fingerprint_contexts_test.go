package reports_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func readFingerprintContexts(t *testing.T, conn *pgx.Conn, args ...any) []testdb.SeedContext {
	t.Helper()
	query, err := os.ReadFile("fingerprint_contexts.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []testdb.SeedContext
	for rows.Next() {
		var (
			controller, action, jobTag *string
			times                      float64
		)
		if err := rows.Scan(&controller, &action, &jobTag, &times); err != nil {
			t.Fatal(err)
		}
		out = append(out, testdb.SeedContext{
			Controller: stringValue(controller),
			Action:     stringValue(action),
			JobTag:     stringValue(jobTag),
			C:          int(times),
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFingerprintContextsAcrossRoles(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintContexts(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		5, nil,
	)

	want := testdb.RecentTopContexts(testdb.GroupKey{Project: "canvas", Cluster: "13", Fingerprint: "users"}, 5)
	if len(want) != 5 {
		t.Fatalf("fixture gives %d contexts, want 5 so the limit is exercised", len(want))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("contexts = %+v, want %+v", got, want)
	}
}

func TestFingerprintContextsAllWithinLimit(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintContexts(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		50, nil,
	)

	want := []testdb.SeedContext{
		{Controller: "users", Action: "show", C: 600},
		{Controller: "grades", Action: "show", C: 200},
		{Controller: "users", Action: "index", C: 120},
		{Controller: "courses", Action: "show", C: 80},
		{Controller: "grades", Action: "index", C: 50},
		{Controller: "api", Action: "list", C: 40},
		{Controller: "login", Action: "new", C: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("contexts = %+v, want %+v", got, want)
	}
}

func TestFingerprintContextsFilterByRoleAndFingerprint(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintContexts(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		50, testdb.ReportReplicaRole,
	)
	want := []testdb.SeedContext{{Controller: "grades", Action: "show", C: 200}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replica contexts = %+v, want %+v", got, want)
	}

	got = readFingerprintContexts(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["jobs"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		50, nil,
	)
	want = []testdb.SeedContext{{JobTag: "Reindex", C: 40}, {JobTag: "SendEmail", C: 30}, {JobTag: "ReplicaReport", C: 15}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("job contexts = %+v, want %+v", got, want)
	}
}

func TestFingerprintContextsExcludeWindowsOutsideRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// Only the canvas13p window 90 minutes back fits. The window 4 hours back
	// starts before the range, the one 50 minutes back straddles its end,
	// and the replica's 45 minutes back starts at its end.
	got := readFingerprintContexts(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-4*time.Hour+time.Minute), fixture.Anchor.Add(-45*time.Minute),
		50, nil,
	)
	want := []testdb.SeedContext{{Controller: "users", Action: "show", C: 300}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("contexts = %+v, want %+v", got, want)
	}
}

func TestFingerprintContextsPrunePartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	plan := explainReport(t, conn, "fingerprint_contexts.sql",
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		start, end,
		50, nil,
	)
	for _, parent := range []string{"rotten.events", "rotten.event_context"} {
		expected := partitionsOverlappingRange(t, conn, parent, start, end)
		if len(expected) != 2 {
			t.Fatalf("fixed range touches %d %s partitions, want 2", len(expected), parent)
		}
		assertPlanTouchesOnlyPartitions(t, conn, plan, parent, expected)
	}
}
