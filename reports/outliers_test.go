package reports_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

const (
	defaultSigma      = 3.0
	defaultMinHistory = 30
	defaultRatio      = 2.0
)

type outlierRow struct {
	LogicalSourceID      int
	FingerprintID        int64
	Project              string
	Environment          string
	Cluster              string
	Role                 string
	Calls                float64
	TotalMS              float64
	AvgMSPerCall         float64
	GlobalMeanMS         float64
	GlobalDeviationMS    float64
	SourceMeanMS         float64
	SourceDeviationMS    float64
	BaselineMeanMS       float64
	BaselineDeviationMS  float64
	DeviationsOverSource float64
	Example              string
	ContextJSON          []byte
}

func readOutliers(t *testing.T, conn *pgx.Conn, args ...any) []outlierRow {
	t.Helper()
	ctx := context.Background()
	query, err := os.ReadFile("outliers.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []outlierRow
	for rows.Next() {
		var r outlierRow
		if err := rows.Scan(
			&r.LogicalSourceID,
			&r.FingerprintID,
			&r.Project,
			&r.Environment,
			&r.Cluster,
			&r.Role,
			&r.Calls,
			&r.TotalMS,
			&r.AvgMSPerCall,
			&r.GlobalMeanMS,
			&r.GlobalDeviationMS,
			&r.SourceMeanMS,
			&r.SourceDeviationMS,
			&r.BaselineMeanMS,
			&r.BaselineDeviationMS,
			&r.DeviationsOverSource,
			&r.Example,
			&r.ContextJSON,
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

func TestOutliersUsesSourceHistoryAndReturnsGlobalHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	unrelatedID := insertOutlierFingerprint(t, conn, "unrelated_global_noise", "select unrelated global noise")
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas13p"], fixture.PhysicalIDs["canvas13p"], unrelatedID, fixture.Anchor.Add(-20*time.Minute), 1, 1_000_000)

	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		10,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	if len(got) != 1 {
		t.Fatalf("got %d outliers, want exactly planted slow query: %+v", len(got), got)
	}
	row := got[0]
	if row.LogicalSourceID != fixture.SourceIDs["canvas7p"] {
		t.Errorf("logical_source_id = %d, want canvas7p %d", row.LogicalSourceID, fixture.SourceIDs["canvas7p"])
	}
	if row.FingerprintID != fixture.FingerprintID["slow"] {
		t.Errorf("fingerprint_id = %d, want slow %d", row.FingerprintID, fixture.FingerprintID["slow"])
	}
	if row.Project != "canvas" || row.Environment != testdb.ReportEnvironment || row.Cluster != "7" || row.Role != testdb.ReportPrimaryRole {
		t.Errorf("source = %s/%s/%s/%s, want canvas/%s/7/%s", row.Project, row.Environment, row.Cluster, row.Role, testdb.ReportEnvironment, testdb.ReportPrimaryRole)
	}
	if row.Calls != 20 || row.TotalMS != 800 || row.AvgMSPerCall != 40 {
		t.Errorf("calls/total/avg = %v/%v/%v, want 20/800/40", row.Calls, row.TotalMS, row.AvgMSPerCall)
	}
	if math.Abs(row.GlobalMeanMS-5) > 0.000001 || math.Abs(row.GlobalDeviationMS-1) > 0.000001 {
		t.Errorf("global mean/deviation = %v/%v, want 5/1", row.GlobalMeanMS, row.GlobalDeviationMS)
	}
	if math.Abs(row.SourceMeanMS-8) > 0.000001 || math.Abs(row.SourceDeviationMS-0.9) > 0.000001 {
		t.Errorf("source mean/deviation = %v/%v, want 8/0.9", row.SourceMeanMS, row.SourceDeviationMS)
	}
	if math.Abs(row.BaselineMeanMS-8) > 0.000001 || math.Abs(row.BaselineDeviationMS-0.9) > 0.000001 {
		t.Errorf("baseline mean/deviation = %v/%v, want source-owned 8/0.9", row.BaselineMeanMS, row.BaselineDeviationMS)
	}
	if want := (40.0 - 8.0) / 0.9; math.Abs(row.DeviationsOverSource-want) > 0.000001 {
		t.Errorf("deviations_over_source = %v, want %v", row.DeviationsOverSource, want)
	}

	var contexts []topByCallsContext
	if err := json.Unmarshal(row.ContextJSON, &contexts); err != nil {
		t.Fatalf("contexts json %s: %v", row.ContextJSON, err)
	}
	wantContexts := []testdb.SeedContext{{Controller: "submissions", Action: "index", C: 20}}
	if gotContexts := fromReportContexts(contexts); !reflect.DeepEqual(gotContexts, wantContexts) {
		t.Errorf("contexts = %+v, want %+v", gotContexts, wantContexts)
	}
}

func TestOutliersSkipsZeroDeviationAndMissingSourceHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	count, mean, deviation := mergedStats(30, 0.4, 0, 0.5)
	if _, err := conn.Exec(ctx, `insert into rotten.fingerprint_stats
		(fingerprint_id, logical_source_id, type, count, mean, deviation, last)
		values ($1,$2,'mean_time',$3,$4,$5,0)`,
		fixture.FingerprintID["users"], fixture.SourceIDs["canvas7p"], count, mean, deviation); err != nil {
		t.Fatal(err)
	}

	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		10,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	if len(got) != 1 || got[0].FingerprintID != fixture.FingerprintID["slow"] {
		t.Fatalf("outliers = %+v, want only slow; users with zero deviation and users on replica with no source history must be excluded", got)
	}
}

func TestOutliersSubtractsInRangeSamplesFromStoredHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	fingerprintID := insertOutlierFingerprint(t, conn, "realistic_slow", "select realistic slow")
	var samples []float64
	for i := 0; i < 10; i++ {
		start := fixture.Anchor.Add(-time.Duration(i+1) * testdb.WindowLength)
		insertOutlierEvent(t, conn, fixture.SourceIDs["canvas7p"], fixture.PhysicalIDs["canvas7p"], fingerprintID, start, 10, 400)
		samples = append(samples, 40)
	}
	count, mean, deviation := mergedStats(30, 8, 0.9, samples...)
	insertOutlierStat(t, conn, fingerprintID, fixture.SourceIDs["canvas7p"], count, mean, deviation)
	insertOutlierStat(t, conn, fingerprintID, 0, count, mean, deviation)

	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		10,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	for _, row := range got {
		if row.FingerprintID == fingerprintID {
			return
		}
	}
	t.Fatalf("realistic outlier fingerprint %d was not returned; rows = %+v", fingerprintID, got)
}

func TestOutliersThresholdAndSourceVsGlobalBaseline(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	insideID := addOutlierCase(t, conn, fixture, "threshold_inside", 12.9, 10, 1, 0, 1)
	overID := addOutlierCase(t, conn, fixture, "threshold_over", 13.1, 10, 1, 100, 1)

	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		10,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	seen := map[int64]bool{}
	for _, row := range got {
		seen[row.FingerprintID] = true
	}
	if seen[insideID] {
		t.Fatalf("inside-threshold fingerprint %d was returned; source baseline excludes it, global baseline would include it", insideID)
	}
	if !seen[overID] {
		t.Fatalf("over-threshold fingerprint %d was not returned; source baseline includes it, global baseline would exclude it; rows = %+v", overID, got)
	}
}

func TestOutliersUsesRatioFallbackForZeroDeviationHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	fingerprintID := addOutlierCase(t, conn, fixture, "steady_then_slow", 17, 8, 0, 8, 0)
	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		10,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	for _, row := range got {
		if row.FingerprintID == fingerprintID {
			if row.SourceDeviationMS != 0 {
				t.Fatalf("source deviation = %v, want adjusted zero-deviation history", row.SourceDeviationMS)
			}
			return
		}
	}
	t.Fatalf("zero-deviation ratio outlier fingerprint %d was not returned; rows = %+v", fingerprintID, got)
}

func TestOutliersOrdersBeforeLimit(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	count, mean, deviation := mergedStats(30, 0, 0.01, 0.5)
	if _, err := conn.Exec(ctx, `insert into rotten.fingerprint_stats
		(fingerprint_id, logical_source_id, type, count, mean, deviation, last)
		values ($1,$2,'mean_time',$3,$4,$5,0)`,
		fixture.FingerprintID["users"], fixture.SourceIDs["canvas7p"], count, mean, deviation); err != nil {
		t.Fatal(err)
	}

	got := readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"7",
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		1,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
	)
	if len(got) != 1 {
		t.Fatalf("got %d rows, want one: %+v", len(got), got)
	}
	if got[0].FingerprintID != fixture.FingerprintID["users"] {
		t.Fatalf("fingerprint = %d, want higher-deviation users %d before slow %d", got[0].FingerprintID, fixture.FingerprintID["users"], fixture.FingerprintID["slow"])
	}
}

func insertOutlierFingerprint(t *testing.T, conn *pgx.Conn, fingerprint string, normalized string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(),
		"insert into rotten.fingerprints (fingerprint, normalized) values ($1,$2) returning id",
		fingerprint, normalized).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertOutlierEvent(t *testing.T, conn *pgx.Conn, sourceID int, physicalSourceID int, fingerprintID int64, start time.Time, calls float64, totalMS float64) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `insert into rotten.events
		(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
		values ($1,$2,$3,$4,$5,$6,$7)`,
		fingerprintID, sourceID, physicalSourceID, start, start.Add(testdb.WindowLength), calls, totalMS); err != nil {
		t.Fatal(err)
	}
}

func insertOutlierStat(t *testing.T, conn *pgx.Conn, fingerprintID int64, sourceID int, count int64, mean float64, deviation float64) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `insert into rotten.fingerprint_stats
		(fingerprint_id, logical_source_id, type, count, mean, deviation, last)
		values ($1,$2,'mean_time',$3,$4,$5,0)`,
		fingerprintID, sourceID, count, mean, deviation); err != nil {
		t.Fatal(err)
	}
}

func addOutlierCase(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, key string, recentMean float64, sourceMean float64, sourceDeviation float64, globalMean float64, globalDeviation float64) int64 {
	t.Helper()
	fingerprintID := insertOutlierFingerprint(t, conn, key, "select "+key)
	start := fixture.Anchor.Add(-20 * time.Minute)
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas7p"], fixture.PhysicalIDs["canvas7p"], fingerprintID, start, 10, recentMean*10)
	sourceCount, sourceStoredMean, sourceStoredDeviation := mergedStats(30, sourceMean, sourceDeviation, recentMean)
	globalCount, globalStoredMean, globalStoredDeviation := mergedStats(30, globalMean, globalDeviation, recentMean)
	insertOutlierStat(t, conn, fingerprintID, fixture.SourceIDs["canvas7p"], sourceCount, sourceStoredMean, sourceStoredDeviation)
	insertOutlierStat(t, conn, fingerprintID, 0, globalCount, globalStoredMean, globalStoredDeviation)
	return fingerprintID
}

func mergedStats(n int64, mean float64, deviation float64, samples ...float64) (int64, float64, float64) {
	count := n + int64(len(samples))
	sum := float64(n) * mean
	sumSquares := float64(n-1)*deviation*deviation + float64(n)*mean*mean
	for _, sample := range samples {
		sum += sample
		sumSquares += sample * sample
	}
	mergedMean := sum / float64(count)
	if count < 2 {
		return count, mergedMean, 0
	}
	variance := (sumSquares - float64(count)*mergedMean*mergedMean) / float64(count-1)
	if variance < 0 {
		variance = 0
	}
	return count, mergedMean, math.Sqrt(variance)
}
