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
	LogicalSourceID  int
	FingerprintID    int64
	Project          string
	Environment      string
	Cluster          string
	Role             string
	Calls            float64
	TotalMS          float64
	AvgMSPerCall     float64
	WorstWindowStart time.Time
	WorstMSPerCall   float64
	HistorySamples   int64
	HistoryMedianMS  float64
	HistorySpreadMS  float64
	Score            float64
	Example          string
	ContextJSON      []byte
	Unparsed         bool
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
			&r.WorstWindowStart,
			&r.WorstMSPerCall,
			&r.HistorySamples,
			&r.HistoryMedianMS,
			&r.HistorySpreadMS,
			&r.Score,
			&r.Example,
			&r.ContextJSON,
			&r.Unparsed,
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

func readFixtureOutliers(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, cluster string, limit int) []outlierRow {
	t.Helper()
	return readOutliers(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		cluster,
		fixture.Anchor.Add(-testdb.RecentRange),
		fixture.Anchor,
		limit,
		defaultSigma,
		defaultMinHistory,
		defaultRatio,
		nil,
		nil,
	)
}

func TestOutliersScoresWorstWindowAgainstSourceHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// A huge sample on cluster 13, with no history there, isn't listed.
	unrelatedID := insertOutlierFingerprint(t, conn, "unrelated_global_noise", "select unrelated global noise")
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas13p"], fixture.PhysicalIDs["canvas13p"], unrelatedID, fixture.Anchor.Add(-20*time.Minute), 1, 1_000_000)

	got := readFixtureOutliers(t, conn, fixture, "7", 10)
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
	if want := fixture.Anchor.Add(-40 * time.Minute); !row.WorstWindowStart.Equal(want) || row.WorstMSPerCall != 40 {
		t.Errorf("worst window = %v at %v ms/call, want %v at 40", row.WorstWindowStart, row.WorstMSPerCall, want)
	}
	// 40 history samples, median 8, MAD 0.5 (1.4826 × 0.5 = 0.74) under the
	// ratio floor (2 - 1) / 3 × 8.
	spread := 8.0 / 3
	if row.HistorySamples != 40 || row.HistoryMedianMS != 8 || math.Abs(row.HistorySpreadMS-spread) > 1e-9 {
		t.Errorf("history samples/median/spread = %v/%v/%v, want 40/8/%v", row.HistorySamples, row.HistoryMedianMS, row.HistorySpreadMS, spread)
	}
	if want := (40 - 8) / spread; math.Abs(row.Score-want) > 1e-9 {
		t.Errorf("score = %v, want %v", row.Score, want)
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

// MAD path: history 4, 7, 10, 13, 16 has median 10 and MAD 3, so the spread
// is 1.4826 × 3 = 4.4478 (over the ratio floor 10 / 3) and the threshold is
// 10 + 3 × 4.4478 = 23.34. Ratio path: a flat history of 10 has spread 10 / 3
// and threshold 20. History on another source doesn't count.
func TestOutliersThresholdAndSourceOwnedHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start := fixture.Anchor.Add(-testdb.RecentRange)
	historyStart := start.Add(-2 * time.Hour)
	inRange := start.Add(time.Hour)
	spreadCase := func(key string, history []float64, worst float64) int64 {
		h := newOutlierHistory(t, conn, fixture, "canvas7p", key)
		h.add(historyStart, time.Minute, history...)
		h.add(inRange, time.Minute, 10, worst, 10)
		return h.finish()
	}
	noisy := repeat(40, 4, 7, 10, 13, 16)
	flat := repeat(40, 10)
	madInside := spreadCase("mad_inside", noisy, 23.2)
	madOver := spreadCase("mad_over", noisy, 23.5)
	ratioInside := spreadCase("ratio_inside", flat, 19.9)
	ratioOver := spreadCase("ratio_over", flat, 20.1)

	// Plenty of history, but on the replica.
	other := newOutlierHistory(t, conn, fixture, "canvas7r", "other_source")
	other.add(historyStart, time.Minute, flat...)
	otherSource := other.fingerprID
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas7p"], fixture.PhysicalIDs["canvas7p"], otherSource, inRange, 10, 1000)

	seen := map[int64]outlierRow{}
	for _, row := range readFixtureOutliers(t, conn, fixture, "7", 50) {
		seen[row.FingerprintID] = row
	}
	for _, c := range []struct {
		name   string
		id     int64
		listed bool
	}{
		{"mad_inside", madInside, false},
		{"mad_over", madOver, true},
		{"ratio_inside", ratioInside, false},
		{"ratio_over", ratioOver, true},
		{"other_source", otherSource, false},
	} {
		if _, ok := seen[c.id]; ok != c.listed {
			t.Errorf("%s listed = %v, want %v; rows = %+v", c.name, ok, c.listed, seen)
		}
	}
	if row, ok := seen[madOver]; ok {
		if want := 1.4826 * 3; math.Abs(row.HistorySpreadMS-want) > 1e-9 {
			t.Errorf("mad_over spread = %v, want %v", row.HistorySpreadMS, want)
		}
	}
}

func TestOutliersOrdersBeforeLimit(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// Slower per call than slow's 40 ms, but a lower score: 100 over a
	// median of 40 with spread 40 / 3 scores 4.5, under slow's 12.
	start := fixture.Anchor.Add(-testdb.RecentRange)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "slower_lower_score")
	h.add(start.Add(-2*time.Hour), time.Minute, repeat(40, 40)...)
	h.add(start.Add(time.Hour), time.Minute, 100)
	lower := h.finish()

	all := readFixtureOutliers(t, conn, fixture, "7", 10)
	if len(all) != 2 || all[0].FingerprintID != fixture.FingerprintID["slow"] || all[1].FingerprintID != lower {
		t.Fatalf("outliers = %+v, want slow then %d", all, lower)
	}
	got := readFixtureOutliers(t, conn, fixture, "7", 1)
	if len(got) != 1 || got[0].FingerprintID != fixture.FingerprintID["slow"] {
		t.Fatalf("limit 1 = %+v, want higher-score slow %d before %d", got, fixture.FingerprintID["slow"], lower)
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
