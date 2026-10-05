package reports_test

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// These tests cover the per-window scoring: each in-range window is scored
// against the median and MAD of the fingerprint's samples on the same source
// before the range.

// outlierHistory places samples on one (source, fingerprint): events with 10
// calls each, so time = 10 × ms per call. finish also stores
// fingerprint_stats over every sample, the way ingest would, so a report that
// used the stored mean and deviation would see the same data.
type outlierHistory struct {
	t          *testing.T
	conn       *pgx.Conn
	sourceID   int
	physicalID int
	fingerprID int64
	samples    []float64
}

const outlierTestWindow = 10 * time.Second

func newOutlierHistory(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, source, key string) *outlierHistory {
	t.Helper()
	return &outlierHistory{
		t: t, conn: conn,
		sourceID:   fixture.SourceIDs[source],
		physicalID: fixture.PhysicalIDs[source],
		fingerprID: insertOutlierFingerprint(t, conn, key, "select "+key),
	}
}

// add places one window per value, every step, starting at first.
func (h *outlierHistory) add(first time.Time, step time.Duration, ms ...float64) *outlierHistory {
	h.t.Helper()
	for i, v := range ms {
		start := first.Add(time.Duration(i) * step)
		if _, err := h.conn.Exec(context.Background(), `insert into rotten.events
			(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			values ($1,$2,$3,$4,$5,10,$6)`,
			h.fingerprID, h.sourceID, h.physicalID, start, start.Add(outlierTestWindow), v*10); err != nil {
			h.t.Fatal(err)
		}
		h.samples = append(h.samples, v)
	}
	return h
}

func (h *outlierHistory) finish() int64 {
	h.t.Helper()
	count, mean, deviation := mergedStats(0, 0, 0, h.samples...)
	for _, source := range []int{h.sourceID, 0} {
		insertOutlierStat(h.t, h.conn, h.fingerprID, source, count, mean, deviation)
	}
	return h.fingerprID
}

// repeat returns n values cycling through pattern.
func repeat(n int, pattern ...float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = pattern[i%len(pattern)]
	}
	return out
}

func concat(parts ...[]float64) []float64 {
	var out []float64
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// readOutlierMaps runs outliers.sql and returns each row by column name.
func readOutlierMaps(t *testing.T, conn *pgx.Conn, args ...any) []map[string]any {
	t.Helper()
	query, err := os.ReadFile("outliers.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		row := map[string]any{}
		for i, f := range rows.FieldDescriptions() {
			row[f.Name] = vals[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func outliersForRange(t *testing.T, conn *pgx.Conn, cluster string, start, end time.Time) []map[string]any {
	t.Helper()
	return readOutlierMaps(t, conn, "canvas", testdb.ReportEnvironment, cluster, start, end, 50,
		defaultSigma, defaultMinHistory, defaultRatio, nil, nil)
}

func findOutlier(rows []map[string]any, fingerprintID int64) map[string]any {
	for _, r := range rows {
		if id, ok := r["fingerprint_id"].(int64); ok && id == fingerprintID {
			return r
		}
	}
	return nil
}

func asFloat(t *testing.T, v any) float64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("%v (%T) is not a float64", v, v)
	}
	return f
}

// The backlog's red test: 60 normal windows and 2 slow ones in a 1-hour
// range, and a history with one earlier slow spell. The range's average is
// barely above normal, and the earlier spell widens the history's standard
// deviation, but the slow windows stand far above the history's median.
func TestOutliersListsShortSpellInPresetRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-time.Hour)
	normal := []float64{9, 9.5, 10, 10.5, 11}
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "short_spell")
	// History: 100 normal windows, then a 12-window slow spell, ending
	// before the range.
	h.add(start.Add(-3*time.Hour), 55*time.Second, concat(repeat(100, normal...), repeat(12, 100))...)
	// The range: 30 normal, 2 slow, 30 normal, 55 seconds apart.
	inRange := concat(repeat(30, normal...), []float64{100, 120}, repeat(30, normal...))
	h.add(start.Add(time.Minute), 55*time.Second, inRange...)
	fingerprintID := h.finish()

	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("fingerprint %d with a 2-window slow spell in a 1-hour range was not listed", fingerprintID)
	}
	worstStart := start.Add(time.Minute + 31*55*time.Second)
	if got, ok := row["worst_window_start"].(time.Time); !ok || !got.Equal(worstStart) {
		t.Errorf("worst_window_start = %v, want %v", row["worst_window_start"], worstStart)
	}
	if got := asFloat(t, row["worst_ms_per_call"]); got != 120 {
		t.Errorf("worst_ms_per_call = %v, want 120", got)
	}
	if got := asFloat(t, row["history_median_ms"]); got != 10 {
		t.Errorf("history_median_ms = %v, want 10", got)
	}
	if got, ok := row["history_samples"].(int64); !ok || got != 112 {
		t.Errorf("history_samples = %v, want 112", row["history_samples"])
	}
	// MAD is 0.5 (×1.4826 = 0.74), below the ratio floor (2 − 1) / 3 × 10.
	spread := 10.0 / 3
	if got := asFloat(t, row["history_spread_ms"]); math.Abs(got-spread) > 1e-9 {
		t.Errorf("history_spread_ms = %v, want %v", got, spread)
	}
	if got, want := asFloat(t, row["score"]), (120-10)/spread; math.Abs(got-want) > 1e-9 {
		t.Errorf("score = %v, want %v", got, want)
	}
}

// A long past spell, over a quarter of the history, doesn't hide a new one:
// the median and MAD ignore it, where the mean and standard deviation don't.
func TestOutliersPastSpellDoesNotHideNewOne(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-3 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "repeat_spell")
	h.add(start.Add(-4*time.Hour), time.Minute, concat(repeat(80, 9, 10, 11), repeat(30, 100))...)
	h.add(start.Add(time.Hour), time.Minute, concat(repeat(5, 100), repeat(20, 9, 10, 11))...)
	fingerprintID := h.finish()

	if findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID) == nil {
		t.Fatalf("fingerprint %d slow again after an earlier long spell was not listed", fingerprintID)
	}
}

// A flat history with tiny jitter would give a huge score to a window only a
// little slower; the floor on the spread keeps it off the list.
func TestOutliersIgnoresSmallRiseOverFlatBaseline(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "flat_jitter")
	h.add(start.Add(-2*time.Hour), time.Minute, repeat(60, 10, 10.001, 9.999)...)
	h.add(start.Add(10*time.Minute), time.Minute, 10.5, 11, 12, 15, 19.5)
	fingerprintID := h.finish()

	if row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID); row != nil {
		t.Fatalf("flat-baseline fingerprint %d at under 2× its median was listed: %v", fingerprintID, row)
	}
}

// History is only what came before the range: samples after it don't count
// toward the minimum, so 29 before and 20 after isn't enough.
func TestOutliersNeedsEnoughHistoryBeforeRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor.Add(-2 * time.Hour)
	start := end.Add(-time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "thin_history")
	h.add(start.Add(-time.Hour), time.Minute, repeat(29, 9, 10, 11)...)
	h.add(start.Add(10*time.Minute), time.Minute, 100, 100, 100)
	h.add(end.Add(10*time.Minute), time.Minute, repeat(20, 9, 10, 11)...)
	fingerprintID := h.finish()

	if row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID); row != nil {
		t.Fatalf("fingerprint %d with 29 samples before the range was listed: %v", fingerprintID, row)
	}

	// One more sample before the range is enough.
	h.add(start.Add(-2*time.Hour), time.Minute, 10)
	if findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID) == nil {
		t.Fatalf("fingerprint %d with 30 samples before the range was not listed", fingerprintID)
	}
}
