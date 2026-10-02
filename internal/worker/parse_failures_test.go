package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/testdb"
)

func progressLog(w *Worker) string {
	var b bytes.Buffer
	old := log.Writer()
	log.SetOutput(&b)
	defer log.SetOutput(old)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.ReportProgress(ctx, false, 3600)
	return b.String()
}

func TestParseFailureSnapshots(t *testing.T) {
	w := New(Config{}, RealClock{})
	longQuery := strings.Repeat("x", 2000)
	w.recordParseFailure(longQuery)
	count, samples := w.parseFailureSnapshot()
	if count != 1 || len(samples) != 1 || samples[0] != longQuery[:1024]+"..." {
		t.Fatalf("long sample not bounded: count=%d samples=%q", count, samples)
	}
	samples[0] = "changed snapshot"
	_, next := w.parseFailureSnapshot()
	if next[0] == samples[0] {
		t.Fatal("snapshot aliases the worker's samples")
	}
	w.resetParseFailures()
	count, samples = w.parseFailureSnapshot()
	if count != 0 || len(samples) != 0 {
		t.Fatalf("reset retained failures: count=%d samples=%q", count, samples)
	}
	if got := progressLog(w); strings.Contains(got, "fingerprint failure samples") {
		t.Fatalf("reset still reports samples: %s", got)
	}
	w.recordParseFailure("new window")
	count, samples = w.parseFailureSnapshot()
	if count != 1 || len(samples) != 1 || samples[0] != "new window" {
		t.Fatalf("new window retained old samples: count=%d samples=%q", count, samples)
	}
}

func TestParseFailureConcurrentSnapshots(t *testing.T) {
	w := New(Config{}, RealClock{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				w.recordParseFailure("bad query")
				count, samples := w.parseFailureSnapshot()
				if len(samples) != min(int(count), 5) {
					t.Errorf("inconsistent snapshot: count=%d samples=%q", count, samples)
				}
			}
		})
	}
	wg.Wait()
	count, samples := w.parseFailureSnapshot()
	if count != 400 || len(samples) != 5 {
		t.Fatalf("concurrent counts/samples lost: count=%d samples=%q", count, samples)
	}
	// Exercise reset concurrently with record/snapshot under the race detector.
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				w.resetParseFailures()
				w.recordParseFailure("another query")
				count, samples := w.parseFailureSnapshot()
				if len(samples) != min(int(count), 5) {
					t.Errorf("inconsistent reset snapshot: count=%d samples=%q", count, samples)
				}
			}
		})
	}
	wg.Wait()
}

// Exercise the real send path with query texts from Postgres 18, rather
// than recording failures directly or relying on the parser's own log.
func TestParseFailuresCountedAndSampled(t *testing.T) {
	db := testdb.StartObserved(t, 18)
	conn := db.Connect(t)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		table := fmt.Sprintf("parse_failure_%d", i)
		for _, q := range []string{
			"create table " + table + " (id int)",
			"insert into " + table + " values (1)",
			"update " + table + " set id = 2 returning with (old as o, new as n) o.id, n.id",
		} {
			if _, err := conn.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	reader := pgss.NewReader(conn)
	stats, err := reader.ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	texts := pgss.NewTextCache(reader)
	if err := texts.Fill(ctx, stats); err != nil {
		t.Fatal(err)
	}
	var deltas []pgss.Delta
	for _, s := range stats {
		if strings.HasPrefix(s.Query, "update parse_failure_") {
			// Give each delta a different cost to make the sample order stable.
			s.TotalExecTime = float64(len(deltas) + 1)
			deltas = append(deltas, pgss.Delta{Stat: s})
		}
	}
	if len(deltas) != 7 {
		t.Fatalf("want 7 failing statements from pg_stat_statements, got %d", len(deltas))
	}
	w := New(Config{}, RealClock{})
	var sendLog bytes.Buffer
	old := log.Writer()
	log.SetOutput(&sendLog)
	w.send(ctx, texts, deltas, time.Unix(1, 0), time.Unix(2, 0))
	log.SetOutput(old)
	if !strings.Contains(sendLog.String(), "window fingerprint failures: 7; fingerprint failure samples (up to 5):") {
		t.Errorf("window summary missing; a later harvest could hide failures: %s", sendLog.String())
	}
	got := progressLog(w)
	if !strings.Contains(got, "7 fingerprints failed") {
		t.Errorf("failure count missing from progress: %s", got)
	}
	if !strings.Contains(got, "fingerprint failure samples (up to 5):") {
		t.Errorf("failure samples missing from progress: %s", got)
	}
	for i, d := range topNDeltas(deltas, 100) {
		quoted := fmt.Sprintf("%q", d.Query)
		if present := strings.Contains(got, quoted); present != (i < 5) {
			t.Errorf("sample %d present = %v, want %v: %s", i, present, i < 5, got)
		}
	}
	if w.eventsPending.Load() != 0 || w.fingerprintCount() != 0 {
		t.Fatal("failed queries should not queue events or create fingerprints")
	}
}
