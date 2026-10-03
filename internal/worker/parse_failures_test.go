package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
	w.buildHarvestBatch(ctx, texts, deltas, time.Unix(1, 0), time.Unix(2, 0))
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
	for i, d := range topNDeltas(deltas, topDeltasPerMetric) {
		quoted := fmt.Sprintf("%q", d.Query)
		if present := strings.Contains(got, quoted); present != (i < 5) {
			t.Errorf("sample %d present = %v, want %v: %s", i, present, i < 5, got)
		}
	}
	if w.eventsPending.Load() != 0 || w.fingerprintCount() != 0 {
		t.Fatal("failed queries should not queue events or create fingerprints")
	}
}

func TestNoIdleHandsWatchdogFiresOnStalledWorker(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	fired := make(chan string, 1)
	w := New(Config{
		ObservationInterval: 1,
		ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
		SanityCheck:         "select true from pg_sleep(30)",
		State:               store,
		MaxReconnectBackoff: 100 * time.Millisecond,
		ConnectTimeout:      10 * time.Millisecond,
		WatchdogMargin:      10 * time.Millisecond,
		WatchdogExit: func(reason string) {
			fired <- reason
		},
		Logger: slog.Default(),
	}, RealClock{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	go w.ReportProgress(ctx, true, 1)
	select {
	case reason := <-fired:
		if !strings.Contains(reason, "no worker liveness") {
			t.Fatalf("watchdog reason = %q, want no worker liveness", reason)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog did not fire")
	}
	cancel()
	<-done
}

func TestNoIdleHandsWatchdogDoesNotFireOnHealthyRunLoop(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	fired := make(chan string, 1)
	reC, reA, reJ := sampleRegexes(t)
	w := New(Config{
		ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           12,
		PhysicalID:          47,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		State:               store,
		ServerOutbox:        store,
		MaxReconnectBackoff: 100 * time.Millisecond,
		ConnectTimeout:      10 * time.Millisecond,
		WatchdogMargin:      10 * time.Millisecond,
		WatchdogExit: func(reason string) {
			fired <- reason
		},
		Logger: slog.Default(),
	}, RealClock{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	go w.ReportProgress(ctx, true, 1)
	time.Sleep(4 * time.Second)
	mid := w.liveness.Load()
	if mid == 0 {
		t.Fatal("liveness counter did not advance by midpoint")
	}
	time.Sleep(4 * time.Second)
	if got := w.liveness.Load(); got <= mid {
		t.Fatalf("liveness counter = %d at end, want greater than midpoint %d", got, mid)
	}
	cancel()
	select {
	case reason := <-fired:
		t.Fatalf("watchdog fired on healthy worker: %s", reason)
	default:
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestWatchdogTimeoutCoversDefaultReconnectBackoff(t *testing.T) {
	w := New(Config{ObservationInterval: 1}, RealClock{})
	got := w.watchdogTimeout(time.Second, time.Second)
	wantAtLeast := w.maxReconnectBackoff() + w.connectTimeout() + w.watchdogMargin()
	if got < wantAtLeast {
		t.Fatalf("watchdog timeout = %s, want at least reconnect term %s", got, wantAtLeast)
	}
	if got == 3*time.Second {
		t.Fatal("watchdog timeout used only 3*base and ignored reconnect backoff")
	}
}

func TestNoIdleHandsWatchdogDoesNotFireDuringReconnectBackoffCap(t *testing.T) {
	fired := make(chan string, 1)
	w := New(Config{
		ObservedDBConnect: func(context.Context) (*pgx.Conn, error) {
			return nil, syscall.ECONNREFUSED
		},
		ReconnectBackoff: func(int) time.Duration {
			return 200 * time.Millisecond
		},
		MaxReconnectBackoff: 200 * time.Millisecond,
		ConnectTimeout:      10 * time.Millisecond,
		WatchdogMargin:      10 * time.Millisecond,
		ObservationInterval: 1,
		SanityCheck:         "select true",
		WatchdogExit: func(reason string) {
			fired <- reason
		},
		Logger: slog.Default(),
	}, RealClock{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	go w.ReportProgress(ctx, true, 1)
	time.Sleep(4 * time.Second)
	cancel()
	select {
	case reason := <-fired:
		t.Fatalf("watchdog fired during reconnect attempts: %s", reason)
	default:
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
