package worker

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/benchub/rotten/internal/pssc"
	"github.com/benchub/rotten/internal/testdb"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func loggingWorker(logs *syncBuffer) *Worker {
	return New(Config{Logger: slog.New(slog.NewTextHandler(logs, nil))}, RealClock{})
}

// At startup Run logs whether pssc is in use on the observed database, and
// with the test image's settings (position=any, job_tag listed) it doesn't
// warn about them.
func TestStartupLogsPSSCInUse(t *testing.T) {
	db := startObservedForWorker(t)
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	rn.stop(t)
	got := logs.String()
	if !strings.Contains(got, psscInUseMessage) || !strings.Contains(got, "schema=public") {
		t.Fatalf("logs = %s, want %q with schema=public", got, psscInUseMessage)
	}
	for _, w := range []string{psscExtractorsWarning, psscTagsWarning, psscNotInUseMessage} {
		if strings.Contains(got, w) {
			t.Fatalf("logs = %s, want no %q", got, w)
		}
	}
}

func TestStartupLogsPSSCNotInUse(t *testing.T) {
	db := testdb.StartObservedWithoutPSSC(t, 16)
	setupObservedForWorker(t, db)
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	rn.stop(t)
	got := logs.String()
	if !strings.Contains(got, psscNotInUseMessage) || strings.Contains(got, psscInUseMessage) {
		t.Fatalf("logs = %s, want %q only", got, psscNotInUseMessage)
	}
	if strings.Contains(got, psscExtractorsWarning) || strings.Contains(got, psscTagsWarning) {
		t.Fatalf("logs = %s, want no settings warnings without pssc", got)
	}
}

func startLoggingWorker(t *testing.T, db *testdb.DB, logs *syncBuffer) *running {
	t.Helper()
	store := openStore(t, t.TempDir())
	t.Cleanup(func() { store.Close() })
	return startOutboxWorkerWith(t, db.DSNAs(t, "rotten_observer"), 7, 42, store, store,
		slog.New(slog.NewTextHandler(logs, nil)))
}

// pssc's defaults (append-only extractors) can't see prepended marginalia,
// and an allowlist without job or job_tag drops jobs. Each warns once per
// process, however often the worker reconnects.
func TestPSSCSettingsWarnOncePerProcess(t *testing.T) {
	var logs syncBuffer
	w := loggingWorker(&logs)
	for range 3 {
		w.checkPSSCSettings(pssc.Settings{Extractors: "sqlcommenter, marginalia", Tags: "action, controller"})
	}
	got := logs.String()
	if n := strings.Count(got, psscExtractorsWarning); n != 1 {
		t.Fatalf("extractors warned %d times, want 1: %s", n, got)
	}
	if n := strings.Count(got, psscTagsWarning); n != 1 || !strings.Contains(got, "job or job_tag") {
		t.Fatalf("tags warned %d times, want 1 naming job or job_tag: %s", n, got)
	}

	var quiet syncBuffer
	w = loggingWorker(&quiet)
	w.checkPSSCSettings(pssc.Settings{Extractors: "marginalia(position=prepend)", Tags: "*"})
	w.checkPSSCSettings(pssc.Settings{Extractors: "marginalia(position=any)", Tags: "controller,action,job_tag"})
	if quiet.String() != "" {
		t.Fatalf("logs = %s, want nothing", quiet.String())
	}
}

// utility_missing_queryid rising 3 times within the last 6 harvests warns
// once, whether or not the rises are back to back; a single rise (a
// prepared utility statement re-run) doesn't. A reconnect starts over.
func TestPSSCUtilityMissingQueryIDRule(t *testing.T) {
	feed := func(w *Worker, ns ...int64) {
		for _, n := range ns {
			w.notePSSCUtilityMissing(n)
		}
	}
	warned := func(l *syncBuffer) int { return strings.Count(l.String(), psscUtilityMissingWarning) }

	var logs syncBuffer
	w := loggingWorker(&logs)
	w.psscConnected()
	feed(w, 5, 6, 6, 6, 6, 6, 6, 6) // one rise
	if warned(&logs) != 0 {
		t.Fatalf("warned on one rise: %s", logs.String())
	}
	feed(w, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 9, 10) // never 3 rises in 6 harvests
	if warned(&logs) != 0 {
		t.Fatalf("warned on spread-out rises: %s", logs.String())
	}
	w.psscConnected()
	feed(w, 11, 12)
	if warned(&logs) != 0 {
		t.Fatalf("counted rises across a reconnect: %s", logs.String())
	}
	feed(w, 12, 13, 13, 14) // 3 non-consecutive rises in 5 harvests
	if warned(&logs) != 1 {
		t.Fatalf("warned %d times on 3 non-consecutive rises in 6 harvests, want 1: %s", warned(&logs), logs.String())
	}
	feed(w, 15, 16, 17, 18)
	if warned(&logs) != 1 {
		t.Fatalf("warned %d times, want once per process", warned(&logs))
	}
}
