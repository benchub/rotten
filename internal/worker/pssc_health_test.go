package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

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
// distinct value, however often it's rechecked; a fix and a relapse warns
// again.
func TestPSSCSettingsWarnOncePerValue(t *testing.T) {
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

	w.checkPSSCSettings(pssc.Settings{Extractors: "marginalia(position=any)", Tags: "*"})
	w.checkPSSCSettings(pssc.Settings{Extractors: "sqlcommenter, marginalia", Tags: "action, controller"})
	got = logs.String()
	if a, b := strings.Count(got, psscExtractorsWarning), strings.Count(got, psscTagsWarning); a != 2 || b != 2 {
		t.Fatalf("after fix and relapse warned extractors %d, tags %d times, want 2 each: %s", a, b, got)
	}

	var quiet syncBuffer
	w = loggingWorker(&quiet)
	w.checkPSSCSettings(pssc.Settings{Extractors: "marginalia(position=prepend)", Tags: "*"})
	w.checkPSSCSettings(pssc.Settings{Extractors: "marginalia(position=any)", Tags: "controller,action,job_tag"})
	if quiet.String() != "" {
		t.Fatalf("logs = %s, want nothing", quiet.String())
	}
}

// pssc applies the tags allowlist after an extractor's rename parameter, so
// tags names final keys. A route key renamed to controller is kept only
// because tags lists controller, and the check doesn't warn.
func TestPSSCTagsCheckIgnoresRename(t *testing.T) {
	var logs syncBuffer
	w := loggingWorker(&logs)
	w.checkPSSCSettings(pssc.Settings{
		Extractors: "sqlcommenter(position=any, rename=route:controller)",
		Tags:       "action, controller, job",
	})
	if got := logs.String(); strings.Contains(got, psscTagsWarning) {
		t.Fatalf("logs = %s, want no %q", got, psscTagsWarning)
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

// A pssc created after the worker connects is noticed at the next harvest,
// which logs that it's in use.
func TestPSSCCreatedAfterConnectLogsInUse(t *testing.T) {
	db := startObservedForWorker(t)
	su := db.Connect(t)
	ctx := context.Background()
	if _, err := su.Exec(ctx, "drop extension pg_stat_statement_context"); err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	if got := logs.String(); !strings.Contains(got, psscNotInUseMessage) || strings.Contains(got, psscInUseMessage) {
		t.Fatalf("logs = %s, want %q only", got, psscNotInUseMessage)
	}
	if _, err := su.Exec(ctx, "create extension pg_stat_statement_context"); err != nil {
		t.Fatal(err)
	}
	rn.window(t)
	rn.window(t)
	got := logs.String()
	if n := strings.Count(got, psscInUseMessage); n != 1 {
		t.Fatalf("in-use logged %d times, want 1: %s", n, got)
	}
}

// A reload that drops a mapped key from tags warns at the next harvest, once
// per change: repeating the bad value stays quiet, and a later, different
// bad value warns again.
func TestPSSCTagsChangedByReloadWarns(t *testing.T) {
	db := startObservedVersionForWorker(t, 16, testdb.PSSCTagsFromConfig())
	su := db.Connect(t)
	ctx := context.Background()
	setTags := func(v string) {
		t.Helper()
		for _, q := range []string{
			"alter system set pg_stat_statement_context.tags = '" + v + "'",
			"select pg_reload_conf()",
		} {
			if _, err := su.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		// pg_reload_conf only signals; wait for this backend to see it.
		var last string
		for range 50 {
			var s string
			if err := su.QueryRow(ctx, "show pg_stat_statement_context.tags").Scan(&s); err != nil {
				t.Fatal(err)
			}
			last = s
			if s == v {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("tags never became %q (last %q)", v, last)
	}
	setTags(testdb.PSSCTags)
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	if strings.Contains(logs.String(), psscTagsWarning) {
		t.Fatalf("warned before the change: %s", logs.String())
	}
	setTags("action, job")
	rn.window(t)
	rn.window(t)
	if n := strings.Count(logs.String(), psscTagsWarning); n != 1 || !strings.Contains(logs.String(), "missing=controller") {
		t.Fatalf("tags warned %d times, want 1 naming controller: %s", n, logs.String())
	}
	setTags("controller, job")
	rn.window(t)
	if n := strings.Count(logs.String(), psscTagsWarning); n != 2 {
		t.Fatalf("tags warned %d times after a second bad change, want 2: %s", n, logs.String())
	}
}
