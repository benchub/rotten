package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/pgss"
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

func noContextWarnings(logs string) int {
	return strings.Count(logs, noContextMatchesWarning)
}

// runStep is some traffic run before one harvest window: n runs of query,
// by the app (a superuser connection) or by the observer role itself.
type runStep struct {
	observer bool
	query    string
	n        int
}

// TestWarnsWhenPostgres18DropsLeadingMarginalia runs the real worker against
// real Postgres. On 18 a leading comment is gone from pg_stat_statements'
// text, so the configured regexes never match and the worker warns, once.
// Trailing comments survive on 18, 17 keeps leading ones, and never-matching
// regexes mean contexts are off on purpose, so none of those warn. The
// observer's own statements don't count, statements the fingerprinter can't
// parse still do, and one match anywhere on the connection, even after a
// window of untagged traffic, means no warning.
func TestWarnsWhenPostgres18DropsLeadingMarginalia(t *testing.T) {
	const (
		tags     = `/*controller:users,action:show,job:CleanupJob*/`
		leading  = tags + ` select count(*) from widgets where id > 3`
		trailing = `select count(*) from widgets where id > 3 ` + tags
		untagged = `select count(*) from widgets where id < 5`
		// Postgres 18 syntax the pinned Postgres 17 parser rejects.
		pg18Only         = `update widgets set name = name where id = 1 returning with (old as o, new as n) o.id`
		pg18OnlyLeading  = tags + ` ` + pg18Only
		pg18OnlyTrailing = pg18Only + ` ` + tags
	)
	half := contextWarnMinCalls/2 + 1
	app := func(q string, n int) []runStep { return []runStep{{query: q, n: n}} }
	repeat := func(steps []runStep, windows int) [][]runStep {
		out := make([][]runStep, windows)
		for i := range out {
			out[i] = steps
		}
		return out
	}
	cases := []struct {
		name      string
		version   int
		windows   [][]runStep
		disabled  bool
		want      int
		wantMatch bool
		// setup runs as superuser before the worker starts; sanity
		// replaces the worker's sanity check.
		setup  []string
		sanity string
		// fewCalls means the watch must stay under contextWarnMinCalls;
		// otherwise it must reach it, so a quiet case isn't quiet for
		// lack of traffic.
		fewCalls bool
	}{
		{name: "pg18 leading", version: 18, windows: repeat(app(leading, half), contextWarnMinWindows+1), want: 1},
		{name: "pg18 trailing", version: 18, windows: repeat(app(trailing, half), contextWarnMinWindows+1), wantMatch: true},
		{name: "pg17 leading", version: 17, windows: repeat(app(leading, half), contextWarnMinWindows+1), wantMatch: true},
		{name: "pg18 leading contexts disabled", version: 18, windows: repeat(app(leading, half), contextWarnMinWindows+1), disabled: true},
		{name: "pg18 unparseable leading", version: 18, windows: repeat(app(pg18OnlyLeading, half), contextWarnMinWindows+1), want: 1},
		{name: "pg18 unparseable trailing", version: 18, windows: repeat(app(pg18OnlyTrailing, half), contextWarnMinWindows+1), wantMatch: true},
		{name: "pg18 observer only", version: 18, windows: repeat([]runStep{{observer: true, query: "select 1", n: half}}, contextWarnMinWindows+1), fewCalls: true},
		// With track = all, a SECURITY DEFINER sanity function's nested
		// statements are recorded under its owner, not the observer.
		{name: "pg18 observer security definer sanity, track all", version: 18, windows: repeat(nil, contextWarnMinWindows+1), fewCalls: true,
			setup: []string{
				"alter system set pg_stat_statements.track = 'all'",
				"select pg_reload_conf()",
				`create function sanity_check() returns boolean language plpgsql security definer as $$
begin
  for i in 1..` + fmt.Sprint(half) + ` loop
    perform count(*) from widgets where id > i;
  end loop;
  return true;
end $$`,
			},
			sanity: "select sanity_check()"},
		{name: "pg18 untagged tool traffic then tagged job", version: 18, windows: append(
			[][]runStep{app(untagged, 2*contextWarnMinCalls), app(trailing, 5)},
			repeat(nil, contextWarnMinWindows)...), wantMatch: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observed := startObservedVersionForWorker(t, tc.version)
			store := openStore(t, t.TempDir())
			defer store.Close()
			workload := observed.Connect(t)
			for _, q := range tc.setup {
				if _, err := workload.Exec(context.Background(), q); err != nil {
					t.Fatalf("%.60s: %v", q, err)
				}
			}
			sanity := "select true"
			if tc.sanity != "" {
				sanity = tc.sanity
			}
			observerDSN := observed.DSNAs(t, "rotten_observer")
			observerTraffic := observerConn(t, observerDSN)

			reC, reA, reJ := sampleRegexes(t)
			if tc.disabled {
				reC, reA, reJ = regexp.MustCompile(`a^`), regexp.MustCompile(`a^`), regexp.MustCompile(`a^`)
			}
			var logs syncBuffer
			cfg := Config{
				ObservedDB:          observerConn(t, observerDSN),
				ObservationInterval: 2,
				SanityCheck:         sanity,
				LogicalID:           7,
				PhysicalID:          42,
				ReController:        reC,
				ReAction:            reA,
				ReJobTag:            reJ,
				State:               store,
				ServerOutbox:        store,
				Logger:              slog.New(slog.NewTextHandler(&logs, nil)),
			}
			clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
			w := New(cfg, clk)
			ctx, cancel := context.WithCancel(context.Background())
			rn := &running{w: w, clk: clk, cancel: cancel, ran: make(chan error, 1)}
			go func() { rn.ran <- w.Run(ctx) }()
			t.Cleanup(func() { rn.stop(t) })
			clk.waitSleep(t)
			rn.harvest = append(rn.harvest, w.lastHarvest.Load())

			for _, steps := range tc.windows {
				for _, st := range steps {
					conn := workload
					if st.observer {
						conn = observerTraffic
					}
					runQuery(t, conn, st.query, st.n)
				}
				rn.window(t)
			}
			rn.stop(t)

			// The quiet cases must be quiet for their stated reason, not
			// because the worker saw too little or the wrong server.
			if got := w.ctxWatch.versionNum / 10000; got != tc.version {
				t.Fatalf("server_version_num major = %d, want %d", got, tc.version)
			}
			if tc.fewCalls && w.ctxWatch.calls >= contextWarnMinCalls {
				t.Fatalf("worker counted %d calls, want under %d", w.ctxWatch.calls, contextWarnMinCalls)
			}
			if !tc.fewCalls && w.ctxWatch.calls < contextWarnMinCalls {
				t.Fatalf("worker counted %d calls, want at least %d", w.ctxWatch.calls, contextWarnMinCalls)
			}
			if w.ctxWatch.matched != tc.wantMatch {
				t.Fatalf("context matched = %v, want %v", w.ctxWatch.matched, tc.wantMatch)
			}

			got := logs.String()
			if n := noContextWarnings(got); n != tc.want {
				t.Fatalf("warning logged %d times, want %d; logs:\n%s", n, tc.want, got)
			}
			if tc.want > 0 && !strings.Contains(got, "level=WARN") {
				t.Fatalf("warning not at WARN level; logs:\n%s", got)
			}
			if w.ctxWatch.observerUserID == 0 {
				t.Fatal("observer role oid wasn't read")
			}
		})
	}
}

// TestPG18OnlySyntaxFailsTheFingerprinter keeps the unparseable cases above
// honest: they rely on the pinned parser rejecting this syntax.
func TestPG18OnlySyntaxFailsTheFingerprinter(t *testing.T) {
	q := `update widgets set name = name where id = $1 returning with (old as o, new as n) o.id /*controller:users*/`
	if _, err := fingerprinting.Normalized(q, fingerprinting.Options{}); err == nil {
		t.Fatal("the fingerprinter parses Postgres 18 RETURNING WITH; pick other syntax for the unparseable cases")
	}
}

func runQuery(t *testing.T, conn *pgx.Conn, query string, n int) {
	t.Helper()
	for range n {
		rows, err := conn.Query(context.Background(), query, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// contextWindow feeds one window of entries, run by userID, through
// buildHarvestBatchFromRows and the warning check, as harvest does.
func contextWindow(w *Worker, userID uint32, calls int64, queries ...string) {
	deltas := make([]pgss.Delta, len(queries))
	rows := make([]pgss.Stat, len(queries))
	for i, q := range queries {
		s := pgss.Stat{UserID: userID, DBID: 1, TopLevel: true, QueryID: int64(i + 1), Calls: calls, TotalExecTime: float64(i + 1)}
		deltas[i] = pgss.Delta{Stat: s}
		rows[i] = s
		rows[i].Query = q
	}
	_ = w.buildHarvestBatchFromRows(deltas, rows, time.Unix(1, 0), time.Unix(2, 0))
	w.maybeWarnNoContexts()
}

const (
	appUser      = 1
	observerUser = 99
)

func contextWarningWorker(t *testing.T, version int, logs *syncBuffer) *Worker {
	t.Helper()
	reC, reA, reJ := sampleRegexes(t)
	w := New(Config{ReController: reC, ReAction: reA, ReJobTag: reJ, Logger: slog.New(slog.NewTextHandler(logs, nil))}, RealClock{})
	w.observedConnected(version, observerUser)
	return w
}

func TestContextWarningRule(t *testing.T) {
	plain := "select 1 from widgets"
	tagged := "select 1 from widgets /*controller:users*/"
	unparseable := "update widgets set name = name returning with (old as o, new as n) o.id"

	t.Run("call threshold", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		for range contextWarnMinWindows {
			contextWindow(w, appUser, (contextWarnMinCalls-1)/contextWarnMinWindows, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 {
			t.Fatalf("warned %d times below the call threshold: %s", n, logs.String())
		}
		contextWindow(w, appUser, 1, plain)
		if n := noContextWarnings(logs.String()); n != 1 {
			t.Fatalf("warned %d times at the call threshold, want 1: %s", n, logs.String())
		}
	})

	t.Run("window threshold", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		for range contextWarnMinWindows - 1 {
			contextWindow(w, appUser, 10*contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 {
			t.Fatalf("warned %d times before %d windows: %s", n, contextWarnMinWindows, logs.String())
		}
		contextWindow(w, appUser, 0)
		if n := noContextWarnings(logs.String()); n != 1 {
			t.Fatalf("warned %d times at %d windows, want 1: %s", n, contextWarnMinWindows, logs.String())
		}
	})

	t.Run("warns once per process", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180004, &logs)
		for range 10 {
			contextWindow(w, appUser, contextWarnMinCalls/4, plain, plain)
		}
		w.observedConnected(180004, observerUser)
		for range 10 {
			contextWindow(w, appUser, contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 1 {
			t.Fatalf("warned %d times, want 1: %s", n, logs.String())
		}
		for _, attr := range []string{"server_version_num=180004", "sampled_calls=1500"} {
			if !strings.Contains(logs.String(), attr) {
				t.Fatalf("warning lacks %s: %s", attr, logs.String())
			}
		}
	})

	t.Run("one match anywhere means no warning", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		contextWindow(w, appUser, 1, tagged)
		for range 2 * contextWarnMinWindows {
			contextWindow(w, appUser, 10*contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 {
			t.Fatalf("warned %d times after a match: %s", n, logs.String())
		}
	})

	t.Run("a match on an earlier connection doesn't hide an upgrade to 18", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 170000, &logs)
		contextWindow(w, appUser, 1, tagged)
		w.observedConnected(180000, observerUser)
		for range contextWarnMinWindows {
			contextWindow(w, appUser, contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 1 {
			t.Fatalf("warned %d times, want 1: %s", n, logs.String())
		}
	})

	t.Run("the observer's own statements don't count", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		for range 2 * contextWarnMinWindows {
			contextWindow(w, observerUser, 10*contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 || w.ctxWatch.calls != 0 {
			t.Fatalf("warned %d times, counted %d calls: %s", n, w.ctxWatch.calls, logs.String())
		}
		contextWindow(w, observerUser, 1, tagged)
		if w.ctxWatch.matched {
			t.Fatal("the observer's own tagged statement counted as a match")
		}
	})

	t.Run("nested statements don't count", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		nested := func(q string) {
			st := pgss.Stat{UserID: appUser, DBID: 1, TopLevel: false, QueryID: 1, Calls: 10 * contextWarnMinCalls, TotalExecTime: 1}
			row := st
			row.Query = q
			_ = w.buildHarvestBatchFromRows([]pgss.Delta{{Stat: st}}, []pgss.Stat{row}, time.Unix(1, 0), time.Unix(2, 0))
			w.maybeWarnNoContexts()
		}
		nested(tagged)
		if w.ctxWatch.matched {
			t.Fatal("a nested tagged statement counted as a match")
		}
		for range 2 * contextWarnMinWindows {
			nested(plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 || w.ctxWatch.calls != 0 {
			t.Fatalf("warned %d times, counted %d calls: %s", n, w.ctxWatch.calls, logs.String())
		}
	})

	t.Run("statements the fingerprinter rejects still count", func(t *testing.T) {
		var logs syncBuffer
		w := contextWarningWorker(t, 180000, &logs)
		contextWindow(w, appUser, 1, unparseable+" /*controller:users*/")
		if !w.ctxWatch.matched {
			t.Fatal("a trailing comment on an unparseable statement didn't count as a match")
		}
		w = contextWarningWorker(t, 180000, &logs)
		for range contextWarnMinWindows {
			contextWindow(w, appUser, contextWarnMinCalls, unparseable)
		}
		if n := noContextWarnings(logs.String()); n != 1 {
			t.Fatalf("warned %d times, want 1: %s", n, logs.String())
		}
	})

	for _, v := range []int{0, 140000, 170005} {
		t.Run(fmt.Sprintf("version %d never warns", v), func(t *testing.T) {
			var logs syncBuffer
			w := contextWarningWorker(t, v, &logs)
			for range 2 * contextWarnMinWindows {
				contextWindow(w, appUser, 10*contextWarnMinCalls, plain)
			}
			if n := noContextWarnings(logs.String()); n != 0 {
				t.Fatalf("warned %d times: %s", n, logs.String())
			}
		})
	}

	t.Run("never-matching regexes never warn", func(t *testing.T) {
		var logs syncBuffer
		off := regexp.MustCompile(`a^`)
		w := New(Config{ReController: off, ReAction: off, ReJobTag: off, Logger: slog.New(slog.NewTextHandler(&logs, nil))}, RealClock{})
		w.observedConnected(180000, observerUser)
		for range 2 * contextWarnMinWindows {
			contextWindow(w, appUser, 10*contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 {
			t.Fatalf("warned %d times: %s", n, logs.String())
		}
	})

	t.Run("nil regexes never warn", func(t *testing.T) {
		var logs syncBuffer
		w := New(Config{Logger: slog.New(slog.NewTextHandler(&logs, nil))}, RealClock{})
		w.observedConnected(180000, observerUser)
		for range 2 * contextWarnMinWindows {
			contextWindow(w, appUser, 10*contextWarnMinCalls, plain)
		}
		if n := noContextWarnings(logs.String()); n != 0 {
			t.Fatalf("warned %d times: %s", n, logs.String())
		}
	})
}

func TestNeverMatches(t *testing.T) {
	for _, tc := range []struct {
		re    string
		never bool
	}{
		{`a^`, true},
		{`x$y`, true},
		{`a\A`, true},
		{`\zb`, true},
		{`[^\x00-\x{10FFFF}]`, true},
		{`(a^|b^)`, true},
		{`$^`, false},
		{`^$`, false},
		{`(a^|b)`, false},
		{`(?m)a^`, false}, // can't match either, but line anchors are left to chance
		{sampleController, false},
		{sampleAction, false},
		{sampleJob, false},
		{`.*`, false},
	} {
		if got := neverMatches(regexp.MustCompile(tc.re)); got != tc.never {
			t.Errorf("neverMatches(%q) = %v, want %v", tc.re, got, tc.never)
		}
	}
	if !neverMatches(nil) {
		t.Error("neverMatches(nil) = false, want true")
	}
}
