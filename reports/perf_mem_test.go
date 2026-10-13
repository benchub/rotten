//go:build perf

package reports_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// perfMemSampleEvery is how often perfPeakMemory asks the report's backend
// to log its memory contexts.
const perfMemSampleEvery = 50 * time.Millisecond

var (
	perfGrandTotal   = regexp.MustCompile(`Grand total: (\d+) bytes`)
	perfContextTotal = regexp.MustCompile(`level: \d+; .*?: (\d+) total in`)
)

// perfMemLogContextOver is the size from which perfPeakMemory logs a memory
// context at the peak, to show what holds the memory.
const perfMemLogContextOver = 4 << 20

// perfMemMinPeak is the smallest peak perfPeakMemory accepts. An idle backend
// logs about 1MB, and the smallest peak seen during a run is about 34MB, so a
// peak under this means no sample landed while the query ran.
const perfMemMinPeak = 8 << 20

// perfPeakMemory runs the case once more on ui, as runPerfCase does, while a
// superuser connection calls pg_log_backend_memory_contexts on ui's backend
// every perfMemSampleEvery. It returns the largest "Grand total" the backend
// logged during the run: its peak private memory, to within the sampling
// interval. Shared buffers aren't in it.
func perfPeakMemory(t *testing.T, db *testdb.DB, ui *pgx.Conn, c perfCase, mode string) int64 {
	t.Helper()
	ctx := context.Background()
	query, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ui.Exec(ctx, "set plan_cache_mode = "+mode); err != nil {
		t.Fatal(err)
	}
	if _, err := ui.Exec(ctx, "prepare perf_mem as "+string(query)); err != nil {
		t.Fatalf("prepare %s: %v", c.file, err)
	}
	defer func() {
		if _, err := ui.Exec(ctx, "deallocate perf_mem"); err != nil {
			t.Fatal(err)
		}
	}()
	var lits []string
	for _, a := range c.args {
		lits = append(lits, perfLiteral(a))
	}
	execute := "execute perf_mem(" + strings.Join(lits, ", ") + ")"

	pid := ui.PgConn().PID()
	sampler := db.Connect(t)
	defer sampler.Close(ctx)
	skip := len(perfContainerLogs(t, db))

	stop, done := make(chan struct{}), make(chan error, 1)
	go func() {
		tick := time.NewTicker(perfMemSampleEvery)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				done <- nil
				return
			case <-tick.C:
				if _, err := sampler.Exec(ctx, "select pg_log_backend_memory_contexts($1)", pid); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	tx, err := beginReportTx(ctx, ui, perfUITimeout)
	if err != nil {
		t.Fatal(err)
	}
	// The UI forces custom plans; set the mode again to measure both.
	if _, err := tx.Exec(ctx, "select set_config('plan_cache_mode', $1, true)", mode); err != nil {
		t.Fatal(err)
	}
	if perfWorkMemOverride != "" {
		if _, err := tx.Exec(ctx, "select set_config('work_mem', $1, true)", perfWorkMemOverride); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := tx.Query(ctx, execute, pgx.QueryExecModeSimpleProtocol)
	if err == nil {
		for rows.Next() {
		}
		rows.Close()
		err = rows.Err()
	}
	_ = tx.Rollback(ctx)
	close(stop)
	if serr := <-done; serr != nil {
		t.Fatalf("%s (%s): sample memory: %v", c.name, mode, serr)
	}
	if err != nil {
		t.Fatalf("%s (%s): %v", c.name, mode, err)
	}
	// The backend logs a request when it next checks for interrupts; give
	// the last one time to reach the log.
	time.Sleep(time.Second)
	logs := perfContainerLogs(t, db)
	var peak int64
	var dump, peakDump []string
	tag := fmt.Sprintf("[%d]", pid)
	for _, line := range strings.Split(logs[min(skip, len(logs)):], "\n") {
		if !strings.Contains(line, tag) {
			continue
		}
		if m := perfGrandTotal.FindStringSubmatch(line); m != nil {
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if n > peak {
				peak, peakDump = n, dump
			}
			dump = nil
			continue
		}
		if m := perfContextTotal.FindStringSubmatch(line); m != nil {
			if n, _ := strconv.ParseInt(m[1], 10, 64); n >= perfMemLogContextOver {
				dump = append(dump, line[strings.Index(line, "level:"):])
			}
		}
	}
	if len(peakDump) > 0 {
		t.Logf("%s (%s): memory contexts over %dMB at the peak:\n%s", c.name, mode, perfMemLogContextOver>>20, strings.Join(peakDump, "\n"))
	}
	if peak == 0 {
		t.Fatalf("%s (%s): no memory context dump from backend %d in the server log", c.name, mode, pid)
	}
	if peak < perfMemMinPeak {
		t.Fatalf("%s (%s): peak memory %d bytes is under %dMB; no sample landed while the query ran", c.name, mode, peak, perfMemMinPeak>>20)
	}
	return peak
}

func perfContainerLogs(t *testing.T, db *testdb.DB) string {
	t.Helper()
	r, err := db.Container().Logs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
