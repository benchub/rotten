//go:build perf

package reports_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// perfLoadSeconds bounds the load container's life: it kills itself after
// this long even if the test's cleanup never runs.
const perfLoadSeconds = 1200

// startPerfLoad starts a container running busy loops, factor per CPU the
// test sees (the Docker VM's CPUs, since the suite runs in Docker), for at
// most perfLoadSeconds. It's terminated on cleanup, pass or fail.
func startPerfLoad(t *testing.T, factor int) int {
	t.Helper()
	loops := runtime.NumCPU() * factor
	script := fmt.Sprintf("for i in $(seq %d); do yes > /dev/null & done; wait", loops)
	ctx := context.Background()
	c, err := testcontainers.Run(ctx, testdb.RottenImage,
		testcontainers.WithEntrypoint("timeout", "-s", "KILL", strconv.Itoa(perfLoadSeconds), "sh", "-c", script),
		testcontainers.WithCmd(),
		// Docker gives each container an equal CPU weight, so loops in one
		// container would only take half the CPUs from Postgres, however
		// many there are. Weighting the container by its loops makes each
		// loop compete like a busy container of its own, as on the shared
		// machine where the reports were canceled.
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.CPUShares = int64(1024 * loops)
		}),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start load container: %v", err)
	}
	return loops
}

// perfWorkMemOverride, from ROTTEN_PERF_WORK_MEM, sets work_mem inside the
// suite's report transactions, to compare settings. The UI uses the server's.
var perfWorkMemOverride = os.Getenv("ROTTEN_PERF_WORK_MEM")

// perfLoadFactor is the busy loops per CPU, from ROTTEN_PERF_LOAD_FACTOR
// (default 6, 0 for none). At 6 on 10 CPUs, with each loop weighted like a
// container, the 7d match reports' generic plans were canceled in nearly
// every run and the custom ones took 6.5-9.6s at the default 4MB work_mem, as on
// the shared machine at load average 30-69 where they were first canceled.
func perfLoadFactor(t *testing.T) int {
	s := os.Getenv("ROTTEN_PERF_LOAD_FACTOR")
	if s == "" {
		return 6
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		t.Fatalf("ROTTEN_PERF_LOAD_FACTOR=%q: want a non-negative integer (0 for no load)", s)
	}
	return n
}

// TestPerfMatchUnderLoad runs the 7d match reports, which read 7 days of a
// cluster's events and contexts, with CPU-bound loops alongside, and
// requires each to finish under the UI's statement_timeout with the plans
// the UI runs: custom ones, since ReportRunner forces them (task
// 20261005-123457-2). TestPerfReports still measures generic plans.
func TestPerfMatchUnderLoad(t *testing.T) {
	db := testdb.StartRotten(t)
	f := seedPerf(t, db, false)
	conn := db.Connect(t)
	ui := db.ConnectAs(t, testdb.UIRole)
	partitionsOf := perfPartitionsOf(t, conn)
	var cases []perfCase
	for _, c := range perfCases(f) {
		if strings.HasSuffix(c.name, " 7d match") && !strings.HasPrefix(c.name, "replica_utilization") {
			cases = append(cases, c)
		}
	}
	if len(cases) != 3 {
		t.Fatalf("got %d 7d match cases, want 3", len(cases))
	}
	loops := 0
	if factor := perfLoadFactor(t); factor > 0 {
		loops = startPerfLoad(t, factor)
	}
	t.Logf("load: %d busy loops on %d CPUs", loops, runtime.NumCPU())
	var lines []string
	for _, c := range cases {
		r := runPerfCase(t, ui, c, "force_custom_plan", partitionsOf)
		if !r.timedOut {
			assertPerfPruning(t, conn, c, r)
		}
		lines = append(lines, perfLine(c, "force_custom_plan", r))
	}
	t.Logf("results under load:\n%s", strings.Join(lines, "\n"))
	if os.Getenv("ROTTEN_PERF_EXPLAIN") != "" {
		logPerfTextPlans(t, ui, cases)
	}
}

// logPerfTextPlans logs each case's EXPLAIN (ANALYZE, BUFFERS) in text, for
// finding the nodes that degrade under load.
func logPerfTextPlans(t *testing.T, conn perfConn, cases []perfCase) {
	t.Helper()
	ctx := context.Background()
	for _, c := range cases {
		for _, mode := range perfModes {
			query, err := os.ReadFile(c.file)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, "prepare perf_explain as "+string(query)); err != nil {
				t.Fatal(err)
			}
			var lits []string
			for _, a := range c.args {
				lits = append(lits, perfLiteral(a))
			}
			tx, err := beginReportTx(ctx, conn, "60s")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "select set_config('plan_cache_mode', $1, true)", mode); err != nil {
				t.Fatal(err)
			}
			if perfWorkMemOverride != "" {
				if _, err := tx.Exec(ctx, "select set_config('work_mem', $1, true)", perfWorkMemOverride); err != nil {
					t.Fatal(err)
				}
			}
			rows, err := tx.Query(ctx, "explain (analyze, buffers, settings) execute perf_explain("+strings.Join(lits, ", ")+")")
			if err != nil {
				t.Fatal(err)
			}
			var b strings.Builder
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatal(err)
				}
				b.WriteString(line + "\n")
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("explain %s (%s): %v", c.name, mode, err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, "deallocate perf_explain"); err != nil {
				t.Fatal(err)
			}
			t.Logf("plan %s (%s):\n%s", c.name, mode, b.String())
		}
	}
}
