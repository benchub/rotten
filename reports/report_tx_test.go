package reports_test

import (
	"context"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// reportConn is a *pgx.Conn, or a pgx.Tx whose Begin makes a savepoint.
type reportConn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// beginReportTx opens a transaction set up the way the UI's ReportRunner
// sets up a report's: read only, with a transaction-local statement_timeout,
// JIT off and custom plans forced. JIT compilation costs more than it saves
// on the reports, and generic plans are what hit the timeout first on a busy
// server (see docs/perf.md). The perf suite measures what the UI runs; it
// sets plan_cache_mode again inside the transaction to also measure generic
// plans.
func beginReportTx(ctx context.Context, conn reportConn, timeout string) (pgx.Tx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "set transaction read only"); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	if _, err := tx.Exec(ctx, "select set_config('statement_timeout', $1, true)", timeout); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	if _, err := tx.Exec(ctx, "select set_config('jit', 'off', true)"); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	if _, err := tx.Exec(ctx, "select set_config('plan_cache_mode', 'force_custom_plan', true)"); err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func reportSettings(t *testing.T, ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (jit, timeout, readOnly string) {
	t.Helper()
	if err := q.QueryRow(ctx, "select current_setting('jit'), current_setting('statement_timeout'), current_setting('transaction_read_only')").
		Scan(&jit, &timeout, &readOnly); err != nil {
		t.Fatal(err)
	}
	return jit, timeout, readOnly
}

// Reports run with JIT off, as in the UI, and the settings end with the
// transaction.
func TestBeginReportTxSettings(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartRotten(t)
	conn := db.ConnectAs(t, testdb.UIRole)

	jitBefore, timeoutBefore, _ := reportSettings(t, ctx, conn)
	if jitBefore != "on" {
		t.Fatalf("jit is %q before the report; want the server default on, or this test proves nothing", jitBefore)
	}

	tx, err := beginReportTx(ctx, conn, "1234ms")
	if err != nil {
		t.Fatal(err)
	}
	jit, timeout, readOnly := reportSettings(t, ctx, tx)
	if jit != "off" || timeout != "1234ms" || readOnly != "on" {
		t.Errorf("inside the report transaction: jit=%s statement_timeout=%s read_only=%s; want off, 1234ms, on", jit, timeout, readOnly)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	jit, timeout, _ = reportSettings(t, ctx, conn)
	if jit != jitBefore || timeout != timeoutBefore {
		t.Errorf("after the report transaction: jit=%s statement_timeout=%s; want %s, %s", jit, timeout, jitBefore, timeoutBefore)
	}
}

// Reports run with custom plans forced, as in the UI (docs/perf.md, "Under
// load"), at the server's work_mem, and the setting ends with the
// transaction.
func TestBeginReportTxPlanCacheMode(t *testing.T) {
	ctx := context.Background()
	db := testdb.StartRotten(t)
	conn := db.ConnectAs(t, testdb.UIRole)
	setting := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}, name string) string {
		var v string
		if err := q.QueryRow(ctx, "select current_setting($1)", name).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	before, workMemBefore := setting(conn, "plan_cache_mode"), setting(conn, "work_mem")
	if before != "auto" {
		t.Fatalf("plan_cache_mode is %s before the report; want the server default auto, or this test proves nothing", before)
	}
	tx, err := beginReportTx(ctx, conn, "1s")
	if err != nil {
		t.Fatal(err)
	}
	if got := setting(tx, "plan_cache_mode"); got != "force_custom_plan" {
		t.Errorf("plan_cache_mode inside the report transaction is %s; want force_custom_plan", got)
	}
	if got := setting(tx, "work_mem"); got != workMemBefore {
		t.Errorf("work_mem inside the report transaction is %s; want the server's %s", got, workMemBefore)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := setting(conn, "plan_cache_mode"); got != before {
		t.Errorf("plan_cache_mode after the report transaction is %s; want %s", got, before)
	}
}
