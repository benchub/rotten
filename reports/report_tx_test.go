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
// sets up a report's: read only, with a transaction-local statement_timeout
// and JIT off. JIT compilation costs more than it saves on the reports (see
// docs/perf.md), and the perf suite measures what the UI runs.
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
