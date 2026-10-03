package ingest

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type retryableErr struct{}

func (retryableErr) Error() string     { return "retryable before write" }
func (retryableErr) SafeToRetry() bool { return true }

type networkErr struct{}

func (networkErr) Error() string   { return "network failed" }
func (networkErr) Timeout() bool   { return false }
func (networkErr) Temporary() bool { return false }

func TestIsUnavailableClassifiesRetryableDatabaseFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "begin register", err: registerError{op: "begin", err: errors.New("closed")}},
		{name: "begin register canceled", err: registerError{op: "begin", err: context.Canceled}},
		{name: "commit submit", err: submitError{op: "commit", err: errors.New("closed")}},
		{name: "pgconn safe to retry", err: registerError{op: "upsert logical source", err: retryableErr{}}},
		{name: "net error", err: submitError{op: "insert batch", err: networkErr{}}},
		{name: "eof", err: io.EOF},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF},
		{name: "connection reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
		{name: "pgconn closed", err: pgconn.ErrConnClosed},
		{name: "connection closed text", err: errors.New("read tcp 127.0.0.1:5432: use of closed network connection")},
		{name: "caller canceled", err: context.Canceled},
		{name: "wrapped caller canceled", err: submitError{op: "insert batch", err: context.Canceled}},
		{name: "context deadline", err: context.DeadlineExceeded},
		{name: "connection sqlstate", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}},
		{name: "deadlock", err: &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
		{name: "serialization", err: &pgconn.PgError{Code: "40001", Message: "serialization failure"}},
		{name: "admin shutdown", err: &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}},
		{name: "crash shutdown", err: &pgconn.PgError{Code: "57P02", Message: "crash shutdown"}},
		{name: "cannot connect now", err: &pgconn.PgError{Code: "57P03", Message: "cannot connect now"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !isUnavailable(tc.err) {
				t.Fatalf("isUnavailable(%T %[1]v) = false, want true", tc.err)
			}
		})
	}
}

func TestIsUnavailableLeavesApplicationAndCallerCancelErrorsInternal(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "permission", err: &pgconn.PgError{Code: "42501", Message: "permission denied"}},
		{name: "unique constraint", err: &pgconn.PgError{Code: "23505", Message: "duplicate key"}},
		{name: "check constraint", err: &pgconn.PgError{Code: "23514", Message: "check violation"}},
		{name: "syntax", err: &pgconn.PgError{Code: "42601", Message: "syntax error"}},
		{name: "non-retryable safe to retry interface", err: nonRetryableErr{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if isUnavailable(tc.err) {
				t.Fatalf("isUnavailable(%T %[1]v) = true, want false", tc.err)
			}
		})
	}
}

type nonRetryableErr struct{}

func (nonRetryableErr) Error() string     { return "not retryable" }
func (nonRetryableErr) SafeToRetry() bool { return false }
