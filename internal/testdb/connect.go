package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// newSuperuserPassword returns a random password for one container's postgres
// superuser. Host ports are recycled quickly under load, and Docker Desktop's
// port forwarding can briefly route a fresh port to another container; a
// unique password turns that into an auth failure instead of a silent
// connection to the wrong database.
func newSuperuserPassword() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("testdb: random password: %v", err))
	}
	return hex.EncodeToString(b)
}

// retryableConnectError reports whether a failed connect may have reached the
// wrong or a not-yet-routed container, so retrying after re-reading the mapped
// port can help.
func retryableConnectError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01", "28000", "08P01", "57P01", "57P02", "57P03":
			return true
		}
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	return strings.Contains(err.Error(), "unexpected EOF")
}

// connectRerouted connects once, and on a retryable failure re-verifies the
// route to the container (reverify may refresh dsn) and tries exactly once
// more. A nil reverify means no retry.
func connectRerouted[T any](ctx context.Context, dsn func() string, connect func(context.Context, string) (T, error), reverify func() error) (T, error) {
	v, err := connect(ctx, dsn())
	if err == nil || reverify == nil || !retryableConnectError(err) {
		return v, err
	}
	if verr := reverify(); verr != nil {
		return v, fmt.Errorf("%w (after connect error: %v)", verr, err)
	}
	return connect(ctx, dsn())
}

func pgxPing(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	return conn.Close(ctx)
}

// connectVerified re-reads the mapped port before each attempt and returns the
// first DSN connect accepts. It retries retryableConnectError failures until
// timeout and gives up at once on anything else.
func connectVerified(ctx context.Context, resolver hostPortResolver, dbName, password string, timeout, pollInterval time.Duration, connect func(context.Context, string) error) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		dsn, err := hostDSN(ctx, resolver, dbName, password)
		if err == nil {
			err = connect(ctx, dsn)
			if err == nil {
				return dsn, nil
			}
			if !retryableConnectError(err) && ctx.Err() == nil {
				return "", err
			}
		}
		if lastErr == nil || ctx.Err() == nil {
			lastErr = err
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("host DSN did not become connectable after %s: %w", timeout, lastErr)
		case <-timer.C:
		}
	}
}
