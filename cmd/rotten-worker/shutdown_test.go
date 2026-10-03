package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/state"
)

type fakeShutdownDrainer struct {
	called   chan struct{}
	block    chan struct{}
	deadline chan bool
	err      error
	once     sync.Once
}

func (d *fakeShutdownDrainer) Drain(ctx context.Context) (int, error) {
	d.once.Do(func() { close(d.called) })
	select {
	case <-d.block:
		return 1, d.err
	case <-ctx.Done():
		d.deadline <- errors.Is(ctx.Err(), context.DeadlineExceeded)
		return 0, ctx.Err()
	}
}

type fakeShutdownStore struct {
	closed chan struct{}
}

func (s *fakeShutdownStore) Close() error {
	close(s.closed)
	return nil
}

type fakeShutdownCounts struct {
	queued int
}

func (c *fakeShutdownCounts) OutboxCounts(context.Context) (state.OutboxCounts, error) {
	return state.OutboxCounts{Queued: c.queued}, nil
}

func TestGracefulWorkerShutdownFlushesOutboxWithinDeadlineAndClosesStore(t *testing.T) {
	drainer := &fakeShutdownDrainer{
		called:   make(chan struct{}),
		block:    make(chan struct{}),
		deadline: make(chan bool, 1),
	}
	store := &fakeShutdownStore{closed: make(chan struct{})}
	runDone := make(chan error, 1)
	signals := make(chan os.Signal, 1)
	signals <- os.Interrupt
	runDone <- nil

	start := time.Now()
	shouldExitZero, err := gracefulWorkerShutdown(context.Background(), signals, runDone, func() {}, func() {}, drainer, &fakeShutdownCounts{queued: 1}, store, 50*time.Millisecond, slog.Default())
	if !shouldExitZero {
		t.Fatal("signal shutdown should exit zero even when durable outbox remains queued")
	}
	if err != nil {
		t.Fatalf("shutdown returned %v, want nil", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown did not respect the flush deadline")
	}
	select {
	case <-drainer.called:
	default:
		t.Fatal("outbox was not flushed")
	}
	select {
	case ok := <-drainer.deadline:
		if !ok {
			t.Fatal("drainer did not receive a deadline context")
		}
	default:
		t.Fatal("flush did not end by deadline")
	}
	select {
	case <-store.closed:
	default:
		t.Fatal("state store was not closed")
	}
}

func TestGracefulWorkerShutdownFinishesCurrentRunBeforeFlush(t *testing.T) {
	drainer := &fakeShutdownDrainer{
		called:   make(chan struct{}),
		block:    make(chan struct{}),
		deadline: make(chan bool, 1),
	}
	close(drainer.block)
	store := &fakeShutdownStore{closed: make(chan struct{})}
	runDone := make(chan error, 1)
	signals := make(chan os.Signal, 1)
	signals <- os.Interrupt

	done := make(chan error, 1)
	go func() {
		_, err := gracefulWorkerShutdown(context.Background(), signals, runDone, func() {}, func() {}, drainer, &fakeShutdownCounts{}, store, time.Second, slog.Default())
		done <- err
	}()
	select {
	case <-drainer.called:
		t.Fatal("outbox flushed before the worker finished")
	case <-time.After(50 * time.Millisecond):
	}
	runDone <- nil
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
}

func TestGracefulWorkerShutdownCancelsRunAfterDeadline(t *testing.T) {
	drainer := &fakeShutdownDrainer{
		called:   make(chan struct{}),
		block:    make(chan struct{}),
		deadline: make(chan bool, 1),
	}
	close(drainer.block)
	store := &fakeShutdownStore{closed: make(chan struct{})}
	runDone := make(chan error, 1)
	signals := make(chan os.Signal, 1)
	signals <- os.Interrupt
	cancelled := make(chan struct{})

	_, err := gracefulWorkerShutdown(context.Background(), signals, runDone, func() {}, func() { close(cancelled) }, drainer, &fakeShutdownCounts{}, store, 25*time.Millisecond, slog.Default())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("run context was not cancelled after deadline")
	}
}

func TestGracefulWorkerShutdownSecondSignalCancelsImmediately(t *testing.T) {
	drainer := &fakeShutdownDrainer{
		called:   make(chan struct{}),
		block:    make(chan struct{}),
		deadline: make(chan bool, 1),
	}
	store := &fakeShutdownStore{closed: make(chan struct{})}
	runDone := make(chan error, 1)
	signals := make(chan os.Signal, 2)
	signals <- os.Interrupt
	cancelled := make(chan struct{})
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := gracefulWorkerShutdown(context.Background(), signals, runDone, func() {}, func() { close(cancelled) }, drainer, &fakeShutdownCounts{queued: 1}, store, time.Hour, slog.Default())
		done <- err
	}()
	signals <- os.Interrupt
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown error = %v, want context.Canceled", err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("second signal did not return quickly")
		}
	case <-time.After(time.Second):
		t.Fatal("second signal did not cancel immediately")
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("run context was not cancelled by second signal")
	}
	select {
	case <-drainer.called:
		t.Fatal("forced shutdown should skip blocking flush")
	default:
	}
	select {
	case <-store.closed:
	default:
		t.Fatal("store was not closed after forced shutdown")
	}
}

func TestGracefulWorkerShutdownSignalDuringFlushAborts(t *testing.T) {
	drainer := &fakeShutdownDrainer{
		called:   make(chan struct{}),
		block:    make(chan struct{}),
		deadline: make(chan bool, 1),
	}
	store := &fakeShutdownStore{closed: make(chan struct{})}
	runDone := make(chan error, 1)
	runDone <- nil
	signals := make(chan os.Signal, 2)
	signals <- os.Interrupt
	done := make(chan error, 1)
	go func() {
		_, err := gracefulWorkerShutdown(context.Background(), signals, runDone, func() {}, func() {}, drainer, &fakeShutdownCounts{queued: 1}, store, time.Hour, slog.Default())
		done <- err
	}()
	select {
	case <-drainer.called:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}
	signals <- os.Interrupt
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("signal during flush did not abort")
	}
}

func TestStartupSignalContextCancelsRegistration(t *testing.T) {
	signals := make(chan os.Signal, 1)
	ctx, cancel, done, consumed := signalContext(context.Background(), signals, nil)
	defer cancel()
	signals <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("startup signal did not cancel context")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signal context goroutine did not exit")
	}
	if !consumed() {
		t.Fatal("signal context did not record consumed signal")
	}
	err := ctx.Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("context err = %v, want canceled", err)
	}
}

func TestSignalContextReportsSignalDeliveredJustBeforeCancel(t *testing.T) {
	signals := make(chan os.Signal, 1)
	ctx, cancel, done, consumed := signalContext(context.Background(), signals, nil)
	signals <- os.Interrupt
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signal context goroutine did not exit")
	}
	if !consumed() {
		t.Fatal("signal delivered before cancel was not reported as consumed")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("context err = %v, want canceled", ctx.Err())
	}
}

func TestShutdownExitCodeMapping(t *testing.T) {
	if got := shutdownExitCode(true, nil); got != 0 {
		t.Fatalf("graceful signal exit code = %d, want 0", got)
	}
	if got := shutdownExitCode(true, context.Canceled); got != 1 {
		t.Fatalf("forced signal exit code = %d, want 1", got)
	}
	if got := shutdownExitCode(false, errors.New("boom")); got == 0 {
		t.Fatal("error exit code was zero")
	}
	if got := shutdownExitCode(false, nil); got != 0 {
		t.Fatalf("nil exit code = %d, want 0", got)
	}
}

func TestNoIdleHandsHelpText(t *testing.T) {
	if !strings.Contains(flag.Lookup("noIdleHands").Usage, "watchdog") {
		t.Fatalf("noIdleHands help should describe the watchdog: %q", flag.Lookup("noIdleHands").Usage)
	}
}
