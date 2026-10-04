package testdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type fakeContainer struct {
	testcontainers.Container
	terminated int
}

func (f *fakeContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	f.terminated++
	return nil
}

type recordingLogger struct{ lines []string }

func (l *recordingLogger) Logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

var inspectTimeout = fmt.Errorf("generic container: start container: wait until ready: mapped port: retries: 30, port: \"invalid port\", last err: inspect: %w", context.DeadlineExceeded)

func TestRunContainerRetriesOnceOnDockerTimeout(t *testing.T) {
	first := &fakeContainer{}
	second := &fakeContainer{}
	attempts := 0
	log := &recordingLogger{}

	c, err := runContainerWithRetry(context.Background(), log, "postgres:18", func(context.Context) (testcontainers.Container, error) {
		attempts++
		if attempts == 1 {
			return first, inspectTimeout
		}
		return second, nil
	})
	if err != nil {
		t.Fatalf("runContainerWithRetry: %v", err)
	}
	if c != second {
		t.Fatalf("container = %v, want the second attempt's container", c)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if first.terminated != 1 {
		t.Fatalf("failed first container terminated %d times, want 1", first.terminated)
	}
	if second.terminated != 0 {
		t.Fatalf("returned container terminated %d times, want 0", second.terminated)
	}
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], "retrying") || !strings.Contains(log.lines[0], "postgres:18") {
		t.Fatalf("log lines = %q, want one retry line naming the image", log.lines)
	}
}

func TestRunContainerGivesUpAfterSecondDockerTimeout(t *testing.T) {
	var made []*fakeContainer
	log := &recordingLogger{}

	_, err := runContainerWithRetry(context.Background(), log, "postgres:18", func(context.Context) (testcontainers.Container, error) {
		c := &fakeContainer{}
		made = append(made, c)
		return c, inspectTimeout
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the second attempt's deadline error", err)
	}
	if len(made) != 2 {
		t.Fatalf("attempts = %d, want 2", len(made))
	}
	if made[0].terminated != 1 {
		t.Fatalf("first container terminated %d times, want 1", made[0].terminated)
	}
	if made[1].terminated != 0 {
		t.Fatalf("last container terminated %d times, want 0 (caller's cleanup owns it)", made[1].terminated)
	}
}

func TestRunContainerDoesNotRetryOtherErrors(t *testing.T) {
	attempts := 0
	log := &recordingLogger{}
	notFound := errors.New("pull access denied for rotten-db-test, repository does not exist")

	_, err := runContainerWithRetry(context.Background(), log, RottenImage, func(context.Context) (testcontainers.Container, error) {
		attempts++
		return nil, notFound
	})
	if !errors.Is(err, notFound) {
		t.Fatalf("err = %v, want %v", err, notFound)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if len(log.lines) != 0 {
		t.Fatalf("log lines = %q, want none", log.lines)
	}
}

func TestRunContainerDoesNotRetryStartupWaitDeadline(t *testing.T) {
	attempts := 0
	log := &recordingLogger{}
	waitDeadline := fmt.Errorf("generic container: start container: wait until ready: log message wait: %w", context.DeadlineExceeded)

	_, err := runContainerWithRetry(context.Background(), log, "postgres:18", func(context.Context) (testcontainers.Container, error) {
		attempts++
		return &fakeContainer{}, waitDeadline
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the wait deadline", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestIsDockerTimeout(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{inspectTimeout, true},
		// Some Docker client paths flatten the error to text.
		{errors.New("mapped port: retries: 3, port: \"\", last err: inspect abc: context deadline exceeded"), true},
		{errors.New("detect internal port: Get \"http://docker/containers/abc/json\": net/http: request canceled (Client.Timeout exceeded while awaiting headers)"), true},
		// A startup-wait deadline (Postgres never got ready) isn't a Docker
		// inspect timeout; retrying it would double a ~3 minute failure.
		{fmt.Errorf("wait until ready: log message wait: %w", context.DeadlineExceeded), false},
		{errors.New("wait until ready: context deadline exceeded"), false},
		{errors.New("mapped port: retries: 30, port: \"5432/tcp\", last err: port not found"), false},
		{errors.New("No such image: rotten-db-test:18"), false},
		{nil, false},
	} {
		if got := isDockerTimeout(tc.err); got != tc.want {
			t.Errorf("isDockerTimeout(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
