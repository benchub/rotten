package testdb

import (
	"context"
	"errors"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

type logger interface {
	Logf(format string, args ...any)
}

// runContainerWithRetry runs run, and if it fails because testcontainers'
// mapped-port inspect timed out (Docker too busy to answer while the whole
// suite starts containers in parallel), terminates whatever the failed attempt
// left behind and tries exactly once more. Other errors, such as a missing
// image or Postgres never becoming ready, aren't retried.
// The caller owns cleanup of the container it gets back, even on error.
func runContainerWithRetry(ctx context.Context, log logger, image string, run func(context.Context) (testcontainers.Container, error)) (testcontainers.Container, error) {
	c, err := run(ctx)
	if err == nil || !isDockerTimeout(err) {
		return c, err
	}
	log.Logf("testdb: start %s hit a Docker timeout, retrying once: %v", image, err)
	if terr := testcontainers.TerminateContainer(c); terr != nil {
		log.Logf("testdb: terminate failed %s container: %v", image, terr)
	}
	return run(ctx)
}

// isDockerTimeout reports whether err is testcontainers' port-inspect wait
// timing out because Docker was too slow to answer, rather than a real startup
// failure. A plain startup-wait deadline (Postgres never got ready) doesn't
// count: retrying it would double a ~3 minute failure and risk the package's
// test timeout.
func isDockerTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	timedOut := errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "Client.Timeout exceeded")
	portInspect := strings.Contains(msg, "mapped port:") ||
		strings.Contains(msg, "detect internal port:")
	return timedOut && portInspect
}
