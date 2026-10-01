package main

import (
	"os"
	"testing"
)

// TestHarnessRuns proves the suite runs inside the Docker test container.
// docker/test.Dockerfile sets ROTTEN_TEST_IN_DOCKER=1.
func TestHarnessRuns(t *testing.T) {
	if got := os.Getenv("ROTTEN_TEST_IN_DOCKER"); got != "1" {
		t.Fatalf("ROTTEN_TEST_IN_DOCKER = %q, want \"1\"; run tests with `make test`", got)
	}
}
