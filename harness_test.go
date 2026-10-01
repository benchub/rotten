package main

import (
	"os"
	"testing"
)

// TestHarnessRuns proves the suite runs inside the Docker test container.
// docker/test.Dockerfile sets ROTTEN_TEST_IN_DOCKER=1. make test-unit runs
// natively with -short, so the check applies only to the full suite.
func TestHarnessRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short runs natively; the Docker check applies to make test")
	}
	if got := os.Getenv("ROTTEN_TEST_IN_DOCKER"); got != "1" {
		t.Fatalf("ROTTEN_TEST_IN_DOCKER = %q, want \"1\"; run tests with `make test`", got)
	}
}
