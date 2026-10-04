package docscheck

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// goTestTimeout dry-runs `make test` with the given extra make arguments and
// returns the timeout the Docker go test command ends up with: the last
// -timeout flag, since go test lets a later flag override an earlier one.
func goTestTimeout(t *testing.T, makeArgs ...string) string {
	t.Helper()
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not installed")
	}
	cmd := exec.Command("make", append([]string{"-n", "--no-print-directory", "test"}, makeArgs...)...)
	cmd.Dir = Root(t)
	for _, kv := range os.Environ() {
		switch strings.SplitN(kv, "=", 2)[0] {
		case "MAKEFLAGS", "MFLAGS", "MAKELEVEL", "GNUMAKEFLAGS", "GO_TEST_ARGS", "GO_TEST_TIMEOUT":
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("make -n test: %v\n%s", err, out)
	}
	var line string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.Contains(l, " go test ") {
			if line != "" {
				t.Fatalf("make -n test printed more than one go test command:\n%s", out)
			}
			line = l
		}
	}
	if line == "" {
		t.Fatalf("make -n test printed no go test command:\n%s", out)
	}
	fields := strings.Fields(line[strings.Index(line, " go test ")+len(" go test "):])
	timeout := ""
	for i, f := range fields {
		switch {
		case f == "-timeout" || f == "--timeout":
			if i+1 < len(fields) {
				timeout = fields[i+1]
			}
		case strings.HasPrefix(f, "-timeout="), strings.HasPrefix(f, "--timeout="):
			timeout = f[strings.Index(f, "=")+1:]
		}
	}
	return timeout
}

// go test's default 10-minute timeout is too short for internal/ingest when
// Docker is under heavy load, so make test sets its own.
func TestMakeTestSetsTimeout(t *testing.T) {
	if got := goTestTimeout(t); got != "30m" {
		t.Errorf("make test: go test -timeout = %q, want 30m", got)
	}
}

func TestMakeTestKeepsTimeoutWithGoTestArgs(t *testing.T) {
	if got := goTestTimeout(t, "GO_TEST_ARGS=-count=1"); got != "30m" {
		t.Errorf("make test GO_TEST_ARGS=-count=1: go test -timeout = %q, want 30m", got)
	}
}

func TestMakeTestTimeoutOverrides(t *testing.T) {
	if got := goTestTimeout(t, "GO_TEST_ARGS=-count=1 -timeout=5m"); got != "5m" {
		t.Errorf("GO_TEST_ARGS -timeout: effective timeout = %q, want 5m", got)
	}
	if got := goTestTimeout(t, "GO_TEST_TIMEOUT=45m"); got != "45m" {
		t.Errorf("GO_TEST_TIMEOUT=45m: effective timeout = %q, want 45m", got)
	}
}

func TestGoTestTimeoutDocumented(t *testing.T) {
	RequireDocumented(t, "docs/building.md", "make test variables", []string{"GO_TEST_TIMEOUT", "GO_TEST_ARGS"})
}
