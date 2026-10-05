package testdb

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// TestDevComposeServicesDontGoRun checks no dev/docker-compose.yaml service
// command uses `go run`, which doesn't pass compose's SIGTERM on to the
// program it runs. Each Go program is built to a binary under /tmp and run
// from there; a long-running one is exec'd, so it's PID 1 and gets the
// signal. Healthchecks aren't PID 1, so they're not checked.
func TestDevComposeServicesDontGoRun(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "dev", "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	goRun := regexp.MustCompile(`\bgo\s+run\b`)
	for name, svc := range compose.Services {
		cmd := strings.Join(svc.Command, " ")
		if goRun.MatchString(cmd) || (len(svc.Command) > 1 && svc.Command[0] == "go" && svc.Command[1] == "run") {
			t.Errorf("%s runs `go run`; build the binary and exec it instead: %q", name, svc.Command)
		}
	}
	buildExec := regexp.MustCompile(`^go build -o (/tmp/\S+) \S+ && exec (/tmp/\S+)(\s|$)`)
	for _, name := range []string{"server", "worker", "worker-replica", "traffic"} {
		cmd := compose.Services[name].Command
		var m []string
		if len(cmd) == 3 && cmd[0] == "sh" && cmd[1] == "-ec" {
			m = buildExec.FindStringSubmatch(cmd[2])
		}
		if m == nil || m[1] != m[2] {
			t.Errorf("%s command = %q, want sh -ec 'go build -o /tmp/<name> ... && exec /tmp/<name> ...'", name, cmd)
		}
	}
}
