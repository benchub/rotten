package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/benchub/rotten/internal/testdb"
)

// stopGrace is how long `docker compose stop` waits after SIGTERM before it
// kills the container: compose's default, as the traffic service sets none.
const stopGrace = 10 * time.Second

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// composeTrafficCommand is the traffic service's command and working
// directory from dev/docker-compose.yaml, with compose's $$ escapes undone.
func composeTrafficCommand(t *testing.T) ([]string, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "dev", "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Command    []string `yaml:"command"`
			WorkingDir string   `yaml:"working_dir"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	svc, ok := compose.Services["traffic"]
	if !ok || len(svc.Command) == 0 {
		t.Fatal("dev/docker-compose.yaml has no traffic service command")
	}
	cmd := make([]string, len(svc.Command))
	for i, a := range svc.Command {
		cmd[i] = strings.ReplaceAll(a, "$$", "$")
	}
	return cmd, svc.WorkingDir
}

// TestComposeStopShutsDownCleanly runs the compose service's own command and
// does what `docker compose stop` does: SIGTERM to the process it started
// (the container's PID 1) only. The generator must get the signal, log its
// shutdown, and exit 0 within the grace period, during a lock_wait episode
// so lock holders are mid-transaction.
func TestComposeStopShutsDownCleanly(t *testing.T) {
	db := testdb.StartObserved(t, 18)
	args, workDir := composeTrafficCommand(t)
	if workDir != "/src" {
		t.Fatalf("traffic working_dir = %q, want /src (the repository mount)", workDir)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = testdb.RepoRoot()
	cmd.Env = append(os.Environ(),
		"TRAFFIC_ADMIN_DSN="+db.DSN,
		"TRAFFIC_SHARDS=1",
		"TRAFFIC_SCALE=0.05",
		"TRAFFIC_RATE=5",
		"TRAFFIC_EPISODE=lock_wait",
	)
	var out syncBuffer
	cmd.Stdout, cmd.Stderr = &out, &out
	// Its own process group, so cleanup can kill anything the command
	// leaves behind, like a `go run` child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })

	waitFor := func(what string, d time.Duration) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		for !strings.Contains(out.String(), what) {
			select {
			case err := <-done:
				t.Fatalf("traffic exited (%v) before logging %q:\n%s", err, what, out.String())
			case <-ctx.Done():
				t.Fatalf("traffic didn't log %q within %v:\n%s", what, d, out.String())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	// The first start may compile.
	waitFor("lock_wait episode for the whole run", 4*time.Minute)
	time.Sleep(3 * time.Second)

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("traffic exited with %v after SIGTERM, want 0:\n%s", err, out.String())
		}
	case <-time.After(stopGrace):
		t.Fatalf("traffic still running %v after SIGTERM:\n%s", stopGrace, out.String())
	}
	log := out.String()
	if !strings.Contains(log, "devtraffic: stopped after") {
		t.Fatalf("no shutdown log line after SIGTERM:\n%s", log)
	}
	if !strings.Contains(log, " 0 errors") {
		t.Errorf("shutdown wasn't clean:\n%s", log)
	}
}
