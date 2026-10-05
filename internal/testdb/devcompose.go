package testdb

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ComposeStopGrace is how long `docker compose stop` waits after SIGTERM
// before it kills a container: compose's default, as the dev services set
// none.
const ComposeStopGrace = 10 * time.Second

// DevComposeCommand returns service's command from dev/docker-compose.yaml,
// with compose's $$ escapes undone, and its working_dir.
func DevComposeCommand(t testing.TB, service string) ([]string, string) {
	t.Helper()
	svc := decodeDevService[struct {
		Command    []string `yaml:"command"`
		WorkingDir string   `yaml:"working_dir"`
	}](t, service)
	if len(svc.Command) == 0 {
		t.Fatalf("testdb: dev/docker-compose.yaml service %s has no command", service)
	}
	return unescapeCompose(svc.Command), svc.WorkingDir
}

// ComposeProcess is a dev/docker-compose.yaml service's command running as
// a local process, standing in for the container's PID 1.
type ComposeProcess struct {
	Service string
	cmd     *exec.Cmd
	out     syncBuffer
	done    chan error
}

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

// StartDevComposeCommand runs service's compose command from the repository
// root, which the dev stack mounts at its working_dir /src, with env added
// to the test's environment. replace holds old, new pairs applied to every
// argument, for container paths and addresses that don't exist here. The
// process gets its own process group, which cleanup kills, so nothing it
// leaves behind, like a `go run` child, outlives the test.
func StartDevComposeCommand(t testing.TB, service string, env []string, replace ...string) *ComposeProcess {
	t.Helper()
	args, workDir := DevComposeCommand(t, service)
	if workDir != "/src" {
		t.Fatalf("testdb: %s working_dir = %q, want /src (the repository mount)", service, workDir)
	}
	if len(replace) > 0 {
		r := strings.NewReplacer(replace...)
		for i, a := range args {
			args[i] = r.Replace(a)
		}
	}
	p := &ComposeProcess{Service: service, done: make(chan error, 1)}
	p.cmd = exec.Command(args[0], args[1:]...)
	p.cmd.Dir = RepoRoot()
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout, p.cmd.Stderr = &p.out, &p.out
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("testdb: start %s: %v", service, err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) })
	return p
}

// Logs returns the process's combined output so far.
func (p *ComposeProcess) Logs() string { return p.out.String() }

// WaitForLog waits up to d for the output to contain what, failing the test
// if the process exits first.
func (p *ComposeProcess) WaitForLog(t testing.TB, what string, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for !strings.Contains(p.Logs(), what) {
		select {
		case err := <-p.done:
			p.done <- err
			t.Fatalf("%s exited (%v) before logging %q:\n%s", p.Service, err, what, p.Logs())
		case <-ctx.Done():
			t.Fatalf("%s didn't log %q within %v:\n%s", p.Service, what, d, p.Logs())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ComposeStop does what `docker compose stop` does: SIGTERM to the process
// it started (the container's PID 1) only. The process must exit 0 within
// ComposeStopGrace. It returns the output.
func (p *ComposeProcess) ComposeStop(t testing.TB) string {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("testdb: SIGTERM %s: %v", p.Service, err)
	}
	select {
	case err := <-p.done:
		p.done <- err
		if err != nil {
			t.Fatalf("%s exited with %v after SIGTERM, want 0:\n%s", p.Service, err, p.Logs())
		}
	case <-time.After(ComposeStopGrace):
		t.Fatalf("%s still running %v after SIGTERM:\n%s", p.Service, ComposeStopGrace, p.Logs())
	}
	return p.Logs()
}
