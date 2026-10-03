package release_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReleaseArtifactsSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("release smoke test skipped under -short")
	}
	if os.Getenv("ROTTEN_RELEASE_SMOKE") != "1" {
		t.Skip("release smoke test runs through make test-release")
	}
	repo := testdb.RepoRoot()
	assertDockerignoreExcludes(t, repo, ".claude/worktrees/other-task/file.go")
	assertDockerignoreExcludes(t, repo, "dist/release-smoke/native/rotten-worker")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	workerImage := "rotten-worker-smoke-" + suffix + ":test"
	serverImage := "rotten-server-smoke-" + suffix + ":test"
	t.Cleanup(func() {
		removeDockerImage(t, workerImage)
		removeDockerImage(t, serverImage)
	})
	distDir := filepath.Join("dist", "release-smoke")
	runHost(t, repo, "make", "build-smoke", "VERSION=smoke", "COMMIT=smoke", "BUILD_DATE=1970-01-01T00:00:00Z", "DIST_DIR="+distDir, "WORKER_IMAGE="+workerImage, "SERVER_IMAGE="+serverImage)

	for _, bin := range []string{"rotten-worker", "rotten-server"} {
		path := filepath.Join(repo, distDir, "native", bin)
		out := runHost(t, repo, path, "--version")
		if !strings.Contains(out, "smoke") {
			t.Fatalf("%s --version did not include version metadata: %q", bin, out)
		}
		runHost(t, repo, path, "--help")
	}
	for _, arch := range []string{"amd64", "arm64"} {
		path := filepath.Join(repo, distDir, "linux", arch, "rotten-server")
		if info, err := os.Stat(path); err != nil {
			t.Fatalf("linux/%s/rotten-server was not built: %v", arch, err)
		} else if info.Size() == 0 {
			t.Fatalf("linux/%s/rotten-server is empty", arch)
		}
	}
	workerPath := filepath.Join(repo, distDir, "linux", runtime.GOARCH, "rotten-worker")
	if info, err := os.Stat(workerPath); err != nil {
		t.Fatalf("native-platform Linux rotten-worker was not built: %v", err)
	} else if info.Size() == 0 {
		t.Fatal("native-platform Linux rotten-worker is empty")
	}

	ctx := context.Background()
	for _, img := range []string{workerImage, serverImage} {
		out := runContainer(t, ctx, img, nil, []string{"--version"}, nil)
		if !strings.Contains(out, "smoke") {
			t.Fatalf("%s --version did not include version metadata: %q", img, out)
		}
		runContainer(t, ctx, img, nil, []string{"--help"}, nil)
	}
	runContainer(t, ctx, workerImage, []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("test"),
	}, []string{"-w", "/var/lib/rotten-worker"}, nil)
	runContainer(t, ctx, workerImage, []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("test"),
	}, []string{"-d", "/etc/rotten-worker"}, nil)

	topology := testdb.StartTopology(t)
	db := topology.StartRottenEmpty(t)
	runContainer(t, ctx, serverImage, []testcontainers.ContainerCustomizer{
		topology.ServerNetworkOptions("rotten-server-migrate")[1],
	}, []string{"migrate"}, map[string]string{
		"ROTTEN_OWNER_DSN": topology.RottenInternalDSN(testdb.OwnerRole),
	})
	rows := db.QueryInContainer(t, "postgres", "select count(*) from public.goose_db_version")
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == "0" {
		t.Fatalf("server image migrate did not apply migrations; rows=%v", rows)
	}
}

func assertDockerignoreExcludes(t *testing.T, repo string, name string) {
	t.Helper()
	patterns, err := os.ReadFile(filepath.Join(repo, ".dockerignore"))
	if err != nil {
		t.Fatalf("read .dockerignore: %v", err)
	}
	if !dockerignoreExcludes(string(patterns), name) {
		t.Fatalf(".dockerignore does not exclude %s", name)
	}
}

func dockerignoreExcludes(patterns string, name string) bool {
	excluded := false
	for _, line := range strings.Split(patterns, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		include := strings.HasPrefix(line, "!")
		pattern := strings.TrimPrefix(line, "!")
		pattern = strings.TrimSuffix(pattern, "/")
		matched := false
		switch {
		case pattern == "*":
			matched = true
		case strings.HasSuffix(pattern, "/**"):
			matched = strings.HasPrefix(name, strings.TrimSuffix(pattern, "**"))
		default:
			matched = name == pattern || strings.HasPrefix(name, pattern+"/")
		}
		if matched {
			excluded = !include
		}
	}
	return excluded
}

func runHost(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func removeDockerImage(t *testing.T, image string) {
	t.Helper()
	cmd := exec.Command("docker", "image", "rm", image)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("cleanup docker image %s: %v\n%s", image, err, out)
	}
}

func runContainer(t *testing.T, ctx context.Context, image string, customizers []testcontainers.ContainerCustomizer, cmd []string, env map[string]string) string {
	t.Helper()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithCmdArgs(cmd...),
		testcontainers.WithEnv(env),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(2 * time.Minute)),
	}
	opts = append(opts, customizers...)
	c, err := testcontainers.Run(ctx, image, opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("run %s %v: %v", image, cmd, err)
	}
	state, err := c.State(ctx)
	if err != nil {
		t.Fatalf("state %s %v: %v", image, cmd, err)
	}
	logs, logErr := c.Logs(ctx)
	var b bytes.Buffer
	if logErr == nil {
		_, _ = io.Copy(&b, logs)
		_ = logs.Close()
	}
	if state.ExitCode != 0 {
		t.Fatalf("run %s %v exited %d\n%s", image, cmd, state.ExitCode, b.String())
	}
	return b.String()
}
