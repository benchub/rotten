package testdb

import (
	"context"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.yaml.in/yaml/v3"
)

// ObservedPair is the dev stack's observed primary and its streaming
// replica, run as dev/docker-compose.yaml defines them.
type ObservedPair struct {
	Primary, Replica *DB
}

// devService is the part of a dev/docker-compose.yaml service needed to run
// it as a test container.
type devService struct {
	Image       string            `yaml:"image"`
	Entrypoint  []string          `yaml:"entrypoint"`
	Command     []string          `yaml:"command"`
	Environment map[string]string `yaml:"environment"`
	Volumes     []string          `yaml:"volumes"`
	Networks    map[string]struct {
		Aliases []string `yaml:"aliases"`
	} `yaml:"networks"`
}

func loadDevService(t testing.TB, name string) devService {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "dev", "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("testdb: parse dev/docker-compose.yaml: %v", err)
	}
	node, ok := compose.Services[name]
	if !ok {
		t.Fatalf("testdb: dev/docker-compose.yaml has no %s service", name)
	}
	var svc devService
	if err := node.Decode(&svc); err != nil {
		t.Fatalf("testdb: parse dev/docker-compose.yaml service %s: %v", name, err)
	}
	return svc
}

// customizers turns svc into container options: its entrypoint and command
// (with compose's $$ escapes undone), its environment with overrides
// applied (an empty override removes the variable), its ./ bind mounts as
// copied files, and its observed-network aliases on network.
func (svc devService) customizers(t testing.TB, network *testcontainers.DockerNetwork, env map[string]string) []testcontainers.ContainerCustomizer {
	t.Helper()
	unescape := func(in []string) []string {
		out := make([]string, len(in))
		for i, a := range in {
			out[i] = strings.ReplaceAll(a, "$$", "$")
		}
		return out
	}
	vars := maps.Clone(svc.Environment)
	if vars == nil {
		vars = map[string]string{}
	}
	for k, v := range env {
		if v == "" {
			delete(vars, k)
		} else {
			vars[k] = v
		}
	}
	var files []testcontainers.ContainerFile
	for _, v := range svc.Volumes {
		parts := strings.Split(v, ":")
		if !strings.HasPrefix(parts[0], "./") || len(parts) < 2 {
			continue
		}
		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      filepath.Join(RepoRoot(), "dev", parts[0]),
			ContainerFilePath: parts[1],
			FileMode:          0o644,
		})
	}
	observed, ok := svc.Networks["observed"]
	if !ok || len(observed.Aliases) == 0 {
		t.Fatalf("testdb: compose service has no observed-network alias")
	}
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(vars),
		testcontainers.WithFiles(files...),
		tcnetwork.WithNetwork(observed.Aliases, network),
		testcontainers.WithCmd(unescape(svc.Command)...),
	}
	if len(svc.Entrypoint) > 0 {
		opts = append(opts, testcontainers.WithEntrypoint(unescape(svc.Entrypoint)...))
	}
	return opts
}

// StartDevObservedPair starts dev/docker-compose.yaml's observed-postgres
// and observed-replica on a fresh network, with their compose entrypoints,
// commands, environments, network aliases and bind-mounted files, so tests
// exercise the dev stack's own replication bootstrap. beforeReplica, if not
// nil, runs once the primary is up and before the replica starts. Both have
// host DSNs, as the postgres superuser, whose password is random and shared
// (the replica's roles come from the primary).
func StartDevObservedPair(t testing.TB, beforeReplica func(primary *DB)) *ObservedPair {
	t.Helper()
	skipShort(t)
	ctx := context.Background()
	network := newTestNetwork(t, ctx, false)

	primarySvc := loadDevService(t, "observed-postgres")
	primary := start(t, primarySvc.Image, "observed",
		primarySvc.customizers(t, network, map[string]string{"POSTGRES_PASSWORD": "", "POSTGRES_DB": ""})...)
	if beforeReplica != nil {
		beforeReplica(primary)
	}

	replicaSvc := loadDevService(t, "observed-replica")
	opts := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("observed"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword(primary.password),
		testcontainers.WithExposedPorts(postgresPort),
	}
	opts = append(opts, replicaSvc.customizers(t, network, map[string]string{
		"POSTGRES_PASSWORD":        primary.password,
		"REPLICA_PRIMARY_PASSWORD": primary.password,
	})...)
	opts = append(opts, testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
		wait.ForListeningPort(postgresPort).WithStartupTimeout(3*time.Minute),
		wait.ForLog("database system is ready to accept read-only connections").WithStartupTimeout(3*time.Minute)))
	c, err := runPostgres(t, ctx, replicaSvc.Image, opts...)
	if err != nil {
		t.Fatalf("testdb: start observed replica: %v", err)
	}
	dsn, err := connectVerified(ctx, c, "observed", primary.password, 30*time.Second, 100*time.Millisecond, pgxPing)
	if err != nil {
		t.Fatalf("testdb: connect to observed replica: %v\n%s", err, containerLogs(t, c))
	}
	replica := &DB{DSN: dsn, c: c, dbName: "observed", password: primary.password, secret: primary.secret}
	return &ObservedPair{Primary: primary, Replica: replica}
}

// ReplicaLogs returns the replica container's logs so far, across restarts.
func (p *ObservedPair) ReplicaLogs(t testing.TB) string {
	t.Helper()
	return containerLogs(t, p.Replica.c)
}

func containerLogs(t testing.TB, c testcontainers.Container) string {
	t.Helper()
	r, err := c.Logs(context.Background())
	if err != nil {
		t.Fatalf("testdb: container logs: %v", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("testdb: read container logs: %v", err)
	}
	return string(out)
}
