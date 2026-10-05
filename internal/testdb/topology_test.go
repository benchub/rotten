package testdb

import (
	"context"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"go.yaml.in/yaml/v3"
)

func TestDevTopologyReachability(t *testing.T) {
	topology := StartTopology(t)
	rotten := topology.StartRotten(t)
	observed := topology.StartObserved(t, 18)
	server := topology.StartServerProbe(t)
	worker := topology.StartWorkerProbe(t)
	traffic := topology.StartTrafficProbe(t)
	replica := topology.StartReplicaProbe(t)
	workerReplica := topology.StartWorkerReplicaProbe(t)

	ctx := context.Background()
	rottenIP := containerIPOnNetwork(t, ctx, rotten.Container(), topology.CoreNetwork.Name)
	observedIP := containerIPOnNetwork(t, ctx, observed.Container(), topology.ObservedNetwork.Name)
	serverEdgeIP := containerIPOnNetwork(t, ctx, server.Container, topology.EdgeNetwork.Name)

	assertExecOK(t, ctx, worker.Container, []string{"pg_isready", "-h", server.Alias, "-p", "5432"})
	assertExecOK(t, ctx, server.Container, []string{"pg_isready", "-h", rottenIP, "-p", "5432", "-t", "2"})
	assertExecOK(t, ctx, worker.Container, []string{"pg_isready", "-h", observedIP, "-p", "5432", "-t", "2"})

	code, out := execCombined(t, ctx, worker.Container, []string{"pg_isready", "-h", rottenIP, "-p", "5432", "-t", "2"})
	if code == 0 {
		t.Fatalf("worker reached rotten DB, expected isolation; output:\n%s", out)
	}
	code, out = execCombined(t, ctx, server.Container, []string{"pg_isready", "-h", observedIP, "-p", "5432", "-t", "2"})
	if code == 0 {
		t.Fatalf("server reached observed DB, expected isolation; output:\n%s", out)
	}

	// The traffic generator is on observed only: it reaches the observed DB
	// and nothing on edge or core.
	assertExecOK(t, ctx, traffic.Container, []string{"pg_isready", "-h", observedIP, "-p", "5432", "-t", "2"})
	for _, target := range []struct{ name, ip string }{{"rotten DB", rottenIP}, {"server", serverEdgeIP}} {
		code, out = execCombined(t, ctx, traffic.Container, []string{"pg_isready", "-h", target.ip, "-p", "5432", "-t", "2"})
		if code == 0 {
			t.Fatalf("traffic reached %s, expected isolation; output:\n%s", target.name, out)
		}
	}

	// The replica worker is on observed and edge, like the primary's: it
	// reaches the replica, and the server only through edge.
	replicaIP := containerIPOnNetwork(t, ctx, replica.Container, topology.ObservedNetwork.Name)
	serverCoreIP := containerIPOnNetwork(t, ctx, server.Container, topology.CoreNetwork.Name)
	assertExecOK(t, ctx, workerReplica.Container, []string{"pg_isready", "-h", replicaIP, "-p", "5432", "-t", "2"})
	assertExecOK(t, ctx, workerReplica.Container, []string{"pg_isready", "-h", replica.Alias, "-p", "5432", "-t", "2"})
	assertExecOK(t, ctx, workerReplica.Container, []string{"pg_isready", "-h", server.Alias, "-p", "5432", "-t", "2"})
	assertExecOK(t, ctx, workerReplica.Container, []string{"pg_isready", "-h", serverEdgeIP, "-p", "5432", "-t", "2"})
	for _, target := range []struct{ name, ip string }{{"rotten DB", rottenIP}, {"server on core", serverCoreIP}} {
		code, out = execCombined(t, ctx, workerReplica.Container, []string{"pg_isready", "-h", target.ip, "-p", "5432", "-t", "2"})
		if code == 0 {
			t.Fatalf("replica worker reached %s, expected isolation; output:\n%s", target.name, out)
		}
	}
	// The replica is on observed only: the server can't reach it, and
	// traffic can.
	code, out = execCombined(t, ctx, server.Container, []string{"pg_isready", "-h", replicaIP, "-p", "5432", "-t", "2"})
	if code == 0 {
		t.Fatalf("server reached the observed replica, expected isolation; output:\n%s", out)
	}
	assertExecOK(t, ctx, traffic.Container, []string{"pg_isready", "-h", replicaIP, "-p", "5432", "-t", "2"})

	if !strings.Contains(topology.RottenInternalDSN(IngestRole), "rotten-db:5432") {
		t.Fatalf("RottenInternalDSN does not use rotten-db:5432: %s", topology.RottenInternalDSN(IngestRole))
	}
	if !strings.Contains(topology.ObservedInternalDSN("rotten_observer"), "observed-db:5432") {
		t.Fatalf("ObservedInternalDSN does not use observed-db:5432: %s", topology.ObservedInternalDSN("rotten_observer"))
	}

	rows := rotten.QueryInContainer(t, OwnerRole, `select count(*) from public.goose_db_version`)
	if got := rows[0][0]; got == "0" {
		t.Fatal("goose_db_version has no rows")
	}
	rows = rotten.QueryInContainer(t, OwnerRole, `select parent_table, retention from public.part_config where parent_table in ('rotten.events', 'rotten.event_context') order by parent_table`)
	want := [][]string{{"rotten.event_context", "21 days"}, {"rotten.events", "21 days"}}
	if len(rows) != len(want) {
		t.Fatalf("retention rows = %v, want %v", rows, want)
	}
	for i := range want {
		if rows[i][0] != want[i][0] || rows[i][1] != want[i][1] {
			t.Fatalf("retention rows = %v, want %v", rows, want)
		}
	}
}

// devComposeService is the part of a dev/docker-compose.yaml service these
// tests check.
type devComposeService struct {
	Networks    yaml.Node `yaml:"networks"`
	NetworkMode string    `yaml:"network_mode"`
	Ports       []any     `yaml:"ports"`
	Volumes     []string  `yaml:"volumes"`
}

func (s devComposeService) networkNames() []string {
	var names []string
	switch s.Networks.Kind {
	case yaml.SequenceNode:
		for _, n := range s.Networks.Content {
			names = append(names, n.Value)
		}
	case yaml.MappingNode:
		for i := 0; i < len(s.Networks.Content); i += 2 {
			names = append(names, s.Networks.Content[i].Value)
		}
	}
	slices.Sort(names)
	return names
}

func loadDevCompose(t *testing.T) map[string]devComposeService {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "dev", "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]devComposeService `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	return compose.Services
}

// TestDevComposeServiceNetworks checks dev/docker-compose.yaml puts each
// observed-side service on the networks the Topology helpers model: the
// observed databases and traffic on observed only, each worker on observed
// and edge only (never core), and the key services on core only. None of
// them publishes a port.
func TestDevComposeServiceNetworks(t *testing.T) {
	services := loadDevCompose(t)
	for name, want := range map[string][]string{
		"observed-postgres":  {"observed"},
		"observed-replica":   {"observed"},
		"traffic":            {"observed"},
		"worker":             {"edge", "observed"},
		"worker-replica":     {"edge", "observed"},
		"worker-key":         {"core"},
		"worker-replica-key": {"core"},
	} {
		svc, ok := services[name]
		if !ok {
			t.Errorf("dev/docker-compose.yaml has no %s service", name)
			continue
		}
		if svc.NetworkMode != "" {
			t.Errorf("%s network_mode = %q, want networks %v", name, svc.NetworkMode, want)
		}
		if len(svc.Ports) != 0 {
			t.Errorf("%s publishes ports %v, want none", name, svc.Ports)
		}
		if got := svc.networkNames(); !slices.Equal(got, want) {
			t.Errorf("%s networks = %v, want %v", name, got, want)
		}
	}
}

// TestDevComposeWorkersKeepTheirOwnSecrets checks each worker mounts its own
// pass key and state volumes, and not the other worker's.
func TestDevComposeWorkersKeepTheirOwnSecrets(t *testing.T) {
	services := loadDevCompose(t)
	for name, want := range map[string]map[string]string{
		"worker":             {"worker-secrets": "/worker-secrets", "worker-state": "/state"},
		"worker-replica":     {"worker-replica-secrets": "/worker-secrets", "worker-replica-state": "/state"},
		"worker-key":         {"worker-secrets": "/worker-secrets"},
		"worker-replica-key": {"worker-replica-secrets": "/worker-secrets"},
	} {
		got := map[string]string{}
		for _, v := range services[name].Volumes {
			parts := strings.Split(v, ":")
			if len(parts) >= 2 && strings.HasPrefix(parts[0], "worker") {
				got[parts[0]] = parts[1]
			}
		}
		if !maps.Equal(got, want) {
			t.Errorf("%s mounts worker volumes %v, want %v", name, got, want)
		}
	}
}

func TestTopologyDBWithoutHostDSNFailsClearly(t *testing.T) {
	switch os.Getenv("ROTTEN_TEST_EMPTY_DSN") {
	case "dsnas":
		db := &DB{}
		db.DSNAs(t, OwnerRole)
		return
	case "connect":
		db := &DB{}
		db.Connect(t)
		return
	}

	for _, mode := range []string{"dsnas", "connect"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestTopologyDBWithoutHostDSNFailsClearly$")
			cmd.Env = append(os.Environ(), "ROTTEN_TEST_EMPTY_DSN="+mode)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("empty DSN subprocess succeeded, output:\n%s", out)
			}
			if !strings.Contains(string(out), "has no host DSN") {
				t.Fatalf("empty DSN failure = %s, want clear no host DSN message", out)
			}
		})
	}
}

func assertExecOK(t *testing.T, ctx context.Context, c execContainer, cmd []string) {
	t.Helper()
	code, out := execCombined(t, ctx, c, cmd)
	if code != 0 {
		t.Fatalf("%v exited %d:\n%s", cmd, code, out)
	}
}

func execCombined(t *testing.T, ctx context.Context, c execContainer, cmd []string) (int, string) {
	t.Helper()
	code, reader, err := c.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("%v: %v", cmd, err)
	}
	out, err := readAll(reader)
	if err != nil {
		t.Fatalf("%v: read output: %v", cmd, err)
	}
	return code, out
}

func readAll(r io.Reader) (string, error) {
	out, err := io.ReadAll(r)
	return string(out), err
}
