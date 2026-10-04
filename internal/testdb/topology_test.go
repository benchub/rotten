package testdb

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

// TestDevComposeTrafficOnObservedOnly checks dev/docker-compose.yaml puts the
// traffic service on the observed network and nothing else, matching
// Topology.TrafficNetworkOptions.
func TestDevComposeTrafficOnObservedOnly(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "dev", "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Networks    yaml.Node `yaml:"networks"`
			NetworkMode string    `yaml:"network_mode"`
			Ports       []any     `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	svc, ok := compose.Services["traffic"]
	if !ok {
		t.Fatal("dev/docker-compose.yaml has no traffic service")
	}
	if svc.NetworkMode != "" {
		t.Fatalf("traffic network_mode = %q, want the observed network", svc.NetworkMode)
	}
	if len(svc.Ports) != 0 {
		t.Fatalf("traffic publishes ports %v, want none", svc.Ports)
	}
	var names []string
	switch svc.Networks.Kind {
	case yaml.SequenceNode:
		for _, n := range svc.Networks.Content {
			names = append(names, n.Value)
		}
	case yaml.MappingNode:
		for i := 0; i < len(svc.Networks.Content); i += 2 {
			names = append(names, svc.Networks.Content[i].Value)
		}
	}
	if len(names) != 1 || names[0] != "observed" {
		t.Fatalf("traffic networks = %v, want [observed]", names)
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
