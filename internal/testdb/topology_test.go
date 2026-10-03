package testdb

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestDevTopologyReachability(t *testing.T) {
	topology := StartTopology(t)
	rotten := topology.StartRotten(t)
	observed := topology.StartObserved(t, 18)
	server := topology.StartServerProbe(t)
	worker := topology.StartWorkerProbe(t)

	ctx := context.Background()
	rottenIP := containerIPOnNetwork(t, ctx, rotten.Container(), topology.CoreNetwork.Name)
	observedIP := containerIPOnNetwork(t, ctx, observed.Container(), topology.ObservedNetwork.Name)

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
