package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/testcerts"
	"github.com/benchub/rotten/internal/testdb"
)

const e2eWorkloadQuery = `select count(*) from widgets where id > 1 /*controller:e2e,action:show*/`

func TestWorkerServerEndToEndSurvivesServerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped under -short")
	}
	if runtime.GOOS != "linux" {
		t.Skip("worker container e2e needs a linux cgo binary; make test runs this inside the linux test image")
	}
	ctx := context.Background()
	artifacts := workerTestArtifacts(t)
	workerBin := buildWorkerBinary(t, artifacts)
	serverBin := buildServerBinary(t, artifacts)
	certDir := filepath.Join(artifacts, "certs")
	if err := testcerts.Generate(certDir, testdb.ServerAlias); err != nil {
		t.Fatal(err)
	}

	topology := testdb.StartTopology(t)
	rotten := topology.StartRotten(t)
	observed := topology.StartObserved(t, 18)
	loadObserverSchemaInContainer(t, observed)
	observed.QueryInContainer(t, "postgres", `create table widgets (id int primary key, name text); insert into widgets select g, 'w' || g from generate_series(1, 10) g`)

	token := createE2EKey(t, rotten, "db-e2e.example")
	passFile := filepath.Join(artifacts, "pass.key")
	if err := os.WriteFile(passFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	confFile := filepath.Join(artifacts, "worker.json")
	conf := fmt.Sprintf(`{
	"ObservedDBConn": [%q],
	"ServerURL": "https://%s:8443",
	"PassKeyFile": "/pass.key",
	"ServerCAFile": "/ca.pem",
	"StateDir": "/state",
	"MaxSnapshotAge": 60,
	"SanityCheck": "select true",
	"StatusInterval": 1,
	"ObservationInterval": 2,
	"FQDN": "db-e2e.example",
	"Project": "e2e",
	"Environment": "test",
	"Cluster": "cluster",
	"Role": "primary",
	"ContextController": "/\\\\*.*controller:([^,\\\\*]+).*\\\\*/",
	"ContextAction": "/\\\\*.*action:([^,\\\\*]+).*\\\\*/",
	"ContextJob": "/\\\\*.*job:([^,\\\\*]+).*\\\\*/",
	"MinmaxResetSchema": "rotten"
}`, topology.ObservedInternalDSN("rotten_observer"), testdb.ServerAlias)
	if err := os.WriteFile(confFile, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}

	server := startE2EServer(t, topology, serverBin, certDir)
	worker := startE2EWorker(t, topology, workerBin, confFile, passFile, filepath.Join(certDir, "ca.pem"))

	runE2EWorkload(t, observed, 3)
	waitForE2ECalls(t, rotten, 3)
	afterThreeCallPhase := time.Now()

	if err := server.Terminate(ctx); err != nil {
		t.Fatalf("terminate first server: %v", err)
	}
	runE2EWorkload(t, observed, 4)
	time.Sleep(5 * time.Second)
	assertE2ECalls(t, rotten, 3)

	secondServerStart := time.Now()
	server = startE2EServer(t, topology, serverBin, certDir)
	runE2EWorkload(t, observed, 5)
	fiveCallWorkloadDone := time.Now()
	waitForE2ECalls(t, rotten, 12)
	assertFourCallsQueuedDuringOutage(t, rotten, afterThreeCallPhase, secondServerStart)
	assertE2EWindowsContiguous(t, rotten)
	assertE2EShowContext(t, rotten)
	waitForSourceWindowAfter(t, observed, rotten, fiveCallWorkloadDone.Add(4*time.Second))
	assertE2ECalls(t, rotten, 12)
	assertNoDuplicateE2EWindows(t, rotten)

	if err := worker.Terminate(ctx); err != nil {
		t.Fatalf("terminate worker: %v", err)
	}
	if err := server.Terminate(ctx); err != nil {
		t.Fatalf("terminate second server: %v", err)
	}
}

func workerTestArtifacts(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(".test-artifacts", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(abs); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(abs) })
	return abs
}

func buildWorkerBinary(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "rotten-worker")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/rotten-worker")
	cmd.Dir = testdb.RepoRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build rotten-worker: %v\n%s", err, out)
	}
	return bin
}

func buildServerBinary(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "rotten-server")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/rotten-server")
	cmd.Dir = testdb.RepoRoot()
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build rotten-server: %v\n%s", err, out)
	}
	return bin
}

func loadObserverSchemaInContainer(t *testing.T, db *testdb.DB) {
	t.Helper()
	ctx := context.Background()
	dst := "/rotten-observer.sql"
	if err := db.Container().CopyFileToContainer(ctx, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), dst, 0o644); err != nil {
		t.Fatalf("copy observer schema: %v", err)
	}
	code, r, err := db.Container().Exec(ctx, []string{"psql", "-X", "-U", "postgres", "-d", "observed", "-v", "ON_ERROR_STOP=1", "-f", dst}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec observer schema: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("observer schema exited %d: %s", code, out)
	}
	observed := topologyObservedPasswordSQL()
	db.QueryInContainer(t, "postgres", observed)
}

func topologyObservedPasswordSQL() string {
	return "alter role rotten_observer password 'rotten_observer'"
}

func createE2EKey(t *testing.T, rotten *testdb.DB, fqdn string) string {
	t.Helper()
	secret, hash, err := auth.NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, fmt.Sprintf("insert into rotten.api_keys (name, secret_hash, fqdn, created_by) values ('e2e-worker', '%s', '%s', 'test') returning id", hash, fqdn))
	if len(rows) == 0 || len(rows[0]) != 1 {
		t.Fatalf("api key insert returned %v", rows)
	}
	id, err := strconv.ParseInt(rows[0][0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return auth.FormatKey(id, secret)
}

func startE2EServer(t *testing.T, topology *testdb.Topology, serverBin string, certDir string) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithFiles(
			testcontainers.ContainerFile{HostFilePath: serverBin, ContainerFilePath: "/rotten-server", FileMode: 0o755},
			testcontainers.ContainerFile{HostFilePath: filepath.Join(certDir, "server.pem"), ContainerFilePath: "/server.pem", FileMode: 0o644},
			testcontainers.ContainerFile{HostFilePath: filepath.Join(certDir, "server-key.pem"), ContainerFilePath: "/server-key.pem", FileMode: 0o600},
		),
		testcontainers.WithEntrypoint("/rotten-server"),
		testcontainers.WithCmdArgs("serve", "-dsn", topology.RottenInternalDSN(testdb.IngestRole), "-listen", ":8443", "-tls-cert", "/server.pem", "-tls-key", "/server-key.pem"),
		testcontainers.WithWaitStrategy(wait.ForLog("listening on https://").WithStartupTimeout(30 * time.Second)),
	}
	opts = append(opts, topology.ServerNetworkOptions(testdb.ServerAlias)...)
	c, err := testcontainers.Run(ctx, "postgres:18", opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start e2e server: %v", err)
	}
	return c
}

func startE2EWorker(t *testing.T, topology *testdb.Topology, workerBin string, confFile string, passFile string, caFile string) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithFiles(
			testcontainers.ContainerFile{HostFilePath: workerBin, ContainerFilePath: "/rotten-worker", FileMode: 0o755},
			testcontainers.ContainerFile{HostFilePath: confFile, ContainerFilePath: "/worker.json", FileMode: 0o600},
			testcontainers.ContainerFile{HostFilePath: passFile, ContainerFilePath: "/pass.key", FileMode: 0o600},
			testcontainers.ContainerFile{HostFilePath: caFile, ContainerFilePath: "/ca.pem", FileMode: 0o644},
		),
		testcontainers.WithEntrypoint("/rotten-worker"),
		testcontainers.WithCmdArgs("-config", "/worker.json"),
		testcontainers.WithWaitStrategy(wait.ForLog("baseline harvest").WithStartupTimeout(45 * time.Second)),
	}
	opts = append(opts, topology.WorkerNetworkOptions(testdb.WorkerAlias)...)
	c, err := testcontainers.Run(ctx, "postgres:18", opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start e2e worker: %v", err)
	}
	return c
}

func runE2EWorkload(t *testing.T, observed *testdb.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		observed.QueryInContainer(t, "postgres", e2eWorkloadQuery)
	}
}

func runE2EOtherWorkload(t *testing.T, observed *testdb.DB) {
	t.Helper()
	observed.QueryInContainer(t, "postgres", `select avg(id) from widgets where id > 5 /*controller:e2e,action:other*/`)
}

func waitForE2ECalls(t *testing.T, rotten *testdb.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if got := e2eCalls(t, rotten); got == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("calls = %d, want %d", e2eCalls(t, rotten), want)
}

func assertE2ECalls(t *testing.T, rotten *testdb.DB, want int) {
	t.Helper()
	if got := e2eCalls(t, rotten); got != want {
		t.Fatalf("calls = %d, want %d", got, want)
	}
}

func e2eCalls(t *testing.T, rotten *testdb.DB) int {
	t.Helper()
	fp := e2eFingerprint(t)
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, fmt.Sprintf(`select coalesce(sum(calls), 0)::int from rotten.events e join rotten.fingerprints f on f.id = e.fingerprint_id where f.fingerprint = '%s' and e.logical_source_id in (select id from rotten.logical_sources where project = 'e2e')`, fp))
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("calls query returned %v", rows)
	}
	got, err := strconv.Atoi(rows[0][0])
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func assertNoDuplicateE2EWindows(t *testing.T, rotten *testdb.DB) {
	t.Helper()
	fp := e2eFingerprint(t)
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, fmt.Sprintf(`select count(*) from (select observed_window_start, observed_window_end, count(*) from rotten.events e join rotten.fingerprints f on f.id = e.fingerprint_id where f.fingerprint = '%s' and e.logical_source_id in (select id from rotten.logical_sources where project = 'e2e') group by 1, 2 having count(*) > 1) dup`, fp))
	if len(rows) != 1 || rows[0][0] != "0" {
		t.Fatalf("duplicate windows = %v, want 0", rows)
	}
}

func assertFourCallsQueuedDuringOutage(t *testing.T, rotten *testdb.DB, afterThreeCallPhase time.Time, secondServerStart time.Time) {
	t.Helper()
	fp := e2eFingerprint(t)
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, fmt.Sprintf(`select coalesce(sum(e.calls), 0)::int from rotten.events e join rotten.fingerprints f on f.id = e.fingerprint_id where f.fingerprint = '%s' and e.observed_window_end > timestamptz '%s' and e.observed_window_end < timestamptz '%s' and e.logical_source_id in (select id from rotten.logical_sources where project = 'e2e')`, fp, afterThreeCallPhase.UTC().Format(time.RFC3339Nano), secondServerStart.UTC().Format(time.RFC3339Nano)))
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "4" {
		t.Fatalf("outage queued calls = %v, want 4", rows)
	}
}

func assertE2EWindowsContiguous(t *testing.T, rotten *testdb.DB) {
	t.Helper()
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, `select extract(epoch from observed_window_start)::bigint, extract(epoch from observed_window_end)::bigint from rotten.ingested_batches where logical_source_id in (select id from rotten.logical_sources where project = 'e2e') order by observed_window_start`)
	if len(rows) < 2 {
		t.Fatalf("windows = %v, want at least two", rows)
	}
	var prevEnd string
	for i, row := range rows {
		if len(row) != 2 {
			t.Fatalf("bad window row %v", row)
		}
		if i > 0 && row[0] != prevEnd {
			t.Fatalf("window %d starts at %s, previous ended at %s; rows=%v", i, row[0], prevEnd, rows)
		}
		prevEnd = row[1]
	}
}

func assertE2EShowContext(t *testing.T, rotten *testdb.DB) {
	t.Helper()
	fp := e2eFingerprint(t)
	rows := rotten.QueryInContainer(t, testdb.OwnerRole, fmt.Sprintf(`select coalesce(sum(c.c), 0)::int from rotten.event_context c join rotten.events e on e.id = c.event_id join rotten.fingerprints f on f.id = e.fingerprint_id join rotten.controllers co on co.id = c.controller_id join rotten.actions a on a.id = c.action_id where f.fingerprint = '%s' and co.controller = 'e2e' and a.action = 'show'`, fp))
	if len(rows) != 1 || rows[0][0] != "12" {
		t.Fatalf("e2e/show context rows = %v, want 12", rows)
	}
}

func waitForSourceWindowAfter(t *testing.T, observed *testdb.DB, rotten *testdb.DB, after time.Time) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		runE2EOtherWorkload(t, observed)
		rows := rotten.QueryInContainer(t, testdb.OwnerRole, `select coalesce(extract(epoch from max(observed_window_end))::bigint, 0) from rotten.events where logical_source_id in (select id from rotten.logical_sources where project = 'e2e')`)
		if len(rows) == 1 && len(rows[0]) == 1 {
			if end, err := strconv.ParseInt(rows[0][0], 10, 64); err == nil && time.Unix(end, 0).After(after) {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("source max observed_window_end did not pass %v", after)
}

func e2eFingerprint(t *testing.T) string {
	t.Helper()
	fp, err := fingerprint.Normalized(e2eWorkloadQuery, fingerprint.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return fp
}
