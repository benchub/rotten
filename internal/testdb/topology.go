package testdb

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	ObservedAlias = "observed-db"
	ServerAlias   = "rotten-server"
	WorkerAlias   = "rotten-worker"
	RottenAlias   = "rotten-db"
	TrafficAlias  = "rotten-traffic"
	// ReplicaAlias and WorkerReplicaAlias are the dev stack's observed
	// replica and its worker.
	ReplicaAlias       = "observed-replica-db"
	WorkerReplicaAlias = "rotten-worker-replica"
)

// Topology is the three-network dev/test layout from docs/plan.md.
type Topology struct {
	ObservedNetwork *testcontainers.DockerNetwork
	EdgeNetwork     *testcontainers.DockerNetwork
	CoreNetwork     *testcontainers.DockerNetwork

	ObservedAlias string
	ServerAlias   string
	WorkerAlias   string
	RottenAlias   string

	Rotten   *DB
	Observed *DB
}

type TopologyContainer struct {
	Alias     string
	Container testcontainers.Container
}

type execContainer interface {
	Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error)
}

// StartTopology creates the observed, edge, and core Docker networks used by
// dev/docker-compose.yaml. Containers should be attached with the methods on
// Topology so tests exercise the same isolation boundaries as the dev stack.
// These helpers verify Docker-network isolation only: host-published ports
// reached through host.docker.internal bypass Docker bridge-network membership.
func StartTopology(t testing.TB) *Topology {
	t.Helper()
	skipShort(t)
	ctx := context.Background()
	observed := newTestNetwork(t, ctx, true)
	edge := newTestNetwork(t, ctx, false)
	core := newTestNetwork(t, ctx, true)
	return &Topology{
		ObservedNetwork: observed,
		EdgeNetwork:     edge,
		CoreNetwork:     core,
		ObservedAlias:   ObservedAlias,
		ServerAlias:     ServerAlias,
		WorkerAlias:     WorkerAlias,
		RottenAlias:     RottenAlias,
	}
}

func newTestNetwork(t testing.TB, ctx context.Context, internal bool) *testcontainers.DockerNetwork {
	t.Helper()
	opts := []tcnetwork.NetworkCustomizer{tcnetwork.WithAttachable()}
	if internal {
		opts = append(opts, tcnetwork.WithInternal())
	}
	n, err := tcnetwork.New(ctx, opts...)
	if err != nil {
		t.Fatalf("testdb: create network: %v", err)
	}
	testcontainers.CleanupNetwork(t, n)
	return n
}

// StartRotten starts the migrated rotten database on the core network without
// publishing its Postgres port to the Docker host.
func (topology *Topology) StartRotten(t testing.TB) *DB {
	t.Helper()
	db := topology.StartRottenEmpty(t)
	migrateRottenInContainer(t, db)
	return db
}

// StartRottenEmpty starts the rotten database on the core network without
// loading rotten's schema.
func (topology *Topology) StartRottenEmpty(t testing.TB) *DB {
	t.Helper()
	db := startNoHostDSN(t, RottenImage, "rotten", tcnetwork.WithNetwork([]string{topology.RottenAlias}, topology.CoreNetwork))
	initializeRottenEmptyInContainer(t, db)
	topology.Rotten = db
	return db
}

// StartObserved starts observed Postgres on the observed network.
func (topology *Topology) StartObserved(t testing.TB, version int) *DB {
	t.Helper()
	if version < 14 || version > 18 {
		t.Fatalf("testdb: unsupported Postgres version %d", version)
	}
	image, args, setup := observedSetup(version, true)
	db := startNoHostDSN(t, image, "observed",
		tcnetwork.WithNetwork([]string{topology.ObservedAlias}, topology.ObservedNetwork),
		testcontainers.WithCmdArgs(args...))
	execSQLInContainer(t, db, strings.Join(setup, ";\n")+";\nCREATE ROLE rotten_observer LOGIN PASSWORD 'rotten_observer';\nGRANT pg_read_all_stats TO rotten_observer;")
	topology.Observed = db
	return db
}

// RottenInternalDSN returns a DSN for containers on the core network.
func (topology *Topology) RottenInternalDSN(role string) string {
	return internalDSN(role, role, topology.RottenAlias, "rotten")
}

// ObservedInternalDSN returns a DSN for containers on the observed network.
func (topology *Topology) ObservedInternalDSN(role string) string {
	password := role
	if role == "postgres" {
		password = "postgres"
	}
	return internalDSN(role, password, topology.ObservedAlias, "observed")
}

func internalDSN(role string, password string, host string, dbName string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(role, password),
		Host:   host + ":5432",
		Path:   dbName,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

// ServerNetworkOptions attaches a container to edge and core with alias. Use
// this for rotten-server containers.
func (topology *Topology) ServerNetworkOptions(alias string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{alias}, topology.EdgeNetwork),
		tcnetwork.WithNetwork([]string{alias}, topology.CoreNetwork),
	}
}

// WorkerNetworkOptions attaches a container to observed and edge with alias,
// deliberately not to core.
func (topology *Topology) WorkerNetworkOptions(alias string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{alias}, topology.ObservedNetwork),
		tcnetwork.WithNetwork([]string{alias}, topology.EdgeNetwork),
	}
}

// TrafficNetworkOptions attaches a container to observed only, like the dev
// stack's traffic generator.
func (topology *Topology) TrafficNetworkOptions(alias string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{alias}, topology.ObservedNetwork),
	}
}

// StartTrafficProbe starts a probe container on observed only, where the dev
// stack's traffic generator runs.
func (topology *Topology) StartTrafficProbe(t testing.TB) TopologyContainer {
	t.Helper()
	ctx := context.Background()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("sleep", "infinity"),
	}
	opts = append(opts, topology.TrafficNetworkOptions(TrafficAlias)...)
	c, err := testcontainers.Run(ctx, "postgres:18", opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("testdb: start traffic probe: %v", err)
	}
	return TopologyContainer{Alias: TrafficAlias, Container: c}
}

// StartServerProbe starts a Postgres-backed probe on edge and core. Tests use
// it before the real server is needed to assert the server's network position:
// reachable from the worker over edge, and able to reach the rotten DB on core.
func (topology *Topology) StartServerProbe(t testing.TB) TopologyContainer {
	t.Helper()
	c := runPostgresProbe(t, topology.ServerAlias, topology.ServerNetworkOptions(topology.ServerAlias)...)
	return TopologyContainer{Alias: topology.ServerAlias, Container: c}
}

// StartWorkerProbe starts a probe container on observed and edge, deliberately
// not on core.
func (topology *Topology) StartWorkerProbe(t testing.TB) TopologyContainer {
	t.Helper()
	return topology.startWorkerProbe(t, topology.WorkerAlias)
}

// StartWorkerReplicaProbe starts a probe for the replica's worker: like the
// primary's, on observed and edge only.
func (topology *Topology) StartWorkerReplicaProbe(t testing.TB) TopologyContainer {
	t.Helper()
	return topology.startWorkerProbe(t, WorkerReplicaAlias)
}

func (topology *Topology) startWorkerProbe(t testing.TB, alias string) TopologyContainer {
	t.Helper()
	ctx := context.Background()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("sleep", "infinity"),
	}
	opts = append(opts, topology.WorkerNetworkOptions(alias)...)
	c, err := testcontainers.Run(ctx, "postgres:18", opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("testdb: start %s probe: %v", alias, err)
	}
	return TopologyContainer{Alias: alias, Container: c}
}

// StartReplicaProbe starts a Postgres probe on observed only, where the dev
// stack's observed replica runs. StartDevObservedPair runs a real replica.
func (topology *Topology) StartReplicaProbe(t testing.TB) TopologyContainer {
	t.Helper()
	c := runPostgresProbe(t, ReplicaAlias, tcnetwork.WithNetwork([]string{ReplicaAlias}, topology.ObservedNetwork))
	return TopologyContainer{Alias: ReplicaAlias, Container: c}
}

func (c TopologyContainer) Exec(ctx context.Context, cmd []string, options ...tcexec.ProcessOption) (int, io.Reader, error) {
	return c.Container.Exec(ctx, cmd, options...)
}

func (c TopologyContainer) Start(ctx context.Context) error {
	return c.Container.Start(ctx)
}

func (c TopologyContainer) Stop(ctx context.Context, timeout *time.Duration) error {
	return c.Container.Stop(ctx, timeout)
}

func (c TopologyContainer) Terminate(ctx context.Context) error {
	return c.Container.Terminate(ctx)
}

func containerIPOnNetwork(t testing.TB, ctx context.Context, c testcontainers.Container, networkName string) string {
	t.Helper()
	inspect, err := c.Inspect(ctx)
	if err != nil {
		t.Fatalf("testdb: inspect container: %v", err)
	}
	network, ok := inspect.NetworkSettings.Networks[networkName]
	if !ok {
		t.Fatalf("testdb: container is not attached to network %s", networkName)
	}
	if !network.IPAddress.IsValid() {
		t.Fatalf("testdb: container has no IP on network %s", networkName)
	}
	return network.IPAddress.String()
}

func runPostgresProbe(t testing.TB, alias string, networks ...testcontainers.ContainerCustomizer) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "postgres",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(3 * time.Minute)),
	}
	opts = append(opts, networks...)
	c, err := testcontainers.Run(ctx, "postgres:18", opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("testdb: start %s probe: %v", alias, err)
	}
	return c
}
