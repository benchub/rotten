package ingest_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

type registerFixture struct {
	ctx     context.Context
	owner   *pgxpool.Pool
	ingest  *pgxpool.Pool
	handler *ingest.Handler
	key     auth.Key
}

func setupRegister(t *testing.T, fqdn string) *registerFixture {
	t.Helper()
	db := testdb.StartRotten(t)
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	ingestPool, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingestPool.Close)
	created, err := auth.CreateKey(ctx, owner, "worker", fqdn, "test")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := auth.ParseKey(created.Token)
	if err != nil {
		t.Fatal(err)
	}
	return &registerFixture{
		ctx:     auth.NewContext(ctx, auth.Key{ID: id, Name: "worker", FQDN: fqdn}),
		owner:   owner,
		ingest:  ingestPool,
		handler: ingest.NewHandler(ingestPool),
		key:     auth.Key{ID: id, Name: "worker", FQDN: fqdn},
	}
}

func registerRequest(project, environment, cluster, role, fqdn string) *connect.Request[rottenv1.RegisterRequest] {
	return connect.NewRequest(&rottenv1.RegisterRequest{
		Project:     project,
		Environment: environment,
		Cluster:     cluster,
		Role:        role,
		Fqdn:        fqdn,
	})
}

func TestRegisterCreatesAndReusesSource(t *testing.T) {
	f := setupRegister(t, "db1.example.com")
	req := registerRequest("billing", "prod", "east", "primary", "db1.example.com")

	first, err := f.handler.Register(f.ctx, req)
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if first.Msg.GetLogicalSourceId() == 0 || first.Msg.GetPhysicalSourceId() == 0 {
		t.Fatalf("Register returned zero IDs: %v", first.Msg)
	}
	second, err := f.handler.Register(f.ctx, req)
	if err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if second.Msg.GetLogicalSourceId() != first.Msg.GetLogicalSourceId() || second.Msg.GetPhysicalSourceId() != first.Msg.GetPhysicalSourceId() {
		t.Fatalf("second Register = %v, want same IDs as %v", second.Msg, first.Msg)
	}
	wantSourceRows(t, f.owner, "billing", "prod", "east", "primary", "db1.example.com", 1, 1)
}

func TestRegisterReusesPhysicalSourceAcrossLogicalSources(t *testing.T) {
	f := setupRegister(t, "db1.example.com")
	first, err := f.handler.Register(f.ctx, registerRequest("billing", "prod", "east", "primary", "db1.example.com"))
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}
	second, err := f.handler.Register(f.ctx, registerRequest("billing", "prod", "east", "replica", "db1.example.com"))
	if err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if first.Msg.GetLogicalSourceId() == second.Msg.GetLogicalSourceId() {
		t.Fatalf("logical source IDs both %d, want distinct logical sources", first.Msg.GetLogicalSourceId())
	}
	if first.Msg.GetPhysicalSourceId() != second.Msg.GetPhysicalSourceId() {
		t.Fatalf("physical source IDs = %d and %d, want one row per host", first.Msg.GetPhysicalSourceId(), second.Msg.GetPhysicalSourceId())
	}
	var links int
	if err := f.owner.QueryRow(context.Background(), `
		select count(*) from rotten.logical_physical_sources
		where physical_source_id = $1`, first.Msg.GetPhysicalSourceId()).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if links != 2 {
		t.Fatalf("logical_physical_sources links = %d, want 2", links)
	}
}

func TestRegisterConcurrentCallsCreateOneSource(t *testing.T) {
	f := setupRegister(t, "db2.example.com")
	req := registerRequest("checkout", "prod", "west", "replica", "db2.example.com")

	const goroutines = 24
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	logicalIDs := make(chan uint32, goroutines)
	physicalIDs := make(chan uint32, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := f.handler.Register(f.ctx, req)
			if err != nil {
				errs <- err
				return
			}
			logicalIDs <- resp.Msg.GetLogicalSourceId()
			physicalIDs <- resp.Msg.GetPhysicalSourceId()
		}()
	}
	wg.Wait()
	close(errs)
	close(logicalIDs)
	close(physicalIDs)
	for err := range errs {
		t.Errorf("Register: %v", err)
	}
	var logical, physical uint32
	for id := range logicalIDs {
		if logical == 0 {
			logical = id
		} else if id != logical {
			t.Errorf("logical ID = %d, want %d", id, logical)
		}
	}
	for id := range physicalIDs {
		if physical == 0 {
			physical = id
		} else if id != physical {
			t.Errorf("physical ID = %d, want %d", id, physical)
		}
	}
	if logical == 0 || physical == 0 {
		t.Fatalf("got zero IDs: logical=%d physical=%d", logical, physical)
	}
	wantSourceRows(t, f.owner, "checkout", "prod", "west", "replica", "db2.example.com", 1, 1)
}

func TestRegisterRejectsPinnedFQDNMismatchWithoutWriting(t *testing.T) {
	f := setupRegister(t, "db3.example.com")
	req := registerRequest("inventory", "prod", "north", "primary", "other.example.com")

	_, err := f.handler.Register(f.ctx, req)
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("Register err = %v, want PermissionDenied", err)
	}
	wantSourceRows(t, f.owner, "inventory", "prod", "north", "primary", "other.example.com", 0, 0)
}

func TestRegisterRejectsReservedLogicalSource(t *testing.T) {
	f := setupRegister(t, "db4.example.com")
	req := registerRequest("all", "all", "all", "all", "db4.example.com")

	_, err := f.handler.Register(f.ctx, req)
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("Register err = %v, want InvalidArgument", err)
	}
	if strings.Contains(err.Error(), "source id 0") {
		t.Fatalf("Register leaked reserved row internals: %v", err)
	}
}

func TestRegisterUnavailableHidesDatabaseErrorAndLogsDetail(t *testing.T) {
	f := setupRegister(t, "db5.example.com")
	var logs bytes.Buffer
	f.handler = ingest.NewHandler(f.ingest, ingest.Options{
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	f.ingest.Close()

	_, err := f.handler.Register(f.ctx, registerRequest("support", "prod", "south", "primary", "db5.example.com"))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("Register err = %v, want Unavailable", err)
	}
	msg := err.Error()
	for _, leaked := range []string{"closed pool", "permission denied", "SQLSTATE", "logical_sources"} {
		if strings.Contains(msg, leaked) {
			t.Fatalf("Register error %q leaked %q", msg, leaked)
		}
	}
	logText := logs.String()
	if !strings.Contains(logText, "register source failed") || !strings.Contains(logText, "key_id=") || !strings.Contains(logText, "closed pool") {
		t.Fatalf("logs = %q, want key id and detailed database error", logText)
	}
}

func wantSourceRows(t *testing.T, db *pgxpool.Pool, project, environment, cluster, role, fqdn string, wantLogical, wantPhysical int) {
	t.Helper()
	ctx := context.Background()
	var logical, physical int
	if err := db.QueryRow(ctx, `select count(*) from rotten.logical_sources
		where project = $1 and environment = $2 and cluster = $3 and role = $4`,
		project, environment, cluster, role).Scan(&logical); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `select count(*) from rotten.physical_sources where fqdn = $1`, fqdn).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if logical != wantLogical || physical != wantPhysical {
		t.Fatalf("source rows logical=%d physical=%d, want logical=%d physical=%d", logical, physical, wantLogical, wantPhysical)
	}
}
