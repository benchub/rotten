package main

import (
	"path/filepath"
	"testing"

	"github.com/benchub/rotten/internal/testdb"
)

// TestDevWorkerConfigs loads both dev stack worker configs with the real
// loader. They report the same project, environment and cluster, differ in
// role and FQDN, use the same context and fingerprint settings, and each
// checks it's pointed at the right kind of server.
func TestDevWorkerConfigs(t *testing.T) {
	load := func(name string) *Configuration {
		c, err := loadConfiguration(filepath.Join(testdb.RepoRoot(), "dev", name))
		if err != nil {
			t.Fatalf("dev/%s: %v", name, err)
		}
		return c
	}
	p, r := load("worker.json"), load("worker-replica.json")
	if p.Role != "primary" || r.Role != "replica" {
		t.Errorf("roles %q and %q, want primary and replica", p.Role, r.Role)
	}
	if p.FQDN != "observed-postgres" || r.FQDN != "observed-replica" {
		t.Errorf("FQDNs %q and %q, want observed-postgres and observed-replica", p.FQDN, r.FQDN)
	}
	if p.Project != r.Project || p.Environment != r.Environment || p.Cluster != r.Cluster {
		t.Errorf("sources %s/%s/%s and %s/%s/%s, want the same project, environment and cluster",
			p.Project, p.Environment, p.Cluster, r.Project, r.Environment, r.Cluster)
	}
	if p.SanityCheck != "select not pg_is_in_recovery()" || r.SanityCheck != "select pg_is_in_recovery()" {
		t.Errorf("sanity checks %q and %q, want a recovery check that fits each", p.SanityCheck, r.SanityCheck)
	}
	same := func(field, a, b string) {
		if a != b {
			t.Errorf("%s differs: %q and %q", field, a, b)
		}
	}
	same("ContextController", p.ContextController, r.ContextController)
	same("ContextAction", p.ContextAction, r.ContextAction)
	same("ContextJob", p.ContextJob, r.ContextJob)
	same("CursorPattern", p.CursorPattern, r.CursorPattern)
	same("TempTablePattern", p.TempTablePattern, r.TempTablePattern)
	same("MinmaxResetSchema", p.MinmaxResetSchema, r.MinmaxResetSchema)
	same("ServerURL", p.ServerURL, r.ServerURL)
	if p.KeepSchemas != r.KeepSchemas || p.ObservationInterval != r.ObservationInterval {
		t.Errorf("KeepSchemas or ObservationInterval differ")
	}
	if len(r.ObservedDBConn) != 1 || len(p.ObservedDBConn) != 1 || r.ObservedDBConn[0] == p.ObservedDBConn[0] {
		t.Errorf("observed DSNs %v and %v, want one each, different", p.ObservedDBConn, r.ObservedDBConn)
	}
}
