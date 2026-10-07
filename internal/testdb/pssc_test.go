package testdb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// TestObservedPSSCDefaultExtractorsMissPrepended documents why StartObserved
// sets PSSCExtractors: pssc's default extractors read only appended
// comments, so the prepended production format goes untagged.
func TestObservedPSSCDefaultExtractorsMissPrepended(t *testing.T) {
	skipShort(t)
	_, args, setup := observedSetup(18, true)
	var defaults []string
	for i := 0; i+1 < len(args); i += 2 {
		if !strings.HasPrefix(args[i+1], "pg_stat_statement_context.extractors=") {
			defaults = append(defaults, args[i], args[i+1])
		}
	}
	db := start(t, ObservedImage(18), "observed", testcontainers.WithCmdArgs(defaults...))
	conn := db.Connect(t)
	ctx := context.Background()
	for _, q := range append(setup,
		"/*controller:prepended,action:b*/ select 1",
		"select 1 /*controller:appended,action:b*/") {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var prepended, appended int64
	if err := conn.QueryRow(ctx, `
select coalesce(sum(calls_total) filter (where tags->>'controller' = 'prepended'), 0)::bigint,
       coalesce(sum(calls_total) filter (where tags->>'controller' = 'appended'), 0)::bigint
  from pg_stat_statement_context_totals`).Scan(&prepended, &appended); err != nil {
		t.Fatal(err)
	}
	if prepended != 0 || appended != 1 {
		t.Fatalf("default extractors: prepended calls = %d (want 0), appended calls = %d (want 1)", prepended, appended)
	}
}

// TestDevObservedPreloadsPSSC checks both dev observed databases run the pssc
// image with the same preload order and extractors as StartObserved.
func TestDevObservedPreloadsPSSC(t *testing.T) {
	_, want, _ := observedSetup(18, true)
	for _, service := range []string{"observed-postgres", "observed-replica"} {
		svc := decodeDevService[struct {
			Image   string   `yaml:"image"`
			Command []string `yaml:"command"`
			Build   *struct {
				Dockerfile string            `yaml:"dockerfile"`
				Args       map[string]string `yaml:"args"`
			} `yaml:"build"`
		}](t, service)
		if svc.Image != "rotten-dev-observed:18" {
			t.Errorf("%s image = %q, want rotten-dev-observed:18", service, svc.Image)
		}
		// Only the primary builds the image; the replica reuses it.
		if service == "observed-postgres" {
			if svc.Build == nil || svc.Build.Dockerfile != "observed-db.Dockerfile" || svc.Build.Args["PG_MAJOR"] != "18" {
				t.Errorf("%s build = %+v, want observed-db.Dockerfile with PG_MAJOR 18", service, svc.Build)
			}
		} else if svc.Build != nil {
			t.Errorf("%s has a build; only observed-postgres builds the image", service)
		}
		cmd := strings.Join(svc.Command, " ")
		for i := 0; i+1 < len(want); i += 2 {
			if !strings.Contains(cmd, want[i]+" "+want[i+1]) {
				t.Errorf("%s command lacks %s %q: %q", service, want[i], want[i+1], svc.Command)
			}
		}
	}
}

// TestObservedPSSCRecordsTaggedStatements is the smoke check for the observed
// images: on each major, pg_stat_statement_context is preloaded after
// pg_stat_statements and records a marginalia comment both appended and
// prepended (the production format, which Postgres 18's pgss text drops).
func TestObservedPSSCRecordsTaggedStatements(t *testing.T) {
	for _, version := range ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			db := StartObserved(t, version)
			conn := db.Connect(t)
			ctx := context.Background()

			var preload string
			if err := conn.QueryRow(ctx, "show shared_preload_libraries").Scan(&preload); err != nil {
				t.Fatal(err)
			}
			if preload != "pg_stat_statements, pg_stat_statement_context" {
				t.Fatalf("shared_preload_libraries = %q, want pgss first, then pssc", preload)
			}

			cases := []struct{ controller, sql string }{
				{"appended", "select 1 /*controller:appended,action:b*/"},
				{"prepended", "/*controller:prepended,action:b*/ select 1"},
			}
			for _, c := range cases {
				// Exec with no arguments uses the simple protocol, so the
				// comment reaches the executor as statement text.
				if _, err := conn.Exec(ctx, c.sql); err != nil {
					t.Fatalf("%s: %v", c.sql, err)
				}
			}
			for _, c := range cases {
				var calls int64
				err := conn.QueryRow(ctx, `
select coalesce(sum(calls_total), 0)::bigint
  from pg_stat_statement_context_totals
 where tags->>'controller' = $1 and tags->>'action' = 'b'`, c.controller).Scan(&calls)
				if err != nil {
					t.Fatalf("%s: read totals: %v", c.controller, err)
				}
				if calls != 1 {
					t.Errorf("%s (%q): calls_total = %d, want 1", c.controller, c.sql, calls)
				}
			}
			// The dev traffic's job marginalia tag job_tag, which pssc's
			// default tags allowlist drops.
			if _, err := conn.Exec(ctx, "select 1 /*job_tag:Cleanup*/"); err != nil {
				t.Fatal(err)
			}
			var jobCalls int64
			if err := conn.QueryRow(ctx, `
select coalesce(sum(calls_total), 0)::bigint
  from pg_stat_statement_context_totals where tags->>'job_tag' = 'Cleanup'`).Scan(&jobCalls); err != nil {
				t.Fatal(err)
			}
			if jobCalls != 1 {
				t.Errorf("job_tag: calls_total = %d, want 1", jobCalls)
			}
		})
	}
}

// TestObservedWithoutPSSCReportsExtensionMissing covers the optional path
// (e.g. RDS): a database started without pssc has neither the library
// preloaded nor the extension available.
func TestObservedWithoutPSSCReportsExtensionMissing(t *testing.T) {
	for _, version := range ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			db := StartObservedWithoutPSSC(t, version)
			conn := db.Connect(t)
			ctx := context.Background()

			var preload string
			if err := conn.QueryRow(ctx, "show shared_preload_libraries").Scan(&preload); err != nil {
				t.Fatal(err)
			}
			if preload != "pg_stat_statements" {
				t.Fatalf("shared_preload_libraries = %q, want pg_stat_statements only", preload)
			}
			var installed, available bool
			if err := conn.QueryRow(ctx, `
select exists (select from pg_extension where extname = 'pg_stat_statement_context'),
       exists (select from pg_available_extensions where name = 'pg_stat_statement_context')`).Scan(&installed, &available); err != nil {
				t.Fatal(err)
			}
			if installed || available {
				t.Fatalf("pssc installed=%v available=%v, want neither", installed, available)
			}
			var pgss bool
			if err := conn.QueryRow(ctx, "select exists (select from pg_extension where extname = 'pg_stat_statements')").Scan(&pgss); err != nil {
				t.Fatal(err)
			}
			if !pgss {
				t.Fatal("pg_stat_statements isn't created")
			}
		})
	}
}
