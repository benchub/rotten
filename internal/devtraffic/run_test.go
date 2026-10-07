package devtraffic_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/devtraffic"
	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/serverclient"
	"github.com/benchub/rotten/internal/state"
	"github.com/benchub/rotten/internal/testdb"
	"github.com/benchub/rotten/internal/worker"
)

type collectingSubmitter struct {
	msgs []*rottenv1.SubmitHarvestRequest
}

func (c *collectingSubmitter) SubmitHarvest(_ context.Context, msg *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestResponse, serverclient.ClipCounts, error) {
	c.msgs = append(c.msgs, msg)
	return &rottenv1.SubmitHarvestResponse{}, serverclient.ClipCounts{}, nil
}

// likeAt is a LIKE pattern for a statement with comment at pos.
func likeAt(pos devtraffic.Position, comment string) string {
	if pos == devtraffic.Trailing {
		return "% " + comment
	}
	return comment + " %"
}

func contextKey(controller, action, jobTag string) string {
	return controller + "\x00" + action + "\x00" + jobTag
}

// TestPostgres18DropsLeadingComments pins the behavior behind Auto: Postgres
// 18's pg_stat_statements keeps no leading comment in the query text, while
// 17 does, and both keep a trailing one.
func TestPostgres18DropsLeadingComments(t *testing.T) {
	for _, v := range []int{17, 18} {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := testdb.StartObserved(t, v)
			conn := db.Connect(t)
			for _, s := range []string{
				"create table pin (a int, b int)",
				"/*controller:lead*/ select a from pin",
				"select b from pin /*controller:trail*/",
			} {
				if _, err := conn.Exec(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			var lead, trail string
			if err := conn.QueryRow(ctx, `select
  (select query from pg_stat_statements where query like '%select a from pin%'),
  (select query from pg_stat_statements where query like '%select b from pin%')`).Scan(&lead, &trail); err != nil {
				t.Fatal(err)
			}
			wantLead := "/*controller:lead*/ select a from pin"
			if v >= 18 {
				wantLead = "select a from pin"
			}
			if lead != wantLead {
				t.Errorf("leading: pg_stat_statements text %q, want %q", lead, wantLead)
			}
			if want := "select b from pin /*controller:trail*/"; trail != want {
				t.Errorf("trailing: pg_stat_statements text %q, want %q", trail, want)
			}
		})
	}
}

// TestRunFeedsTheWorkerSeveralContextsPerFingerprint runs a short generator,
// with Auto comments, against real Postgres 17 (leading comments) and 18
// (trailing, what the dev stack runs) while the real worker harvests it with
// dev/worker.json's regexes and fingerprint settings. It checks that the
// worker sees many of the generator's fingerprints, that every context it
// attributes is one the generator really ran that shape under, and that
// several fingerprints carry several contexts.
func TestRunFeedsTheWorkerSeveralContextsPerFingerprint(t *testing.T) {
	for _, v := range []int{17, 18} {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			runFeedsTheWorker(t, v)
		})
	}
}

func runFeedsTheWorker(t *testing.T, version int) {
	ctx := context.Background()
	db := testdb.StartObserved(t, version)
	if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	su := db.Connect(t)
	if _, err := su.Exec(ctx, "alter role rotten_observer password '"+db.RolePassword("rotten_observer")+"'"); err != nil {
		t.Fatal(err)
	}

	obsCfg, err := pgx.ParseConfig(db.DSNAs(t, "rotten_observer"))
	if err != nil {
		t.Fatal(err)
	}
	obs, err := pgx.ConnectConfig(ctx, obsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close(ctx)

	store, err := state.Open(t.TempDir(), state.Options{MaxSnapshotAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reC, reA, reJ := devRegexes(t)
	fpOpts := devFingerprintOptions(t)
	w := worker.New(worker.Config{
		ObservedDB:          obs,
		ObservationInterval: 1,
		SanityCheck:         "select true",
		LogicalID:           1,
		PhysicalID:          1,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		Fingerprint:         fpOpts,
		State:               store,
		ServerOutbox:        store,
	}, worker.RealClock{})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- w.Run(runCtx) }()

	// Wait for the worker's baseline, so every traffic entry is new to it.
	deadline := time.Now().Add(time.Minute)
	for {
		loaded, err := store.Load(ctx)
		if err == nil && !loaded.Baseline {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker took no baseline snapshot: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	stats, err := devtraffic.Run(ctx, devtraffic.Config{
		AdminDSN: db.DSN,
		Shards:   4,
		Scale:    0.05,
		Rate:     40,
		Conns:    3,
		Seed:     7,
		Duration: 4 * time.Second,
		Logf:     t.Logf,
	})
	if err != nil {
		t.Fatalf("devtraffic.Run: %v", err)
	}
	if stats.Errors != 0 || stats.Statements < 100 {
		t.Fatalf("generator stats %+v, want no errors and at least 100 statements", stats)
	}

	// pg_stat_statements itself: many commented entries from both app roles,
	// with the comment where Auto puts it for this version.
	pos := devtraffic.PositionFor(devtraffic.Auto, version*10000)
	var webEntries, jobEntries int
	if err := su.QueryRow(ctx, `
select count(*) filter (where r.rolname = $1 and s.query like $3),
       count(*) filter (where r.rolname = $2 and s.query like $4)
  from pg_stat_statements s join pg_roles r on r.oid = s.userid`,
		devtraffic.WebRole, devtraffic.JobRole, likeAt(pos, "/*action:%controller:%*/"), likeAt(pos, "/*context_id:%job_tag:%*/")).Scan(&webEntries, &jobEntries); err != nil {
		t.Fatal(err)
	}
	if webEntries < 40 || jobEntries < 15 {
		t.Fatalf("pg_stat_statements has %d web and %d job commented entries, want at least 40 and 15", webEntries, jobEntries)
	}

	// Let the worker harvest what the generator did, then stop it.
	time.Sleep(2500 * time.Millisecond)
	w.StopAfterCurrent()
	select {
	case err := <-ran:
		if err != nil {
			t.Fatalf("worker Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker didn't stop")
	}

	sub := &collectingSubmitter{}
	if _, err := worker.NewOutboxSender(store, sub, nil).Drain(ctx); err != nil {
		t.Fatal(err)
	}

	// Each shape's fingerprint, and the contexts the catalog runs it under.
	r := rand.New(rand.NewPCG(9, 9))
	pool := devtraffic.NewHostPool(r)
	sz := devtraffic.SizesFor(0.05)
	shapeOf := map[string]string{}
	allowed := map[string]map[string]bool{}
	for _, c := range devtraffic.Contexts() {
		for _, name := range c.Shapes {
			shape, _ := devtraffic.ShapeByName(name)
			stmt, _ := shape.Render(c, pool.NewRequest(r, c), devtraffic.ShardSchema(1), pos, r, sz)
			fp, err := fingerprint.Normalized(stmt, fpOpts)
			if err != nil {
				t.Fatal(err)
			}
			shapeOf[fp] = name
			if allowed[fp] == nil {
				allowed[fp] = map[string]bool{}
			}
			allowed[fp][contextKey(c.Controller, c.Action, c.JobTag)] = true
		}
	}

	seen := map[string]map[string]bool{}
	var webSeen, jobSeen bool
	var taggedCalls, untaggedCalls uint64
	for _, msg := range sub.msgs {
		for _, agg := range msg.GetAggregates() {
			fp := agg.GetFingerprint()
			name, ok := shapeOf[fp]
			if !ok {
				continue
			}
			if seen[fp] == nil {
				seen[fp] = map[string]bool{}
			}
			for _, qc := range agg.GetContexts() {
				k := contextKey(qc.GetController(), qc.GetAction(), qc.GetJobTag())
				// Untagged is pssc not attributing calls (e.g. calls that
				// finished between the pgss and pssc reads), never a wrong
				// attribution, so any shape may have it.
				if k == contextKey("", "", "") {
					untaggedCalls += qc.GetCount()
					continue
				}
				taggedCalls += qc.GetCount()
				if !allowed[fp][k] {
					t.Errorf("shape %s attributed to context %q, which never runs it", name, k)
				}
				seen[fp][k] = true
				if qc.GetJobTag() != "" {
					jobSeen = true
				} else {
					webSeen = true
				}
			}
		}
	}
	if len(seen) < 20 {
		t.Fatalf("worker saw %d of %d generator fingerprints, want at least 20", len(seen), len(devtraffic.Shapes()))
	}
	if untaggedCalls*10 > taggedCalls {
		t.Fatalf("untagged calls %d against tagged %d, want under a tenth", untaggedCalls, taggedCalls)
	}
	if !webSeen || !jobSeen {
		t.Fatalf("worker saw web contexts %v and job contexts %v, want both", webSeen, jobSeen)
	}
	multi := 0
	for fp, ctxs := range seen {
		if len(ctxs) >= 3 {
			multi++
		}
		t.Logf("%-28s %d contexts", shapeOf[fp], len(ctxs))
	}
	if multi < 8 {
		t.Fatalf("%d fingerprints have 3 or more contexts, want at least 8", multi)
	}
}
