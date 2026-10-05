package devtraffic_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/devtraffic"
	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/testdb"
)

// TestRunSplitsBetweenPrimaryAndReplica runs the generator against the dev
// stack's primary and streaming replica, and checks each side's
// pg_stat_statements: primary-only shapes never run on the replica,
// replica-only shapes never run on the primary, and split shapes run on
// both.
func TestRunSplitsBetweenPrimaryAndReplica(t *testing.T) {
	ctx := context.Background()
	pair := testdb.StartDevObservedPair(t, nil)
	stats, err := devtraffic.Run(ctx, devtraffic.Config{
		AdminDSN:   pair.Primary.DSN,
		ReplicaDSN: pair.Replica.DSN,
		Shards:     2,
		Scale:      0.05,
		Rate:       60,
		Conns:      3,
		Seed:       11,
		Duration:   5 * time.Second,
		Logf:       t.Logf,
	})
	if err != nil {
		t.Fatalf("devtraffic.Run: %v", err)
	}
	if stats.Errors != 0 || stats.ReplicaStatements < 50 || stats.Statements-stats.ReplicaStatements < 50 {
		t.Fatalf("generator stats %+v, want no errors and at least 50 statements on each side", stats)
	}

	fpOpts := devFingerprintOptions(t)
	r := rand.New(rand.NewPCG(9, 9))
	pool := devtraffic.NewHostPool(r)
	sz := devtraffic.SizesFor(0.05)
	shapeOf := map[string]devtraffic.Shape{}
	for _, c := range devtraffic.Contexts() {
		for _, name := range c.Shapes {
			s, _ := devtraffic.ShapeByName(name)
			stmt, _ := s.Render(c, pool.NewRequest(r, c), devtraffic.ShardSchema(1), devtraffic.Trailing, r, sz)
			fp, err := fingerprint.Normalized(stmt, fpOpts)
			if err != nil {
				t.Fatal(err)
			}
			shapeOf[fp] = s
		}
	}

	ran := func(db *testdb.DB) map[string]bool {
		conn := db.Connect(t)
		rows, err := conn.Query(ctx, `select s.query from pg_stat_statements s join pg_roles r on r.oid = s.userid where r.rolname = any($1)`,
			[]string{devtraffic.WebRole, devtraffic.JobRole})
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				t.Fatal(err)
			}
			fp, err := fingerprint.Normalized(q, fpOpts)
			if err != nil {
				continue
			}
			if s, ok := shapeOf[fp]; ok {
				out[s.Name] = true
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	onPrimary, onReplica := ran(pair.Primary), ran(pair.Replica)
	t.Logf("primary ran %d shapes, replica %d", len(onPrimary), len(onReplica))

	split := 0
	for _, s := range devtraffic.Shapes() {
		switch s.RouteOf() {
		case devtraffic.OnPrimary:
			if onReplica[s.Name] {
				t.Errorf("primary-only shape %s ran on the replica", s.Name)
			}
		case devtraffic.OnReplica:
			if onPrimary[s.Name] {
				t.Errorf("replica-only shape %s ran on the primary", s.Name)
			}
			if !onReplica[s.Name] {
				t.Errorf("replica-only shape %s never ran on the replica", s.Name)
			}
		case devtraffic.Split:
			if onPrimary[s.Name] && onReplica[s.Name] {
				split++
			}
		}
		if onReplica[s.Name] && !s.ReadOnly {
			t.Errorf("shape %s writes but ran on the replica", s.Name)
		}
	}
	if split < 5 {
		t.Fatalf("%d split shapes ran on both sides, want at least 5", split)
	}
}
