package devtraffic

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/testdb"
)

// TestEpisodesSlowTheirShapeInPgss checks that each episode makes its shape
// slower in pg_stat_statements' own exec time, not just for the client: the
// outliers report only sees what pgss measures. It runs a fixed number of
// calls of each target shape alone, outside and then inside its episode,
// through the generator's own request path, episode goroutine and lock
// holders.
//
// Slow reading counts only once the server blocks on a full socket, so the
// test reaches Postgres by its container address, not Docker's forwarded
// port (which buffers everything), with a small server send buffer, like a
// compose network's 1500-byte MTU gives. See testdb.SmallTCPSendBuffer.
func TestEpisodesSlowTheirShapeInPgss(t *testing.T) {
	db := testdb.StartObserved(t, 18, testdb.SmallTCPSendBuffer())
	dsn := db.ContainerDSN(t)
	admin := db.Connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := Config{AdminDSN: dsn, Shards: 1, Comments: Trailing, Seed: 7, Logf: t.Logf}
	cfg.defaults()
	g, err := newGenerator(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	r := rand.New(rand.NewPCG(7, 8))

	const calls = 4
	// mean runs calls calls of shape in episode ep and returns their mean
	// exec time in pgss, found by a fragment of the shape's text.
	mean := func(ep Episode, shape, fragment string) float64 {
		t.Helper()
		if _, err := admin.Exec(ctx, "select pg_stat_statements_reset()"); err != nil {
			t.Fatal(err)
		}
		g.cfg.Episode = ep
		ectx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		er := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))
		go func() {
			defer close(done)
			g.episodes(ectx, er)
		}()
		s, _ := ShapeByName(shape)
		c := s.RunBy()[0]
		for range calls {
			if ep == LockWait {
				waitForLockHolder(t, ctx, admin)
			}
			g.runOn(ctx, c, 1, []string{shape}, []Target{Primary}, r)
		}
		stop()
		<-done
		if n := g.n.errors.Load(); n > 0 {
			t.Fatalf("%d errors running %s in %q", n, shape, ep)
		}
		var total float64
		var n int64
		if err := admin.QueryRow(ctx, `
			select coalesce(sum(total_exec_time), 0), coalesce(sum(calls), 0)::bigint
			  from pg_stat_statements
			 where position($1 in query) > 0`, fragment).Scan(&total, &n); err != nil {
			t.Fatal(err)
		}
		if n != calls {
			t.Fatalf("%s in %q: pgss counted %d calls, want %d", shape, ep, n, calls)
		}
		t.Logf("%-16s in %-11q mean %8.2f ms", shape, ep, total/calls)
		return total / calls
	}

	for _, tc := range []struct {
		ep              Episode
		shape, fragment string
	}{
		{SlowRead, ExportShape, "ORDER BY c.id, u.sortable_name"},
		{LockWait, "touch_user", "SET last_seen_at = now() WHERE id"},
		{SlowSleep, "course_activity", "pg_sleep"},
	} {
		base := mean(NoEpisode, tc.shape, tc.fragment)
		got := mean(tc.ep, tc.shape, tc.fragment)
		if got < 100 || got < 10*base {
			t.Errorf("%q episode: %s's mean exec time is %.2f ms, want at least 100 ms and 10 times the %.2f ms baseline", tc.ep, tc.shape, got, base)
		}
	}
}

// waitForLockHolder waits until a lock holder has its transaction open, so
// the next touch_user waits for most of lockHold.
func waitForLockHolder(t *testing.T, ctx context.Context, admin *pgx.Conn) {
	t.Helper()
	for {
		var open bool
		if err := admin.QueryRow(ctx, `select exists (select from pg_stat_activity where usename = $1 and state = 'idle in transaction')`, JobRole).Scan(&open); err != nil {
			t.Fatal(err)
		}
		if open {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("no lock holder opened a transaction")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
