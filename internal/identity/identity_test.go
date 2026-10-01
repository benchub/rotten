package identity

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/internal/testdb"
)

// The sample regexes from the descriptions in conf.
const (
	sampleController = `/\*.*controller(_with_namespace)?:([^,]+).*\*/`
	sampleAction     = `/\*.*action:([^,]+).*\*/`
	sampleJob        = `/\*.*job(_tag)?:([^,]+).*\*/`
)

// startIdentityDB starts the rotten DB, loads the schema, and returns a pool.
func startIdentityDB(t *testing.T) (*testdb.DB, *pgxpool.Pool) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test skipped under -short")
	}
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	sql, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "schema", "tables.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), string(sql)); err != nil {
		t.Fatalf("load schema: %v", err)
	}
	// New connections pick up the database's search_path, which includes
	// rotten, the same way production sessions do.
	pool, err := pgxpool.New(context.Background(), db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return db, pool
}

func sampleRegexes(t *testing.T) (c, a, j *regexp.Regexp) {
	t.Helper()
	c, a, j, err := CompileRegexes(sampleController, sampleAction, sampleJob)
	if err != nil {
		t.Fatal(err)
	}
	return c, a, j
}

func rowID(pool *pgxpool.Pool, table, col, val string) (uint32, error) {
	var id uint32
	q := "select id from rotten." + table + " where " + col + " = $1"
	err := pool.QueryRow(context.Background(), q, val).Scan(&id)
	return id, err
}

func count(t *testing.T, pool *pgxpool.Pool, q string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func TestCompileContextRegexes(t *testing.T) {
	c, a, j, err := CompileRegexes(sampleController, sampleAction, sampleJob)
	if err != nil {
		t.Fatalf("sample regexes: %v", err)
	}
	if c.String() != sampleController || a.String() != sampleAction || j.String() != sampleJob {
		t.Errorf("regexes returned in the wrong order: %q %q %q", c, a, j)
	}
	bad := "("
	for name, in := range map[string][3]string{
		"controller": {bad, sampleAction, sampleJob},
		"action":     {sampleController, bad, sampleJob},
		"job":        {sampleController, sampleAction, bad},
	} {
		if _, _, _, err := CompileRegexes(in[0], in[1], in[2]); err == nil {
			t.Errorf("%s: bad regex compiled without error", name)
		}
	}
}

func TestFindIdentityExtraction(t *testing.T) {
	_, pool := startIdentityDB(t)
	caches := NewCaches()
	reC, reA, reJ := sampleRegexes(t)

	web := "select 1 /*application:Canvas,controller:users,action:show*/"
	ns := "select 1 /*controller_with_namespace:api/v1/courses,action:index*/"
	jobTag := "select 1 /*job_tag:Delayed::Job#perform*/"
	job := "select 1 /*job:Other#run*/"

	// The last capture group wins, so the "_with_namespace" and "_tag"
	// groups never become the identity. Captures with no comma after them
	// stop before "*/" because the regex needs a closing \*/ to match.
	for _, tc := range []struct {
		name, table, col, want string
		got                    uint32
	}{
		{"controller", "controllers", "controller", "users", caches.Controllers.Find(pool, web, reC)},
		{"namespaced controller", "controllers", "controller", "api/v1/courses", caches.Controllers.Find(pool, ns, reC)},
		{"action", "actions", "action", "show", caches.Actions.Find(pool, web, reA)},
		{"job_tag", "job_tags", "job_tag", "Delayed::Job#perform", caches.JobTags.Find(pool, jobTag, reJ)},
		{"job", "job_tags", "job_tag", "Other#run", caches.JobTags.Find(pool, job, reJ)},
	} {
		if tc.got == 0 {
			t.Errorf("%s: got id 0", tc.name)
			continue
		}
		id, err := rowID(pool, tc.table, tc.col, tc.want)
		if err != nil {
			t.Errorf("%s: select %s %q: %v", tc.name, tc.table, tc.want, err)
			continue
		}
		if id != tc.got {
			t.Errorf("%s: got id %d, row for %q has id %d", tc.name, tc.got, tc.want, id)
		}
	}
}

func TestFindIdentityNoMatch(t *testing.T) {
	_, pool := startIdentityDB(t)
	caches := NewCaches()
	reC, reA, reJ := sampleRegexes(t)
	ev := "select 1"
	if id := caches.Controllers.Find(pool, ev, reC); id != 0 {
		t.Errorf("controller: got %d, want 0", id)
	}
	if id := caches.Actions.Find(pool, ev, reA); id != 0 {
		t.Errorf("action: got %d, want 0", id)
	}
	if id := caches.JobTags.Find(pool, ev, reJ); id != 0 {
		t.Errorf("job: got %d, want 0", id)
	}
	// A regex that matches but has no capture group also returns 0.
	if id := caches.Controllers.Find(pool, "select 1 /* x */", regexp.MustCompile(`/\*`)); id != 0 {
		t.Errorf("no capture group: got %d, want 0", id)
	}
	n := count(t, pool, `select (select count(*) from rotten.controllers)
		+ (select count(*) from rotten.actions) + (select count(*) from rotten.job_tags)`)
	if n != 0 {
		t.Errorf("no-match lookups inserted %d rows", n)
	}
}

func TestFindIdentityCacheHit(t *testing.T) {
	_, pool := startIdentityDB(t)
	caches := NewCaches()
	reC, _, _ := sampleRegexes(t)
	ev := "select 1 /*controller:users,action:show*/"
	first := caches.Controllers.Find(pool, ev, reC)
	if first == 0 {
		t.Fatal("first lookup got 0")
	}
	caches.Controllers.mu.Lock()
	cached, ok := caches.Controllers.m["users"]
	caches.Controllers.mu.Unlock()
	if !ok || cached != first {
		t.Fatalf("cache[users] = %d, %v; want %d", cached, ok, first)
	}
	// Delete the row. A cache hit returns the old ID and doesn't re-insert.
	if _, err := pool.Exec(context.Background(), "delete from rotten.controllers"); err != nil {
		t.Fatal(err)
	}
	if got := caches.Controllers.Find(pool, ev, reC); got != first {
		t.Errorf("second lookup got %d, want cached %d", got, first)
	}
	if n := count(t, pool, "select count(*) from rotten.controllers"); n != 0 {
		t.Errorf("cache hit touched the DB: %d rows", n)
	}
}

func TestFindIdentityReusesExistingRow(t *testing.T) {
	_, pool := startIdentityDB(t)
	caches := NewCaches()
	_, reA, _ := sampleRegexes(t)
	var want uint32
	err := pool.QueryRow(context.Background(),
		"insert into rotten.actions(id, action) values (4242, 'show') returning id").Scan(&want)
	if err != nil {
		t.Fatal(err)
	}
	got := caches.Actions.Find(pool, "select 1 /*controller:users,action:show*/", reA)
	if got != want {
		t.Errorf("got %d, want existing %d", got, want)
	}
	if n := count(t, pool, "select count(*) from rotten.actions"); n != 1 {
		t.Errorf("actions has %d rows, want 1", n)
	}
}

// TestFindIdentityInsertRace holds an uncommitted insert of the same value in
// another session. Find's select sees no row, and its insert blocks
// on the unique index. Once the other session commits, the insert fails and
// Find re-selects the winner's ID.
func TestFindIdentityInsertRace(t *testing.T) {
	db, pool := startIdentityDB(t)
	caches := NewCaches()
	_, _, reJ := sampleRegexes(t)
	ctx := context.Background()

	other := db.Connect(t)
	tx, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var winner uint32
	err = tx.QueryRow(ctx,
		"insert into rotten.job_tags(id, job_tag) values (777, 'Racy#go') returning id").Scan(&winner)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan uint32, 1)
	go func() {
		done <- caches.JobTags.Find(pool, "select 1 /*job:Racy#go*/", reJ)
	}()

	// Wait until Find's insert is blocked on the other session.
	// The query match depends on the SQL text Find builds
	// ("insert into job_tags(job_tag) ..."). If that text changes, update it.
	deadline := time.Now().Add(30 * time.Second)
	for count(t, pool, `select count(*) from pg_stat_activity
		where wait_event_type = 'Lock' and query like 'insert into job_tags%'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Find's insert never blocked")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got != winner {
			t.Errorf("got %d, want the winning session's id %d", got, winner)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Find didn't return")
	}
	if n := count(t, pool, "select count(*) from rotten.job_tags"); n != 1 {
		t.Errorf("job_tags has %d rows, want 1", n)
	}
}
