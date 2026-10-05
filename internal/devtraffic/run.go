package devtraffic

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config controls a generator run. Zero values take the defaults noted.
type Config struct {
	// AdminDSN connects as a role that can create roles and databases. The
	// load itself connects as WebRole and JobRole to the same host's shard
	// databases, with the password Setup gives them.
	AdminDSN string
	// ReplicaDSN, if set, is a streaming replica of AdminDSN's server. The
	// load connects to its host and port with the app roles, and runs each
	// shape on the primary or the replica as its Route says. It must be
	// able to call pg_last_wal_replay_lsn(), as any role can. Empty runs
	// everything on the primary.
	ReplicaDSN string
	// Shards is the number of shard databases (default 4).
	Shards int
	// Scale multiplies each shard's seeded size (default 1).
	Scale float64
	// Rate is requests and jobs started per second, on average (default 1).
	// Each runs 1 to 6 statements.
	Rate float64
	// Conns is the most connections each role holds to each shard
	// (default 1). Idle ones close after a minute.
	Conns int
	// Comments is where each statement's marginalia comment goes: Auto
	// (the default), Leading, or Trailing. See Position.
	Comments Position
	// Seed seeds the random choices; 0 picks one from the clock.
	Seed uint64
	// Duration stops the run after this long; 0 runs until ctx ends.
	Duration time.Duration
	// EpisodeEvery starts an Episode at each multiple of it since the Unix
	// epoch (see ScheduledEpisode); 0, the default, turns episodes off.
	EpisodeEvery time.Duration
	// EpisodeLength is how long each episode lasts (default 2 minutes). It
	// must be shorter than EpisodeEvery.
	EpisodeLength time.Duration
	// Episode, if set, runs that episode for the whole run instead of the
	// schedule, for tests and demos.
	Episode Episode
	// LogEvery is how often to log progress (default 1 minute).
	LogEvery time.Duration
	// Logf logs progress and errors. Nil discards.
	Logf func(format string, args ...any)
}

// Stats counts what a run did.
type Stats struct {
	Requests, Statements, Errors int64
	// ReplicaStatements is how many of Statements ran on the replica.
	ReplicaStatements int64
}

type counters struct {
	requests, statements, errors, replica atomic.Int64
}

func (c *counters) snapshot() Stats {
	return Stats{Requests: c.requests.Load(), Statements: c.statements.Load(), Errors: c.errors.Load(), ReplicaStatements: c.replica.Load()}
}

func (cfg *Config) defaults() {
	if cfg.Shards <= 0 {
		cfg.Shards = 4
	}
	if cfg.Scale <= 0 {
		cfg.Scale = 1
	}
	if cfg.Rate <= 0 {
		cfg.Rate = 1
	}
	if cfg.Conns <= 0 {
		cfg.Conns = 1
	}
	if cfg.Comments == "" {
		cfg.Comments = Auto
	}
	if cfg.Seed == 0 {
		cfg.Seed = uint64(time.Now().UnixNano())
	}
	if cfg.EpisodeEvery > 0 && cfg.EpisodeLength == 0 {
		cfg.EpisodeLength = 2 * time.Minute
	}
	if cfg.LogEvery <= 0 {
		cfg.LogEvery = time.Minute
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
}

// Run sets up the schema (see Setup), then generates load until ctx ends or
// cfg.Duration passes. Statement errors are counted and logged, not
// returned; Run returns an error only when setup or connecting fails.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	cfg.defaults()
	switch cfg.Comments {
	case Auto, Leading, Trailing:
	default:
		return Stats{}, fmt.Errorf("devtraffic: comment position %q, want auto, leading or trailing", cfg.Comments)
	}
	switch {
	case !validEpisode(cfg.Episode):
		return Stats{}, fmt.Errorf("devtraffic: episode %q, want one of %v", cfg.Episode, Episodes())
	case cfg.EpisodeEvery < 0:
		return Stats{}, fmt.Errorf("devtraffic: episode interval %v is negative", cfg.EpisodeEvery)
	case cfg.EpisodeEvery > 0 && (cfg.EpisodeLength <= 0 || cfg.EpisodeLength >= cfg.EpisodeEvery):
		return Stats{}, fmt.Errorf("devtraffic: episode length %v, want more than 0 and less than the %v interval", cfg.EpisodeLength, cfg.EpisodeEvery)
	}
	g, err := newGenerator(ctx, cfg)
	if err != nil {
		return Stats{}, err
	}
	defer g.close()
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}
	g.warmup(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	er := g.childRand()
	go func() {
		defer wg.Done()
		g.episodes(ctx, er)
	}()
	g.loop(ctx)
	wg.Wait()
	st := g.n.snapshot()
	g.cfg.Logf("devtraffic: stopped after %d requests, %d statements (%d on the replica), %d errors", st.Requests, st.Statements, st.ReplicaStatements, st.Errors)
	return st, nil
}

// newGenerator sets up the schema (see Setup) and connects. cfg has its
// defaults. Close it when done.
func newGenerator(ctx context.Context, cfg Config) (*generator, error) {
	sz, err := Setup(ctx, cfg.AdminDSN, cfg.Shards, cfg.Scale)
	if err != nil {
		return nil, err
	}
	if cfg.ReplicaDSN != "" {
		if err := waitForReplay(ctx, cfg.AdminDSN, cfg.ReplicaDSN, cfg.Logf); err != nil {
			return nil, err
		}
	}
	g := &generator{cfg: cfg, sz: sz, rnd: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)), started: time.Now()}
	// pools[target][shard-1][0] is WebRole's, [1] JobRole's. There are no
	// replica pools without a ReplicaDSN.
	targets := []Target{Primary}
	if cfg.ReplicaDSN != "" {
		targets = append(targets, Replica)
	}
	for _, target := range targets {
		dsn := cfg.AdminDSN
		if target == Replica {
			dsn = cfg.ReplicaDSN
		}
		g.pools[target] = make([][2]*pgxpool.Pool, cfg.Shards)
		for i := range g.pools[target] {
			for j, role := range []string{WebRole, JobRole} {
				if g.pools[target][i][j], err = shardPool(ctx, cfg, dsn, ShardSchema(i+1), role); err != nil {
					g.close()
					return nil, fmt.Errorf("%w (%s)", err, target)
				}
			}
		}
	}
	var version int
	if err := g.pools[Primary][0][0].QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		g.close()
		return nil, fmt.Errorf("devtraffic: server version: %w", err)
	}
	g.pos = PositionFor(cfg.Comments, version)
	g.hosts = NewHostPool(g.rnd)
	cfg.Logf("devtraffic: %d shards of %d users and %d courses, %.2g requests/s, up to %d connections per role per shard, %s comments (server %d), seed %d",
		cfg.Shards, sz.Users, sz.Courses, cfg.Rate, cfg.Conns, g.pos, version, cfg.Seed)
	if g.hasReplica() {
		cfg.Logf("devtraffic: splitting reads between the primary and the replica; see dev/README.md")
	} else {
		cfg.Logf("devtraffic: no replica, so everything runs on the primary")
	}
	if g.pos == Leading && version >= 180000 {
		cfg.Logf("devtraffic: Postgres 18 drops leading comments from pg_stat_statements, so rotten will see no contexts")
	}
	return g, nil
}

func (g *generator) close() {
	for _, side := range g.pools {
		for _, p := range side {
			for _, pool := range p {
				if pool != nil {
					pool.Close()
				}
			}
		}
	}
}

func (g *generator) hasReplica() bool { return g.pools[Replica] != nil }

// waitForReplay waits until the replica has replayed the primary's WAL as of
// now, so the shard databases and roles Setup just made are there.
func waitForReplay(ctx context.Context, primaryDSN, replicaDSN string, logf func(string, ...any)) error {
	primary, err := pgx.Connect(ctx, primaryDSN)
	if err != nil {
		return fmt.Errorf("devtraffic: connect to the primary: %w", err)
	}
	defer primary.Close(context.Background())
	var lsn string
	if err := primary.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return fmt.Errorf("devtraffic: primary WAL position: %w", err)
	}
	replica, err := pgx.Connect(ctx, replicaDSN)
	if err != nil {
		return fmt.Errorf("devtraffic: connect to the replica: %w", err)
	}
	defer replica.Close(context.Background())
	start := time.Now()
	logged := false
	for {
		var caughtUp bool
		if err := replica.QueryRow(ctx, `SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)`, lsn).Scan(&caughtUp); err != nil {
			return fmt.Errorf("devtraffic: replica replay position: %w", err)
		}
		if caughtUp {
			return nil
		}
		if time.Since(start) > replayWait {
			return fmt.Errorf("devtraffic: the replica hasn't replayed the primary's setup (to %s) after %v", lsn, replayWait)
		}
		if !logged && time.Since(start) > 5*time.Second {
			logf("devtraffic: waiting for the replica to replay to %s", lsn)
			logged = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// replayWait is how long waitForReplay waits for the replica.
const replayWait = 2 * time.Minute

// receiveBuffer is the load's socket receive buffer size, in bytes.
const receiveBuffer = 16 << 10

func shardPool(ctx context.Context, cfg Config, dsn, database, role string) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("devtraffic: parse DSN: %w", err)
	}
	pc.ConnConfig.Database = database
	pc.ConnConfig.User = role
	pc.ConnConfig.Password = role
	pc.ConnConfig.RuntimeParams["application_name"] = "rotten-dev-traffic"
	// Every statement's text is unique (its comment has a fresh context ID),
	// so a prepared-statement cache would only churn.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	// A small receive buffer makes a slowly read result back up into the
	// server within a few hundred kilobytes, so Postgres spends the
	// SlowRead episode blocked in execution, where pg_stat_statements
	// counts it. Small results don't notice.
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second,
		Control: func(_, _ string, c syscall.RawConn) error { return setReceiveBuffer(c, receiveBuffer) }}
	pc.ConnConfig.DialFunc = d.DialContext
	pc.MaxConns = int32(cfg.Conns)
	pc.MaxConnIdleTime = time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("devtraffic: pool for %s on %s: %w", role, database, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("devtraffic: connect as %s to %s: %w", role, database, err)
	}
	return pool, nil
}

type generator struct {
	cfg   Config
	sz    Sizes
	pos   Position
	pools [2][][2]*pgxpool.Pool
	hosts *HostPool
	rnd   *rand.Rand // only the dispatching goroutine uses it
	// started is when the load started, after setup.
	started time.Time
	n       counters
}

// warmup runs every shape once in each shard database as each role that
// runs it, on each server it runs on. pg_stat_statements keeps the first
// text it sees for an entry, and each (server, shard, role) is its own
// entry, so warmup picks that first text's context: the shard's turn in the
// list of contexts that run the shape as that role on that server. This
// spreads a shape's attributed contexts across its entries, and credits
// each server's entries only to contexts that really run the shape there.
func (g *generator) warmup(ctx context.Context) {
	targets := []Target{Primary}
	if g.hasReplica() {
		targets = append(targets, Replica)
	}
	for _, target := range targets {
		for shard := 1; shard <= g.cfg.Shards; shard++ {
			for _, shape := range shapes {
				for _, job := range []bool{false, true} {
					runners := runnersOf(shape.Name, job, target, g.hasReplica())
					if len(runners) == 0 {
						continue
					}
					if ctx.Err() != nil {
						return
					}
					c := runners[(shard-1)%len(runners)]
					g.runOn(ctx, c, shard, []string{shape.Name}, []Target{target}, g.childRand())
				}
			}
		}
	}
}

// runnersOf lists the job or web contexts that run the named shape on
// target. Without a replica, that's every context that runs it.
func runnersOf(name string, job bool, target Target, replica bool) []Context {
	shape, _ := ShapeByName(name)
	var out []Context
	for _, c := range shape.RunBy() {
		if c.IsJob() != job {
			continue
		}
		if replica {
			p := ReplicaPercent(c, shape)
			if (target == Replica && p == 0) || (target == Primary && p == 100) {
				continue
			}
		}
		out = append(out, c)
	}
	return out
}

func (g *generator) loop(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	sem := make(chan struct{}, 2*g.cfg.Conns*g.cfg.Shards)
	total := 0
	for _, c := range contexts {
		total += c.Weight
	}
	nextLog := time.Now().Add(g.cfg.LogEvery)
	for {
		wait := time.Duration(g.rnd.ExpFloat64() / g.cfg.Rate * float64(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if now := time.Now(); now.After(nextLog) {
			st := g.n.snapshot()
			g.cfg.Logf("devtraffic: %d requests, %d statements (%d on the replica), %d errors so far", st.Requests, st.Statements, st.ReplicaStatements, st.Errors)
			nextLog = now.Add(g.cfg.LogEvery)
		}
		pick := g.rnd.IntN(total)
		var c Context
		for _, c = range contexts {
			if pick < c.Weight {
				break
			}
			pick -= c.Weight
		}
		shard := 1 + g.rnd.IntN(g.cfg.Shards)
		r := g.childRand()
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			g.request(ctx, c, shard, r)
		}()
	}
}

// episodeAt is the episode running at t, and when it started: cfg.Episode
// for the whole run if set, otherwise the schedule's.
func (g *generator) episodeAt(t time.Time) (Episode, time.Time) {
	if g.cfg.Episode != NoEpisode {
		return g.cfg.Episode, g.started
	}
	return ScheduledEpisode(g.cfg.EpisodeEvery, g.cfg.EpisodeLength, t)
}

func (g *generator) childRand() *rand.Rand {
	return rand.New(rand.NewPCG(g.rnd.Uint64(), g.rnd.Uint64()))
}

// request runs one web request or job: c's shapes in order, each on the
// primary or the replica as PickTarget chooses, all with the same comment.
func (g *generator) request(ctx context.Context, c Context, shard int, r *rand.Rand) {
	targets := make([]Target, len(c.Shapes))
	if g.hasReplica() {
		for i, name := range c.Shapes {
			shape, _ := ShapeByName(name)
			targets[i] = PickTarget(c, shape, r)
		}
	}
	g.runOn(ctx, c, shard, c.Shapes, targets, r)
}

// runOn runs the named shapes in order in shard, as c's role, all with one
// fresh comment for c: names[i] on targets[i], or on targets[0] if there's
// just one. It holds at most one connection per server, for the whole run,
// and takes the primary's before the replica's, so two runs can't each hold
// the one the other waits for.
func (g *generator) runOn(ctx context.Context, c Context, shard int, names []string, targets []Target, r *rand.Rand) {
	role := 0
	if c.IsJob() {
		role = 1
	}
	targetOf := func(i int) Target {
		if len(targets) == 1 {
			return targets[0]
		}
		return targets[i]
	}
	var conns [2]*pgxpool.Conn
	for _, target := range []Target{Primary, Replica} {
		for i := range names {
			if targetOf(i) != target {
				continue
			}
			conn, err := g.pools[target][shard-1][role].Acquire(ctx)
			if err != nil {
				g.fail(ctx, c, "acquire "+target.String(), err)
				return
			}
			defer conn.Release()
			conns[target] = conn
			break
		}
	}
	g.n.requests.Add(1)
	meta := g.hosts.NewRequest(r, c)
	schema := ShardSchema(shard)
	ep, _ := g.episodeAt(time.Now())
	for i, name := range names {
		shape, _ := ShapeByName(name)
		sql, args := shape.RenderIn(c, meta, schema, g.pos, r, g.sz, ep)
		conn := conns[targetOf(i)]
		var err error
		if name == ExportShape {
			err = export(ctx, conn.Conn(), sql, args, ep == SlowRead)
		} else {
			_, err = conn.Exec(ctx, sql, args...)
		}
		if err != nil {
			g.fail(ctx, c, name+" on the "+targetOf(i).String(), err)
			return
		}
		g.n.statements.Add(1)
		if targetOf(i) == Replica {
			g.n.replica.Add(1)
		}
	}
}

// export reads an export's rows, pausing now and then if slow, like a job
// writing each batch somewhere slow.
func export(ctx context.Context, conn *pgx.Conn, sql string, args []any, slow bool) error {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for n := 1; rows.Next(); n++ {
		if slow && n%readEvery == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(readPause):
			}
		}
	}
	return rows.Err()
}

// episodes logs each episode as it starts and ends, and runs the LockWait
// lock holders, one per shard, while that episode lasts. r is its own.
func (g *generator) episodes(ctx context.Context, r *rand.Rand) {
	var cur Episode
	stop := func() {}
	defer func() { stop() }()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		ep, start := g.episodeAt(time.Now())
		if ep != cur {
			stop()
			stop = func() {}
			switch {
			case ep == NoEpisode:
				g.cfg.Logf("devtraffic: %s episode ended at %s", cur, time.Now().UTC().Format(time.RFC3339))
			case g.cfg.Episode != NoEpisode:
				g.cfg.Logf("devtraffic: %s episode for the whole run", ep)
			default:
				g.cfg.Logf("devtraffic: %s episode from %s to %s UTC; the outliers report shows it in any range that covers it",
					ep, start.Format("2006-01-02 15:04:05"), start.Add(g.cfg.EpisodeLength).Format("15:04:05"))
			}
			if ep == LockWait {
				stop = g.startHolders(ctx, r)
			}
			cur = ep
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// startHolders starts a lock holder on each shard, and returns a func that
// stops them and waits until they've committed.
func (g *generator) startHolders(ctx context.Context, r *rand.Rand) func() {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for shard := 1; shard <= g.cfg.Shards; shard++ {
		hr := rand.New(rand.NewPCG(r.Uint64(), r.Uint64()))
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.holdLocks(ctx, shard, hr)
		}()
	}
	return func() { cancel(); wg.Wait() }
}

// holdLocks is the LockWait episode's job on one shard, until ctx ends: it
// locks the hot users in a transaction, keeps it open for lockHold, commits,
// and lets go for lockGap. It has its own connection, so it doesn't starve
// the shard's job pool.
func (g *generator) holdLocks(ctx context.Context, shard int, r *rand.Rand) {
	conn, err := pgx.ConnectConfig(ctx, g.pools[Primary][shard-1][1].Config().ConnConfig.Copy())
	if err != nil {
		g.fail(ctx, lockHolder, "connect", err)
		return
	}
	defer conn.Close(context.Background())
	shape, _ := ShapeByName(LockShape)
	for ctx.Err() == nil {
		g.n.requests.Add(1)
		sql, args := shape.RenderIn(lockHolder, g.hosts.NewRequest(r, lockHolder), ShardSchema(shard), g.pos, r, g.sz, LockWait)
		tx, err := conn.Begin(ctx)
		if err != nil {
			g.fail(ctx, lockHolder, "begin", err)
			return
		}
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			_ = tx.Rollback(context.Background())
			g.fail(ctx, lockHolder, LockShape, err)
			return
		}
		g.n.statements.Add(1)
		sleep(ctx, lockHold)
		// Commit even if ctx ended, so the locks go now, not when the
		// connection closes.
		if err := tx.Commit(context.Background()); err != nil {
			g.fail(ctx, lockHolder, "commit", err)
			return
		}
		sleep(ctx, lockGap)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// fail counts and logs an error, unless it's just the run ending. It logs
// the first 20 errors, then one in 100.
func (g *generator) fail(ctx context.Context, c Context, what string, err error) {
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || pgconnTimeout(err)) {
		return
	}
	if n := g.n.errors.Add(1); n <= 20 || n%100 == 0 {
		g.cfg.Logf("devtraffic: %s %s: %v (error %d)", c.Label(), what, err, n)
	}
}

// pgconnTimeout reports whether err is pgconn's wrapper for a canceled
// context, which doesn't always unwrap to context.Canceled.
func pgconnTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}
