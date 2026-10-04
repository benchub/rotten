package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/harvestlimits"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/state"
	"github.com/benchub/rotten/internal/testdb"
)

const (
	sampleController = `/\*.*controller(_with_namespace)?:([^,]+).*\*/`
	sampleAction     = `/\*.*action:([^,]+).*\*/`
	sampleJob        = `/\*.*job(_tag)?:([^,]+).*\*/`
	diffQuery        = `select count(*) from widgets where id > 3 /*diff_marker*/`
)

type stepClock struct {
	sleeping chan time.Duration
	proceed  chan struct{}

	mu   sync.Mutex
	nows []int64
}

func (c *stepClock) Now() time.Time {
	n := time.Now()
	c.mu.Lock()
	c.nows = append(c.nows, n.Unix())
	c.mu.Unlock()
	return n
}

func (c *stepClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case c.sleeping <- d:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-c.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return RealClock{}.Sleep(ctx, d)
}

func (c *stepClock) waitSleep(t *testing.T) time.Duration {
	t.Helper()
	select {
	case d := <-c.sleeping:
		return d
	case <-time.After(30 * time.Second):
		t.Fatal("worker never reached its sleep")
		return 0
	}
}

func sampleRegexes(t *testing.T) (c, a, j *regexp.Regexp) {
	t.Helper()
	var err error
	if c, err = regexp.Compile(sampleController); err != nil {
		t.Fatal(err)
	}
	if a, err = regexp.Compile(sampleAction); err != nil {
		t.Fatal(err)
	}
	if j, err = regexp.Compile(sampleJob); err != nil {
		t.Fatal(err)
	}
	return c, a, j
}

func startObservedForWorker(t *testing.T) *testdb.DB {
	t.Helper()
	return startObservedVersionForWorker(t, 16)
}

func startObservedVersionForWorker(t *testing.T, version int) *testdb.DB {
	t.Helper()
	db := testdb.StartObserved(t, version)
	if out, err := db.PSQL(t, filepath.Join(testdb.RepoRoot(), "schema", "observer.sql"), nil); err != nil {
		t.Fatalf("observer.sql: %v\n%s", err, out)
	}
	conn := db.Connect(t)
	ctx := context.Background()
	for _, s := range []string{
		"alter role rotten_observer password '" + db.RolePassword("rotten_observer") + "'",
		"create table widgets (id int primary key, name text)",
		"insert into widgets select g, 'w' || g from generate_series(1, 10) g",
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%.60s: %v", s, err)
		}
	}
	return db
}

func observerConn(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func observerConnector(dsn func() string) ObservedConnector {
	return func(ctx context.Context) (*pgx.Conn, error) {
		cfg, err := pgx.ParseConfig(dsn())
		if err != nil {
			return nil, err
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
		return pgx.ConnectConfig(ctx, cfg)
	}
}

func runDiffWorkload(t *testing.T, conn *pgx.Conn, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rows, err := conn.Query(context.Background(), diffQuery, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func statsReset(t *testing.T, conn *pgx.Conn) time.Time {
	t.Helper()
	var ts time.Time
	if err := conn.QueryRow(context.Background(), `select stats_reset from pg_stat_statements_info`).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	return ts
}

func openStore(t *testing.T, dir string) *state.Store {
	t.Helper()
	s, err := state.Open(dir, state.Options{MaxSnapshotAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type running struct {
	w       *Worker
	clk     *stepClock
	cancel  context.CancelFunc
	ran     chan error
	harvest []int64
}

func startOutboxWorker(t *testing.T, obsDSN string, logical, physical uint32, snapshot StateStore, outbox ServerOutboxStore) *running {
	t.Helper()
	reC, reA, reJ := sampleRegexes(t)
	cfg := Config{
		ObservedDB:          observerConn(t, obsDSN),
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           logical,
		PhysicalID:          physical,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		State:               snapshot,
		ServerOutbox:        outbox,
	}
	clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
	w := New(cfg, clk)
	ctx, cancel := context.WithCancel(context.Background())
	rn := &running{w: w, clk: clk, cancel: cancel, ran: make(chan error, 1)}
	go func() { rn.ran <- w.Run(ctx) }()
	t.Cleanup(func() { rn.stop(t) })
	clk.waitSleep(t)
	rn.harvest = append(rn.harvest, w.lastHarvest.Load())
	return rn
}

type faultOutboxStore struct {
	inner                        *state.Store
	failLoad, failSaveAndEnqueue bool
}

func (f *faultOutboxStore) Load(ctx context.Context) (state.Loaded, error) {
	if f.failLoad {
		return state.Loaded{}, errors.New("injected load failure")
	}
	return f.inner.Load(ctx)
}

func (f *faultOutboxStore) Save(ctx context.Context, snap pgss.Snapshot, takenAt time.Time) error {
	return f.inner.Save(ctx, snap, takenAt)
}

func (f *faultOutboxStore) SaveSnapshotAndEnqueue(ctx context.Context, snap pgss.Snapshot, takenAt time.Time, batch *rottenv1.SubmitHarvestRequest) (state.OutboxEnqueueResult, error) {
	if f.failSaveAndEnqueue {
		return state.OutboxEnqueueResult{}, errors.New("injected save and enqueue failure")
	}
	return f.inner.SaveSnapshotAndEnqueue(ctx, snap, takenAt, batch)
}

func (rn *running) window(t *testing.T) int64 {
	t.Helper()
	rn.clk.proceed <- struct{}{}
	rn.clk.waitSleep(t)
	h := rn.w.lastHarvest.Load()
	if h <= rn.harvest[len(rn.harvest)-1] {
		t.Fatalf("harvest time %d didn't move past %d", h, rn.harvest[len(rn.harvest)-1])
	}
	rn.harvest = append(rn.harvest, h)
	return h
}

func (rn *running) stop(t *testing.T) {
	if rn.cancel == nil {
		return
	}
	rn.cancel()
	rn.cancel = nil
	select {
	case err := <-rn.ran:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("run returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("run didn't return after cancel")
	}
}

func TestWorkerOutboxHarvestQueuesValidPayloadsInWindowOrder(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	workload := observed.Connect(t)

	rn := startOutboxWorker(t, observed.DSNAs(t, "rotten_observer"), 7, 42, store, store)
	base := rn.harvest[0]
	runDiffWorkload(t, workload, 1)
	h1 := rn.window(t)
	runDiffWorkload(t, workload, 2)
	rn.window(t)
	runDiffWorkload(t, workload, 3)
	rn.window(t)
	rn.stop(t)

	first, err := store.NextOutboxBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("outbox is empty, want first queued harvest")
	}
	var msg rottenv1.SubmitHarvestRequest
	if err := proto.Unmarshal(first.Payload, &msg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := harvestlimits.ValidateHarvest(&msg, time.Now().UTC(), harvestlimits.CheckFutureSkew); err != nil {
		t.Fatalf("queued payload failed harvest validation: %v", err)
	}
	wantFingerprint := fingerprintOf(t, diffQuery)
	found := false
	for _, aggregate := range msg.GetAggregates() {
		if aggregate.GetFingerprint() == wantFingerprint {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("queued harvest fingerprints do not include workload %s: %+v", wantFingerprint, msg.GetAggregates())
	}

	client := &fakeSubmitter{}
	sender := NewOutboxSender(store, client, nil)
	if sent, err := sender.Drain(context.Background()); err != nil || sent != 3 {
		t.Fatalf("Drain sent=%d err=%v, want three queued harvests", sent, err)
	}
	if len(client.batchIDs) != 3 {
		t.Fatalf("sent batch count = %d, want 3", len(client.batchIDs))
	}
	var lastEnd int64
	for i, batchID := range client.batchIDs {
		var gotPhysical uint32
		var start, end int64
		if _, err := fmt.Sscanf(batchID, "%d:%d:%d", &gotPhysical, &start, &end); err != nil {
			t.Fatalf("parse batch_id %q: %v", batchID, err)
		}
		if gotPhysical != 42 {
			t.Fatalf("batch %d physical = %d, want 42", i, gotPhysical)
		}
		if i == 0 {
			if time.UnixMicro(start).Unix() != base || time.UnixMicro(end).Unix() != h1 {
				t.Fatalf("first batch window seconds = [%d,%d], want [%d,%d]", time.UnixMicro(start).Unix(), time.UnixMicro(end).Unix(), base, h1)
			}
		} else if start != lastEnd {
			t.Fatalf("batch %d starts at %d, want previous end %d; order=%v", i, start, lastEnd, client.batchIDs)
		}
		if end <= start {
			t.Fatalf("batch %d window [%d,%d] is not increasing", i, start, end)
		}
		lastEnd = end
	}
}

func TestOutboxHarvestAdvancesProgressCountersAndContexts(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	workload := observed.Connect(t)
	rn := startOutboxWorker(t, observed.DSNAs(t, "rotten_observer"), 7, 42, store, store)

	query := `select count(*) from widgets where id > 3 /*controller:users,action:show,job:CleanupJob*/`
	for i := 0; i < 2; i++ {
		rows, err := workload.Query(context.Background(), query, pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	rn.window(t)
	rn.stop(t)

	if got := rn.w.eventCount.Load(); got == 0 {
		t.Fatal("eventCount did not advance for a non-baseline outbox harvest")
	}
	batches := drainOutboxBatches(t, store)
	aggregate := findAggregate(t, batches, fingerprintOf(t, query))
	if len(aggregate.GetContexts()) != 1 {
		t.Fatalf("contexts = %+v, want one context", aggregate.GetContexts())
	}
	ctx := aggregate.GetContexts()[0]
	if ctx.GetController() != "users" || ctx.GetAction() != "show" || ctx.GetJobTag() != "CleanupJob" || ctx.GetCount() != 2 {
		t.Fatalf("context = %+v, want users/show/CleanupJob count 2", ctx)
	}
}

func TestBuildHarvestBatchLogsHiddenAndNoText(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42, Fingerprint: fingerprinting.Options{}}, RealClock{})
	withText := pgss.Stat{UserID: 1, DBID: 1, TopLevel: true, QueryID: 10, Calls: 1, TotalExecTime: 1}
	noText := pgss.Stat{UserID: 1, DBID: 1, TopLevel: true, QueryID: 11, Calls: 1, TotalExecTime: 2}
	hidden := pgss.Stat{UserID: 1, DBID: 1, TopLevel: true, QueryID: 0, Calls: 1, TotalExecTime: 3}
	rows := []pgss.Stat{withText, noText, hidden}
	rows[0].Query = "select 1"
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(old) })
	_ = w.buildHarvestBatchFromRows([]pgss.Delta{{Stat: withText}, {Stat: noText}, {Stat: hidden}}, rows, time.Unix(1, 0), time.Unix(2, 0))
	if !strings.Contains(logs.String(), "top entries are hidden from the observer") {
		t.Fatalf("missing hidden log line: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "top entries have no query text") {
		t.Fatalf("missing no-text log line: %s", logs.String())
	}
}

type sequenceTextFiller struct {
	failures int
	texts    map[int64]string
}

func (f *sequenceTextFiller) Fill(ctx context.Context, stats []pgss.Stat) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("injected text fetch failure")
	}
	for i := range stats {
		stats[i].Query = f.texts[stats[i].QueryID]
	}
	return nil
}

func TestBuildHarvestBatchRetriesTextFetch(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42, Fingerprint: fingerprinting.Options{}}, RealClock{})
	key := pgss.Key{UserID: 1, DBID: 1, TopLevel: true, QueryID: 201}
	delta := pgss.Delta{Stat: pgss.Stat{UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 3, TotalExecTime: 3, TotalTime: 3, MeanTime: 1}, New: true}
	filler := &sequenceTextFiller{
		failures: 1,
		texts: map[int64]string{
			key.QueryID: "select 201",
		},
	}
	batch, _ := w.buildHarvestBatchAndSnapshot(context.Background(), filler, pgss.Snapshot{}, pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{key: delta.Stat}}, []pgss.Delta{delta}, time.Unix(10, 0), time.Unix(20, 0))
	aggregate := findAggregate(t, []*rottenv1.SubmitHarvestRequest{batch}, fingerprintOf(t, "select 201"))
	if got := aggregate.GetMetrics().GetCalls(); got != 3 {
		t.Fatalf("calls = %d, want 3 after retry", got)
	}
}

func TestBuildHarvestBatchCarriesSkippedTextDeltas(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42, Fingerprint: fingerprinting.Options{}}, RealClock{})
	info := pgss.Info{StatsReset: time.Unix(1, 0)}
	t0 := time.Unix(9, 0)
	tAfterSecondWindow := time.Unix(31, 0)
	existingKey := pgss.Key{UserID: 1, DBID: 1, TopLevel: true, QueryID: 101}
	newKey := pgss.Key{UserID: 1, DBID: 1, TopLevel: true, QueryID: 102}
	prev := pgss.Snapshot{
		Info: info,
		Entries: map[pgss.Key]pgss.Stat{
			existingKey: {UserID: existingKey.UserID, DBID: existingKey.DBID, TopLevel: existingKey.TopLevel, QueryID: existingKey.QueryID, Calls: 10, TotalExecTime: 10, TotalTime: 10, MeanTime: 1, MinmaxStatsSince: &t0},
		},
	}
	firstStats := []pgss.Stat{
		{UserID: existingKey.UserID, DBID: existingKey.DBID, TopLevel: existingKey.TopLevel, QueryID: existingKey.QueryID, Calls: 13, TotalExecTime: 13, TotalTime: 13, MeanTime: 1, MinmaxStatsSince: &t0},
		{UserID: newKey.UserID, DBID: newKey.DBID, TopLevel: newKey.TopLevel, QueryID: newKey.QueryID, Calls: 4, TotalExecTime: 4, TotalTime: 4, MeanTime: 1, MinmaxStatsSince: &t0},
	}
	firstDeltas, firstNext := pgss.Diff(prev, firstStats, info)
	filler := &sequenceTextFiller{
		failures: textFetchAttempts,
		texts: map[int64]string{
			existingKey.QueryID: "select id from widgets where id = 101",
			newKey.QueryID:      "select name from widgets where id = 102",
		},
	}
	firstBatch, carried := w.buildHarvestBatchAndSnapshot(context.Background(), filler, prev, firstNext, firstDeltas, time.Unix(10, 0), time.Unix(20, 0))
	if len(firstBatch.GetAggregates()) != 0 {
		t.Fatalf("failed text fetch batch has %d aggregates, want 0", len(firstBatch.GetAggregates()))
	}

	secondStats := []pgss.Stat{
		{UserID: existingKey.UserID, DBID: existingKey.DBID, TopLevel: existingKey.TopLevel, QueryID: existingKey.QueryID, Calls: 15, TotalExecTime: 15, TotalTime: 15, MeanTime: 1, MinmaxStatsSince: &tAfterSecondWindow},
		{UserID: newKey.UserID, DBID: newKey.DBID, TopLevel: newKey.TopLevel, QueryID: newKey.QueryID, Calls: 6, TotalExecTime: 6, TotalTime: 6, MeanTime: 1, MinmaxStatsSince: &tAfterSecondWindow},
	}
	secondDeltas, secondNext := pgss.Diff(carried, secondStats, info)
	secondBatch, _ := w.buildHarvestBatchAndSnapshot(context.Background(), filler, carried, secondNext, secondDeltas, time.Unix(20, 0), time.Unix(30, 0))
	got := map[string]uint64{}
	for _, aggregate := range secondBatch.GetAggregates() {
		got[aggregate.GetFingerprint()] = aggregate.GetMetrics().GetCalls()
	}
	existingFingerprint := fingerprintOf(t, "select id from widgets where id = 101")
	newFingerprint := fingerprintOf(t, "select name from widgets where id = 102")
	if got[existingFingerprint] != 5 {
		t.Fatalf("existing query calls = %d, want 5 after carry; aggregates = %+v", got[existingFingerprint], got)
	}
	if got[newFingerprint] != 6 || len(got) != 2 {
		t.Fatalf("aggregates = %+v, want carried query totals 5 and 6", got)
	}
	for _, aggregate := range secondBatch.GetAggregates() {
		if !aggregate.GetMinmaxLifetime() {
			t.Fatalf("aggregate %+v has minmax_lifetime false, want true because the min/max reset happened during the skipped harvest", aggregate)
		}
	}
}

func TestBuildHarvestBatchEmptyTextSuccessAdvancesSnapshot(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42, Fingerprint: fingerprinting.Options{}}, RealClock{})
	info := pgss.Info{StatsReset: time.Unix(1, 0)}
	key := pgss.Key{UserID: 1, DBID: 1, TopLevel: true, QueryID: 301}
	prev := pgss.Snapshot{
		Info: info,
		Entries: map[pgss.Key]pgss.Stat{
			key: {UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 10, TotalExecTime: 10, TotalTime: 10, MeanTime: 1},
		},
	}
	firstStats := []pgss.Stat{
		{UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 13, TotalExecTime: 13, TotalTime: 13, MeanTime: 1},
	}
	firstDeltas, firstNext := pgss.Diff(prev, firstStats, info)
	emptyText := &sequenceTextFiller{
		texts: map[int64]string{
			key.QueryID: "",
		},
	}
	firstBatch, advanced := w.buildHarvestBatchAndSnapshot(context.Background(), emptyText, prev, firstNext, firstDeltas, time.Unix(10, 0), time.Unix(20, 0))
	if len(firstBatch.GetAggregates()) != 0 {
		t.Fatalf("empty text batch has %d aggregates, want 0", len(firstBatch.GetAggregates()))
	}

	secondStats := []pgss.Stat{
		{UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 15, TotalExecTime: 15, TotalTime: 15, MeanTime: 1},
	}
	secondDeltas, secondNext := pgss.Diff(advanced, secondStats, info)
	goodText := &sequenceTextFiller{
		texts: map[int64]string{
			key.QueryID: "select id from widgets where id = 301",
		},
	}
	secondBatch, _ := w.buildHarvestBatchAndSnapshot(context.Background(), goodText, advanced, secondNext, secondDeltas, time.Unix(20, 0), time.Unix(30, 0))
	aggregate := findAggregate(t, []*rottenv1.SubmitHarvestRequest{secondBatch}, fingerprintOf(t, "select id from widgets where id = 301"))
	if got := aggregate.GetMetrics().GetCalls(); got != 2 {
		t.Fatalf("calls = %d, want only the second window's 2 calls after successful empty text", got)
	}
}

func TestCarryMinmaxSentinelSurvivesStatePersistence(t *testing.T) {
	store := openStore(t, t.TempDir())
	defer store.Close()
	key := pgss.Key{UserID: 1, DBID: 1, TopLevel: true, QueryID: 401}
	snap := pgss.Snapshot{
		Info: pgss.Info{StatsReset: time.Unix(1, 0)},
		Entries: map[pgss.Key]pgss.Stat{
			key: {UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 1, MinmaxStatsSince: &textCarryMinmaxSentinel},
		},
	}
	if err := store.Save(context.Background(), snap, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := loaded.Snapshot.Entries[key].MinmaxStatsSince
	if got == nil || !got.Equal(textCarryMinmaxSentinel) {
		t.Fatalf("persisted minmax sentinel = %v, want %v", got, textCarryMinmaxSentinel)
	}
	curReset := textCarryMinmaxSentinel.Add(-time.Second)
	delta := pgss.Delta{
		Stat: pgss.Stat{UserID: key.UserID, DBID: key.DBID, TopLevel: key.TopLevel, QueryID: key.QueryID, Calls: 1, MinmaxStatsSince: &curReset},
		Prev: &pgss.Stat{MinmaxStatsSince: got},
	}
	if _, _, lifetime := pgss.WindowMinMax(delta); !lifetime {
		t.Fatal("persisted carry sentinel did not force min/max to remain lifetime")
	}
}

func TestWorkerDiffingOutbox(t *testing.T) {
	for _, version := range []int{14, 18} {
		t.Run(fmt.Sprintf("pg%d", version), func(t *testing.T) {
			observed := startObservedVersionForWorker(t, version)
			su := observed.Connect(t)
			obsDSN := observed.DSNAs(t, "rotten_observer")

			t.Run("windows", func(t *testing.T) {
				resetBefore := statsReset(t, su)
				runDiffWorkload(t, su, 7)
				store := openStore(t, t.TempDir())
				defer store.Close()
				rn := startOutboxWorker(t, obsDSN, 7, 42, store, store)
				base := rn.harvest[0]
				runDiffWorkload(t, su, 3)
				h1 := rn.window(t)
				runDiffWorkload(t, su, 5)
				h2 := rn.window(t)
				h3 := rn.window(t)
				rn.stop(t)
				batches := drainOutboxBatches(t, store)
				expectBatchCalls(t, batches, base, h1, 3)
				expectBatchCalls(t, batches, h1, h2, 5)
				expectBatchCalls(t, batches, h2, h3, 0)
				if got := statsReset(t, su); !got.Equal(resetBefore) {
					t.Fatalf("stats_reset moved from %v to %v; worker must not run a full reset", resetBefore, got)
				}

				if version >= 17 {
					var moved bool
					if err := su.QueryRow(context.Background(), `select bool_and(minmax_stats_since > stats_since) from pg_stat_statements where query like '%diff_marker%'`).Scan(&moved); err != nil {
						t.Fatal(err)
					}
					if !moved {
						t.Fatal("minmax_stats_since never moved")
					}
				}
			})

			t.Run("outside reset", func(t *testing.T) {
				store := openStore(t, t.TempDir())
				defer store.Close()
				rn := startOutboxWorker(t, obsDSN, 8, 43, store, store)
				base := rn.harvest[0]
				runDiffWorkload(t, su, 3)
				h1 := rn.window(t)
				runDiffWorkload(t, su, 2)
				if _, err := su.Exec(context.Background(), `select pg_stat_statements_reset()`); err != nil {
					t.Fatal(err)
				}
				runDiffWorkload(t, su, 4)
				h2 := rn.window(t)
				runDiffWorkload(t, su, 6)
				h3 := rn.window(t)
				rn.stop(t)
				batches := drainOutboxBatches(t, store)
				expectBatchCalls(t, batches, base, h1, 3)
				expectBatchCalls(t, batches, h1, h2, 4)
				expectBatchCalls(t, batches, h2, h3, 6)
			})

			t.Run("restart", func(t *testing.T) {
				dir := t.TempDir()
				store := openStore(t, dir)
				rn := startOutboxWorker(t, obsDSN, 9, 44, store, store)
				base := rn.harvest[0]
				runDiffWorkload(t, su, 3)
				h1 := rn.window(t)
				rn.stop(t)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				runDiffWorkload(t, su, 4)
				time.Sleep(1100 * time.Millisecond)
				store = openStore(t, dir)
				defer store.Close()
				rn = startOutboxWorker(t, obsDSN, 9, 44, store, store)
				rn.stop(t)
				batches := drainOutboxBatches(t, store)
				expectBatchCalls(t, batches, base, h1, 3)
				expectBatchCalls(t, batches, h1, rn.harvest[0], 4)
			})

			t.Run("state errors", func(t *testing.T) {
				inner := openStore(t, t.TempDir())
				defer inner.Close()
				fs := &faultOutboxStore{inner: inner}
				rn := startOutboxWorker(t, obsDSN, 10, 45, fs, fs)
				base := rn.harvest[0]
				runDiffWorkload(t, su, 3)
				fs.failLoad = true
				h1 := rn.window(t)
				fs.failLoad = false
				runDiffWorkload(t, su, 5)
				h2 := rn.window(t)
				runDiffWorkload(t, su, 2)
				fs.failSaveAndEnqueue = true
				h3 := rn.window(t)
				fs.failSaveAndEnqueue = false
				runDiffWorkload(t, su, 6)
				h4 := rn.window(t)
				runDiffWorkload(t, su, 1)
				h5 := rn.window(t)
				rn.stop(t)
				batches := drainOutboxBatches(t, inner)
				expectNoBatchEnding(t, batches, base)
				expectNoBatchEnding(t, batches, h1)
				expectBatchCalls(t, batches, h1, h2, 5)
				expectNoBatchEnding(t, batches, h3)
				expectNoBatchEnding(t, batches, h4)
				expectBatchCalls(t, batches, h4, h5, 1)
			})
		})
	}
}

func TestWorkerReconnectsToObservedDatabaseAfterRestart(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()

	var currentDSN atomic.Value
	currentDSN.Store(observed.DSNAs(t, "rotten_observer"))
	var failedConnects atomic.Int32
	connector := func(ctx context.Context) (*pgx.Conn, error) {
		cfg, err := pgx.ParseConfig(currentDSN.Load().(string))
		if err != nil {
			return nil, err
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			failedConnects.Add(1)
		}
		return conn, err
	}
	reC, reA, reJ := sampleRegexes(t)
	cfg := Config{
		ObservedDBConnect:   connector,
		ReconnectBackoff:    func(int) time.Duration { return 10 * time.Millisecond },
		ObservationInterval: 2,
		SanityCheck:         "select true",
		LogicalID:           11,
		PhysicalID:          46,
		ReController:        reC,
		ReAction:            reA,
		ReJobTag:            reJ,
		State:               store,
		ServerOutbox:        store,
		Logger:              slog.Default(),
	}
	clk := &stepClock{sleeping: make(chan time.Duration), proceed: make(chan struct{})}
	w := New(cfg, clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- w.Run(ctx) }()
	clk.waitSleep(t)

	workload := observed.Connect(t)
	runDiffWorkload(t, workload, 4)
	observed.Restart(t, func() {
		clk.proceed <- struct{}{}
		deadline := time.Now().Add(10 * time.Second)
		for failedConnects.Load() < 2 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	})
	currentDSN.Store(observed.DSNAs(t, "rotten_observer"))
	clk.waitSleep(t)
	if got := failedConnects.Load(); got < 2 {
		t.Fatalf("failed connect attempts = %d, want at least 2", got)
	}
	cancel()
	if err := <-ran; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	batches := drainOutboxBatches(t, store)
	found := false
	for _, batch := range batches {
		for _, aggregate := range batch.GetAggregates() {
			if aggregate.GetFingerprint() == fingerprintOf(t, diffQuery) && aggregate.GetMetrics().GetCalls() == 4 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("reconnected worker did not enqueue the post-restart workload: %+v", batches)
	}
}

func TestRunStopAfterCurrentFinishesHarvestAndReturnsNil(t *testing.T) {
	observed := startObservedForWorker(t)
	st := &blockingState{
		saveEntered: make(chan struct{}),
		unblockSave: make(chan struct{}),
	}
	cfg := Config{
		ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
		ObservationInterval: 30,
		SanityCheck:         "select true",
		State:               st,
		Logger:              slog.Default(),
	}
	w := New(cfg, RealClock{})
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()
	select {
	case <-st.saveEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("harvest did not reach Save")
	}
	w.StopAfterCurrent()
	select {
	case err := <-done:
		t.Fatalf("Run returned before current harvest finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(st.unblockSave)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after current harvest")
	}
}

type blockingState struct {
	saveEntered chan struct{}
	unblockSave chan struct{}
	once        sync.Once
}

func (s *blockingState) Load(context.Context) (state.Loaded, error) {
	return emptyBaseline(), nil
}

func (s *blockingState) Save(ctx context.Context, snap pgss.Snapshot, takenAt time.Time) error {
	s.once.Do(func() { close(s.saveEntered) })
	select {
	case <-s.unblockSave:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRunReturnsErrorWhenSanityCheckIsFalse(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	cfg := Config{
		ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
		ObservationInterval: 2,
		SanityCheck:         "select false",
		State:               store,
		Logger:              slog.Default(),
	}
	w := New(cfg, RealClock{})
	err := w.Run(context.Background())
	if !errors.Is(err, ErrSanityCheckFailed) {
		t.Fatalf("Run returned %v, want ErrSanityCheckFailed", err)
	}
}

func TestRunReturnsErrorWhenSanityCheckIsNullOrNoRows(t *testing.T) {
	observed := startObservedForWorker(t)
	for _, query := range []string{"select null::boolean", "select true where false"} {
		t.Run(query, func(t *testing.T) {
			store := openStore(t, t.TempDir())
			defer store.Close()
			cfg := Config{
				ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
				ObservationInterval: 2,
				SanityCheck:         query,
				State:               store,
				Logger:              slog.Default(),
			}
			w := New(cfg, RealClock{})
			err := w.Run(context.Background())
			if err == nil {
				t.Fatal("Run returned nil, want sanity check error")
			}
			if retryObservedError(err) {
				t.Fatalf("sanity check error should be fatal, got retryable %v", err)
			}
		})
	}
}

func TestRunReturnsPromptlyWhenContextCancelsMidQuery(t *testing.T) {
	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	cfg := Config{
		ObservedDB:          observerConn(t, observed.DSNAs(t, "rotten_observer")),
		ObservationInterval: 2,
		SanityCheck:         "select true from pg_sleep(30)",
		State:               store,
		Logger:              slog.Default(),
	}
	w := New(cfg, RealClock{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return promptly after context cancellation")
	}
}

func drainOutboxBatches(t *testing.T, store *state.Store) []*rottenv1.SubmitHarvestRequest {
	t.Helper()
	client := &fakeSubmitter{}
	sender := NewOutboxSender(store, client, nil)
	if _, err := sender.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := make([]*rottenv1.SubmitHarvestRequest, 0, len(client.payloads))
	for _, payload := range client.payloads {
		var msg rottenv1.SubmitHarvestRequest
		if err := proto.Unmarshal(payload, &msg); err != nil {
			t.Fatal(err)
		}
		out = append(out, &msg)
	}
	return out
}

func findAggregate(t *testing.T, batches []*rottenv1.SubmitHarvestRequest, fp string) *rottenv1.FingerprintAggregate {
	t.Helper()
	for _, batch := range batches {
		for _, aggregate := range batch.GetAggregates() {
			if aggregate.GetFingerprint() == fp {
				return aggregate
			}
		}
	}
	t.Fatalf("fingerprint %s not found in batches", fp)
	return nil
}

func expectBatchCalls(t *testing.T, batches []*rottenv1.SubmitHarvestRequest, start, end int64, calls uint64) {
	t.Helper()
	for _, batch := range batches {
		if batch.GetWindowStart().AsTime().Unix() == start && batch.GetWindowEnd().AsTime().Unix() == end {
			if calls == 0 {
				for _, aggregate := range batch.GetAggregates() {
					if aggregate.GetFingerprint() == fingerprintOf(t, diffQuery) {
						t.Fatalf("window [%d,%d] has diff aggregate %+v, want none", start, end, aggregate)
					}
				}
				return
			}
			aggregate := findAggregate(t, []*rottenv1.SubmitHarvestRequest{batch}, fingerprintOf(t, diffQuery))
			if got := aggregate.GetMetrics().GetCalls(); got != calls {
				t.Fatalf("window [%d,%d] calls = %d, want %d", start, end, got, calls)
			}
			return
		}
	}
	t.Fatalf("window [%d,%d] not found in batches", start, end)
}

func expectNoBatchEnding(t *testing.T, batches []*rottenv1.SubmitHarvestRequest, end int64) {
	t.Helper()
	for _, batch := range batches {
		if batch.GetWindowEnd().AsTime().Unix() == end {
			t.Fatalf("found batch ending %d, want none: %+v", end, batch)
		}
	}
}

func fingerprintOf(t *testing.T, query string) string {
	t.Helper()
	fp, err := fingerprinting.Normalized(query, fingerprinting.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return fp
}
