// Package worker reads pg_stat_statements from an observed database and
// records events, contexts, and fingerprint stats in the rotten DB.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/state"
)

type PoorMansTime struct {
	// a pointerless version of time.Time, in an attempt to reduce GC activity.
	// We will assume all times are in UTC (which is just God's Time anyway)
	sec int64
}

type QueryEvent struct {
	// the query, as seen by pg_stats_statement
	query string

	// how many calls we saw in this window
	calls float64

	// aggregate runtime stats of the query in this window
	mean_time   float64
	stddev_time float64
	total_time  float64

	// stddev_absent is true when stddev_time can't be trusted for this window
	// (pgss.WindowStats said so). stddev_time is 0 then, and the outbox
	// payload leaves it out of fingerprint_stats.
	stddev_absent bool

	// the fastest time this query ran in this window
	min_time float64

	// the longest time this query ran in this window
	max_time float64

	// minmax_lifetime is true when min_time and max_time reach back before
	// this window (pgss.WindowMinMax). Nothing stores it yet.
	minmax_lifetime bool

	rows                float64
	shared_blks_hit     float64
	shared_blks_read    float64
	shared_blks_dirtied float64
	shared_blks_written float64
	local_blks_hit      float64
	local_blks_read     float64
	local_blks_dirtied  float64
	local_blks_written  float64
	temp_blks_read      float64
	temp_blks_written   float64
	blk_read_time       float64
	blk_write_time      float64

	// A histogram of the marginalia contexts observed for this query in this window
	context map[string]uint32

	// pg_stat_statment's observation window boundaries this event was seen in
	observationTimeStart PoorMansTime
	observationTimeEnd   PoorMansTime
}

// Config is what Run needs: the connections main opened and the settings
// main read from the config file.
type Config struct {
	// ObservedDBConnect opens a fresh connection to the observed database.
	// When set, Run reconnects with backoff after transient connection
	// failures. ObservedDB is used only when this is nil.
	ObservedDBConnect ObservedConnector
	ObservedDB        *pgx.Conn
	// ReconnectBackoff returns the delay before reconnect attempt n. Nil
	// uses a capped exponential backoff.
	ReconnectBackoff    func(attempt int) time.Duration
	MaxReconnectBackoff time.Duration
	ConnectTimeout      time.Duration
	WatchdogMargin      time.Duration
	ObservationInterval uint32
	SanityCheck         string
	LogicalID           uint32
	PhysicalID          uint32
	ReController        *regexp.Regexp
	ReAction            *regexp.Regexp
	ReJobTag            *regexp.Regexp
	Fingerprint         fingerprinting.Options
	// MinmaxResetSchema holds <schema>.pg_stat_statements_minmax_reset()
	// on 17+. Run calls it right after each harvest's read. Empty means
	// pgss.DefaultMinmaxResetSchema.
	MinmaxResetSchema string
	// State holds the snapshot between harvests. main opens a *state.Store.
	State StateStore
	// ServerOutbox, when set, makes harvest enqueue a SubmitHarvest batch
	// and save the next snapshot in one local transaction. This library hook
	// is for -39's config cutover; until main wires it, the worker keeps the
	// direct rotten DB write path.
	ServerOutbox ServerOutboxStore
	// WatchdogExit is called when noIdleHands fires. Nil exits the process
	// with status 1.
	WatchdogExit func(reason string)
	Logger       *slog.Logger
}

// ErrSanityCheckFailed is returned when the configured sanity check runs and
// returns false. Supervisors should treat it as an intentional nonzero exit.
var ErrSanityCheckFailed = errors.New("sanity check fails")

var errGracefulStop = errors.New("worker graceful stop")

// ObservedConnector opens a new observed database connection.
type ObservedConnector func(context.Context) (*pgx.Conn, error)

// StateStore is the part of *state.Store that Run uses.
type StateStore interface {
	Load(ctx context.Context) (state.Loaded, error)
	Save(ctx context.Context, snap pgss.Snapshot, takenAt time.Time) error
}

type ServerOutboxStore interface {
	SaveSnapshotAndEnqueue(context.Context, pgss.Snapshot, time.Time, *rottenv1.SubmitHarvestRequest) (state.OutboxEnqueueResult, error)
}

// Clock is Run's source of time. Sleep returns ctx.Err() if ctx ends first.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Worker holds the state one worker shares across its goroutines: the
// progress counters, the identity caches, and the fingerprints it's tracking.
type Worker struct {
	cfg Config
	clk Clock

	// Progress stats. Run writes them and ReportProgress reads them from
	// another goroutine, so they're atomics. lastWindowEnd holds the last
	// harvest's time in Unix seconds.
	eventCount     atomic.Uint64
	lastWindowEnd  atomic.Int64
	eventsPending  atomic.Uint32
	liveness       atomic.Uint64
	livenessReason atomic.Value

	// Count and samples are one snapshot for the progress goroutine.
	parseFailuresMu     sync.Mutex
	parseFailures       uint32
	parseFailureSamples []string

	// lastHarvest is the last harvest's time in Unix seconds.
	lastHarvest atomic.Int64

	// staleState is set when the last Save failed, so the stored snapshot
	// is behind what was sent. Only Run's goroutine touches it.
	staleState bool

	// How many event-processing goroutines are running. The server-outbox
	// worker path never starts any; this remains for progress log continuity.
	processing atomic.Uint32

	gracefulStop     chan struct{}
	gracefulStopOnce sync.Once
}

// New returns a Worker for cfg that tells time with clk.
func New(cfg Config, clk Clock) *Worker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{
		cfg:          cfg,
		clk:          clk,
		gracefulStop: make(chan struct{}),
	}
}

// StopAfterCurrent asks Run to return after the current harvest, or
// immediately if it is sleeping between harvests. It does not cancel in-flight
// database work; cancel Run's context for an urgent stop.
func (w *Worker) StopAfterCurrent() {
	w.gracefulStopOnce.Do(func() { close(w.gracefulStop) })
}

func (w *Worker) stopping() bool {
	select {
	case <-w.gracefulStop:
		return true
	default:
		return false
	}
}

// Run is the worker's main loop. Each pass runs the sanity check, then one
// harvest (see harvest), then sleeps out the rest of the observation
// interval. The first harvest happens right away; with no saved snapshot
// it's a baseline and sends nothing. Run never resets pg_stat_statements'
// counters.
func (w *Worker) Run(ctx context.Context) error {
	cfg := w.cfg
	interval := time.Duration(cfg.ObservationInterval) * time.Second
	var observed *pgx.Conn
	var reader *pgss.Reader
	var texts *pgss.TextCache
	ownsObserved := cfg.ObservedDBConnect != nil
	defer func() {
		if ownsObserved && observed != nil {
			_ = observed.Close(context.Background())
		}
	}()
	connectAttempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.markAlive("loop")
		if w.stopping() {
			return nil
		}
		if observed == nil {
			conn, err := w.connectObserved(ctx, connectAttempt)
			if err != nil {
				if errors.Is(err, errGracefulStop) {
					return nil
				}
				return err
			}
			w.markAlive("connected")
			observed = conn
			reader = pgss.NewReader(observed)
			texts = pgss.NewTextCache(reader)
			connectAttempt = 0
		}
		doIt := true
		log.Println("Performing sanity check")
		if err := observed.QueryRow(ctx, cfg.SanityCheck).Scan(&doIt); err != nil {
			if retryObservedError(err) {
				w.markAlive("sanity reconnect")
				w.cfg.Logger.Warn("observed database sanity check failed; reconnecting", "err", err)
				w.closeObserved(observed)
				observed = nil
				reader = nil
				texts = nil
				connectAttempt++
				if err := w.sleepOrStop(ctx, w.reconnectBackoff(connectAttempt)); err != nil {
					if errors.Is(err, errGracefulStop) {
						return nil
					}
					return err
				}
				continue
			}
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrSanityCheckFailed
			}
			return fmt.Errorf("couldn't run sanity check test: %w", err)
		}
		if !doIt {
			return ErrSanityCheckFailed
		}

		now := w.clk.Now()
		if err := w.harvest(ctx, reader, texts, now); err != nil {
			if retryObservedError(err) {
				w.markAlive("harvest reconnect")
				w.cfg.Logger.Warn("observed database harvest failed; reconnecting", "err", err)
				w.closeObserved(observed)
				observed = nil
				reader = nil
				texts = nil
				connectAttempt++
				if err := w.sleepOrStop(ctx, w.reconnectBackoff(connectAttempt)); err != nil {
					if errors.Is(err, errGracefulStop) {
						return nil
					}
					return err
				}
				continue
			}
			return err
		}

		if w.stopping() {
			return nil
		}

		if elapsed := w.clk.Now().Sub(now); elapsed < interval {
			slackoff := interval - elapsed
			log.Println("doing nothing for", slackoff.Round(time.Millisecond), "more")
			sleepCtx, cancel := context.WithCancel(ctx)
			go func() {
				select {
				case <-w.gracefulStop:
					cancel()
				case <-sleepCtx.Done():
				}
			}()
			err := w.clk.Sleep(sleepCtx, slackoff)
			cancel()
			if err != nil {
				if w.stopping() {
					return nil
				}
				return err
			}
		} else {
			log.Println("ruh oh, our harvest took", (elapsed - interval).Round(time.Millisecond), "longer than the observation window")
		}
		log.Println("main loop complete")
	}
}

func (w *Worker) connectObserved(ctx context.Context, attempt int) (*pgx.Conn, error) {
	if w.cfg.ObservedDBConnect == nil {
		if w.cfg.ObservedDB == nil {
			return nil, errors.New("observed database connection is required")
		}
		return w.cfg.ObservedDB, nil
	}
	for {
		conn, err := w.cfg.ObservedDBConnect(ctx)
		if err == nil {
			w.markAlive("connect attempt succeeded")
			return conn, nil
		}
		w.markAlive("connect attempt failed")
		attempt++
		delay := w.reconnectBackoff(attempt)
		w.cfg.Logger.Warn("observed database connection failed; retrying", "err", err, "attempt", attempt, "retry_in", delay.String())
		if err := w.sleepOrStop(ctx, delay); err != nil {
			return nil, err
		}
	}
}

func (w *Worker) reconnectBackoff(attempt int) time.Duration {
	if w.cfg.ReconnectBackoff != nil {
		return w.cfg.ReconnectBackoff(attempt)
	}
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 6 {
		shift = 6
	}
	delay := time.Second << shift
	maxBackoff := w.maxReconnectBackoff()
	if delay > maxBackoff {
		delay = maxBackoff
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func (w *Worker) maxReconnectBackoff() time.Duration {
	if w.cfg.MaxReconnectBackoff > 0 {
		return w.cfg.MaxReconnectBackoff
	}
	return time.Minute
}

func (w *Worker) connectTimeout() time.Duration {
	if w.cfg.ConnectTimeout > 0 {
		return w.cfg.ConnectTimeout
	}
	return 5 * time.Second
}

func (w *Worker) watchdogMargin() time.Duration {
	if w.cfg.WatchdogMargin > 0 {
		return w.cfg.WatchdogMargin
	}
	return time.Second
}

func (w *Worker) closeObserved(conn *pgx.Conn) {
	if conn != nil && w.cfg.ObservedDBConnect != nil {
		_ = conn.Close(context.Background())
	}
}

func retryObservedError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) || isNetworkUnavailable(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03" {
			return true
		}
		return false
	}
	return false
}

func isNetworkUnavailable(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, pgconn.ErrConnClosed) || errors.Is(err, net.ErrClosed) {
		return true
	}
	for _, target := range []error{syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ECONNREFUSED, syscall.EPIPE} {
		if errors.Is(err, target) {
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func (w *Worker) sleepOrStop(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.gracefulStop:
		return errGracefulStop
	case <-timer.C:
		return nil
	}
}

// emptyBaseline is a baseline Loaded with an empty snapshot.
func emptyBaseline() state.Loaded {
	return state.Loaded{Baseline: true, Snapshot: pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{}}}
}

// harvest reads pg_stat_statements, diffs it against the saved snapshot, and
// sends the top entries' activity for the window from the snapshot's taken_at
// to now. Non-baseline harvests save the serialized SubmitHarvest batch and
// next snapshot together, then the sender loop deletes the batch only after the
// server acks it.
//
// A baseline harvest (no usable snapshot, a state store error on Load, or a
// failed Save last time) saves the snapshot and sends nothing, because Diff
// against it reports lifetime totals or counts a window twice.
func (w *Worker) harvest(ctx context.Context, reader *pgss.Reader, texts *pgss.TextCache, now time.Time) error {
	cfg := w.cfg
	w.markAlive("harvest attempt")
	w.resetParseFailures()
	w.eventsPending.Store(0)

	log.Println("retrieving stats results")
	info, err := reader.Info(ctx)
	if err != nil {
		return fmt.Errorf("couldn't read pg_stat_statements_info: %w", err)
	}
	stats, err := reader.ReadStats(ctx)
	if err != nil {
		return fmt.Errorf("couldn't select from pg_stat_statements: %w", err)
	}
	// Right after the read, so the next window's min and max cover only
	// that window. This resets min and max only, never the counters.
	if err := reader.MinmaxReset(ctx, cfg.MinmaxResetSchema); err != nil && !errors.Is(err, pgss.ErrNoMinmaxReset) {
		log.Println("min/max reset failed, so the next window reports lifetime min and max:", err)
	}
	w.lastWindowEnd.Store(now.Unix())
	w.lastHarvest.Store(now.Unix())

	loaded, err := cfg.State.Load(ctx)
	if err != nil {
		log.Println("couldn't load the snapshot, so this harvest is a baseline:", err)
		loaded = emptyBaseline()
	}
	if w.staleState && !loaded.Baseline {
		log.Println("the last snapshot save failed, so this harvest is a baseline")
		loaded = emptyBaseline()
	}
	deltas, next := pgss.Diff(loaded.Snapshot, stats, info)
	texts.Retain(stats)

	if loaded.Baseline {
		log.Println("baseline harvest: saving the snapshot and sending nothing")
	} else if cfg.ServerOutbox != nil {
		batch := w.buildHarvestBatch(ctx, texts, deltas, loaded.TakenAt, now)
		result, err := cfg.ServerOutbox.SaveSnapshotAndEnqueue(ctx, next, now, batch)
		if err != nil {
			log.Println("couldn't save the snapshot and outbox batch, so the next harvest is a baseline:", err)
			w.staleState = true
			return nil
		}
		if result.DroppedCap > 0 {
			log.Println("worker outbox cap dropped oldest batches", result.DroppedCap)
		}
		w.staleState = false
		return nil
	} else {
		log.Println("no server outbox configured; dropping harvest and treating next harvest as a baseline")
		w.staleState = true
		return nil
	}

	if err := cfg.State.Save(ctx, next, now); err != nil {
		log.Println("couldn't save the snapshot, so the next harvest is a baseline:", err)
		w.staleState = true
		return nil
	}
	w.staleState = false
	w.markAlive("harvest completed")
	return nil
}

func (w *Worker) markAlive(reason string) {
	w.liveness.Add(1)
	w.livenessReason.Store(reason)
}

func (w *Worker) buildHarvestBatch(ctx context.Context, texts *pgss.TextCache, deltas []pgss.Delta, start, end time.Time) *rottenv1.SubmitHarvestRequest {
	picked := topNDeltas(deltas, topDeltasPerMetric)
	rows := make([]pgss.Stat, len(picked))
	for i := range picked {
		rows[i] = picked[i].Stat
	}
	if err := texts.Fill(ctx, rows); err != nil {
		log.Println("couldn't fetch query text, so entries without cached text are skipped this window:", err)
	}
	return w.buildHarvestBatchFromRows(picked, rows, start, end)
}

func (w *Worker) buildHarvestBatchFromRows(picked []pgss.Delta, rows []pgss.Stat, start, end time.Time) *rottenv1.SubmitHarvestRequest {
	cfg := w.cfg
	events := make(map[string]QueryEvent)
	hidden, noText := 0, 0
	for i, d := range picked {
		if d.Calls <= 0 {
			continue
		}
		if d.QueryID == 0 {
			hidden++
			continue
		}
		d.Query = rows[i].Query
		if d.Query == "" {
			noText++
			continue
		}
		event := eventFromDelta(d)
		event.query = d.Query
		event.observationTimeStart = PoorMansTime{sec: start.Unix()}
		event.observationTimeEnd = PoorMansTime{sec: end.Unix()}
		w.eventCount.Add(1)
		fingerprint, err := fingerprinting.Normalized(event.query, cfg.Fingerprint)
		if err != nil {
			w.recordParseFailure(event.query)
			continue
		}
		event.context = map[string]uint32{
			serverContextKey(
				extractContextValue(event.query, cfg.ReController),
				extractContextValue(event.query, cfg.ReAction),
				extractContextValue(event.query, cfg.ReJobTag),
			): uint32(event.calls),
		}
		if existing, ok := events[fingerprint]; ok {
			events[fingerprint] = mergeEvent(existing, event)
		} else {
			events[fingerprint] = event
			w.eventsPending.Add(1)
		}
	}
	if hidden > 0 {
		log.Println(hidden, "top entries are hidden from the observer (no queryid), so they're skipped")
	}
	if noText > 0 {
		log.Println(noText, "top entries have no query text, so they're skipped")
	}
	failures, samples := w.parseFailureSnapshot()
	if failures > 0 {
		log.Printf("window fingerprint failures: %d; fingerprint failure samples (up to %d): %q", failures, parseFailureSampleLimit, samples)
	}
	aggregates := make([]*rottenv1.FingerprintAggregate, 0, len(events))
	for fingerprint, event := range events {
		normalized, err := fingerprinting.Query(event.query)
		if err != nil {
			w.recordParseFailure(event.query)
			continue
		}
		aggregates = append(aggregates, eventAggregate(fingerprint, normalized, event))
	}
	w.eventsPending.Store(0)
	sort.Slice(aggregates, func(i, j int) bool {
		return aggregates[i].GetFingerprint() < aggregates[j].GetFingerprint()
	})
	return &rottenv1.SubmitHarvestRequest{
		BatchId:          fmt.Sprintf("%d:%d:%d", cfg.PhysicalID, start.UnixMicro(), end.UnixMicro()),
		LogicalSourceId:  cfg.LogicalID,
		PhysicalSourceId: cfg.PhysicalID,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates:       aggregates,
	}
}

func eventAggregate(fingerprint, normalized string, event QueryEvent) *rottenv1.FingerprintAggregate {
	metrics := &rottenv1.Metrics{
		Calls:             uint64(event.calls),
		TotalTime:         event.total_time,
		MinTime:           event.min_time,
		MaxTime:           event.max_time,
		MeanTime:          event.mean_time,
		Rows:              uint64(event.rows),
		SharedBlksHit:     uint64(event.shared_blks_hit),
		SharedBlksRead:    uint64(event.shared_blks_read),
		SharedBlksDirtied: uint64(event.shared_blks_dirtied),
		SharedBlksWritten: uint64(event.shared_blks_written),
		LocalBlksHit:      uint64(event.local_blks_hit),
		LocalBlksRead:     uint64(event.local_blks_read),
		LocalBlksDirtied:  uint64(event.local_blks_dirtied),
		LocalBlksWritten:  uint64(event.local_blks_written),
		TempBlksRead:      uint64(event.temp_blks_read),
		TempBlksWritten:   uint64(event.temp_blks_written),
		BlkReadTime:       event.blk_read_time,
		BlkWriteTime:      event.blk_write_time,
	}
	if !event.stddev_absent {
		metrics.StddevTime = &event.stddev_time
	}
	contexts := make([]*rottenv1.QueryContext, 0, len(event.context))
	for key, count := range event.context {
		parts := strings.SplitN(key, "\x00", 3)
		for len(parts) < 3 {
			parts = append(parts, "")
		}
		contexts = append(contexts, &rottenv1.QueryContext{
			Controller: parts[0],
			Action:     parts[1],
			JobTag:     parts[2],
			Count:      uint64(count),
		})
	}
	sort.Slice(contexts, func(i, j int) bool {
		if contexts[i].GetController() != contexts[j].GetController() {
			return contexts[i].GetController() < contexts[j].GetController()
		}
		if contexts[i].GetAction() != contexts[j].GetAction() {
			return contexts[i].GetAction() < contexts[j].GetAction()
		}
		return contexts[i].GetJobTag() < contexts[j].GetJobTag()
	})
	return &rottenv1.FingerprintAggregate{
		Fingerprint:    fingerprint,
		Normalized:     normalized,
		Contexts:       contexts,
		Metrics:        metrics,
		MinmaxLifetime: event.minmax_lifetime,
	}
}

func extractContextValue(query string, re *regexp.Regexp) string {
	if re == nil {
		return ""
	}
	matches := re.FindStringSubmatch(query)
	if len(matches) <= 1 {
		return ""
	}
	return matches[len(matches)-1]
}

func serverContextKey(controller, action, jobTag string) string {
	return controller + "\x00" + action + "\x00" + jobTag
}

// ReportProgress logs the progress counters every interval seconds. It
// returns when ctx ends. With noIdleHands set, two intervals in a row with no
// new events fire the watchdog.
func (w *Worker) ReportProgress(ctx context.Context, noIdleHands bool, interval uint32) {
	w.reportProgress(ctx, noIdleHands, interval, RealClock{})
}

func (w *Worker) reportProgress(ctx context.Context, noIdleHands bool, interval uint32, clk Clock) {
	observation_interval := w.cfg.ObservationInterval
	lastLive := w.liveness.Load()
	lastLiveAt := time.Now()
	watchdogTimeout := w.watchdogTimeout(time.Duration(interval)*time.Second, time.Duration(observation_interval)*time.Second)
	w.lastWindowEnd.Store(time.Now().Unix())

	for {
		closed := time.Now().Unix() - w.lastWindowEnd.Load()
		processed := w.eventCount.Load()
		live := w.liveness.Load()
		if live != lastLive {
			lastLive = live
			lastLiveAt = time.Now()
		}
		failures, samples := w.parseFailureSnapshot()

		log.Println("Current window closed", closed, "seconds ago,", int64(observation_interval)-closed, "seconds till new window,", w.eventsPending.Load(), "unique events queued,", failures, "fingerprints failed. Overall,", processed, "processed")
		if failures > 0 {
			log.Printf("fingerprint failure samples (up to %d): %q", parseFailureSampleLimit, samples)
		}
		if noIdleHands && time.Since(lastLiveAt) > watchdogTimeout {
			reason := fmt.Sprintf("no worker liveness for %s (last: %v)", time.Since(lastLiveAt).Round(time.Millisecond), w.livenessReason.Load())
			w.cfg.Logger.Error("noIdleHands watchdog firing", "reason", reason, "interval_seconds", interval, "observation_interval_seconds", observation_interval)
			if w.cfg.WatchdogExit != nil {
				w.cfg.WatchdogExit(reason)
			} else {
				os.Exit(1)
			}
			return
		}

		if err := clk.Sleep(ctx, time.Duration(interval)*time.Second); err != nil {
			return
		}
	}
}

func (w *Worker) watchdogTimeout(status, observation time.Duration) time.Duration {
	base := observation
	if status > base {
		base = status
	}
	if base <= 0 {
		base = time.Second
	}
	loopTimeout := 3 * base
	reconnectTimeout := w.maxReconnectBackoff() + w.connectTimeout() + w.watchdogMargin()
	if reconnectTimeout > loopTimeout {
		return reconnectTimeout
	}
	return loopTimeout
}
