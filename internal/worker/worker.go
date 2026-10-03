// Package worker reads pg_stat_statements from an observed database and
// records events, contexts, and fingerprint stats in the rotten DB.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runningstat "github.com/benchub/runningstat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/identity"
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
	// (pgss.WindowStats said so). stddev_time is 0 then, and processEvent
	// leaves it out of fingerprint_stats.
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
	RottenDB            *pgxpool.Pool
	ObservedDB          *pgx.Conn
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
}

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

	// The controller, action, and job tag ID caches.
	identities *identity.Caches

	// Progress stats. Run writes them and ReportProgress reads them from
	// another goroutine, so they're atomics. lastWindowEnd holds the last
	// harvest's time in Unix seconds.
	eventCount    atomic.Uint64
	lastWindowEnd atomic.Int64
	eventsPending atomic.Uint32

	// Count and samples are one snapshot for the progress goroutine.
	parseFailuresMu     sync.Mutex
	parseFailures       uint32
	parseFailureSamples []string

	// lastHarvest is the last harvest's time in Unix seconds.
	lastHarvest atomic.Int64

	// staleState is set when the last Save failed, so the stored snapshot
	// is behind what was sent. Only Run's goroutine touches it.
	staleState bool

	// The fingerprints we've seen since startup and are currently processing.
	fingerprintsMu sync.RWMutex
	fingerprints   map[uint64]*Fingerprint

	// How many processEvent calls are running.
	processingMu sync.RWMutex
	processing   uint32

	// statsWait blocks between reportSamples passes. Returning false makes
	// reportSamples return. Production always sleeps and returns true, so
	// the loop runs forever. Tests set their own before starting anything
	// that runs reportSamples, to drive and stop it. Each Worker has its
	// own, so goroutines from one test never see another test's.
	statsWait func(time.Duration) bool
}

// New returns a Worker for cfg that tells time with clk.
func New(cfg Config, clk Clock) *Worker {
	return &Worker{
		cfg:          cfg,
		clk:          clk,
		identities:   identity.NewCaches(),
		fingerprints: make(map[uint64]*Fingerprint),
		statsWait: func(d time.Duration) bool {
			time.Sleep(d)
			return true
		},
	}
}

// Run is the worker's main loop. Each pass runs the sanity check, then one
// harvest (see harvest), then sleeps out the rest of the observation
// interval. The first harvest happens right away; with no saved snapshot
// it's a baseline and sends nothing. Run never resets pg_stat_statements'
// counters.
//
// It returns only when ctx ends, and only at its sleep. Observed database
// errors still exit through log.Fatalln, as they always have. main starts
// ReportProgress before calling Run.
func (w *Worker) Run(ctx context.Context) error {
	cfg := w.cfg
	reader := pgss.NewReader(cfg.ObservedDB)
	texts := pgss.NewTextCache(reader)
	interval := time.Duration(cfg.ObservationInterval) * time.Second

	for {
		doIt := true
		log.Println("Performing sanity check")
		if err := cfg.ObservedDB.QueryRow(context.Background(), cfg.SanityCheck).Scan(&doIt); err != nil {
			log.Fatalln("couldn't run sanity check test", err)
		}
		if !doIt {
			log.Fatalln("sanity check fails; exiting")
			// will now exit because Fatal
		}

		now := w.clk.Now()
		w.harvest(reader, texts, now)

		if elapsed := w.clk.Now().Sub(now); elapsed < interval {
			slackoff := interval - elapsed
			log.Println("doing nothing for", slackoff.Round(time.Millisecond), "more")
			if err := w.clk.Sleep(ctx, slackoff); err != nil {
				return err
			}
		} else {
			log.Println("ruh oh, our harvest took", (elapsed - interval).Round(time.Millisecond), "longer than the observation window")
		}
		log.Println("main loop complete")
	}
}

// emptyBaseline is a baseline Loaded with an empty snapshot.
func emptyBaseline() state.Loaded {
	return state.Loaded{Baseline: true, Snapshot: pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{}}}
}

// harvest reads pg_stat_statements, diffs it against the saved snapshot, and
// sends the top entries' activity for the window from the snapshot's taken_at
// to now. Then it saves the new snapshot, taken at now. In the legacy direct
// DB path, processEvent still runs asynchronously before the snapshot save: a
// crash after the save can lose a window, and a crash between direct writes
// and the save can count it twice. The ServerOutbox path closes that gap by
// saving the serialized SubmitHarvest batch and next snapshot together, then
// deleting the batch only after the server acks it.
//
// A baseline harvest (no usable snapshot, a state store error on Load, or a
// failed Save last time) saves the snapshot and sends nothing, because Diff
// against it reports lifetime totals or counts a window twice.
func (w *Worker) harvest(reader *pgss.Reader, texts *pgss.TextCache, now time.Time) {
	ctx := context.Background()
	cfg := w.cfg
	w.resetParseFailures()
	w.eventsPending.Store(0)

	log.Println("retrieving stats results")
	info, err := reader.Info(ctx)
	if err != nil {
		log.Fatalln("couldn't read pg_stat_statements_info", err)
	}
	stats, err := reader.ReadStats(ctx)
	if err != nil {
		log.Fatalln("couldn't select from pg_stat_statements", err)
		// will now exit because Fatal
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
			return
		}
		if result.DroppedCap > 0 {
			log.Println("worker outbox cap dropped oldest batches", result.DroppedCap)
		}
		w.staleState = false
		return
	} else {
		w.send(ctx, texts, deltas, loaded.TakenAt, now)
	}

	if err := cfg.State.Save(ctx, next, now); err != nil {
		log.Println("couldn't save the snapshot, so the next harvest is a baseline:", err)
		w.staleState = true
		return
	}
	w.staleState = false
}

// send picks the top deltas, fetches their text, fingerprints them, merges
// them by fingerprint, and hands each merged event to processEvent.
func (w *Worker) send(ctx context.Context, texts *pgss.TextCache, deltas []pgss.Delta, start, end time.Time) {
	cfg := w.cfg
	picked := topNDeltas(deltas, topDeltasPerMetric)
	rows := make([]pgss.Stat, len(picked))
	for i := range picked {
		rows[i] = picked[i].Stat
	}
	if err := texts.Fill(ctx, rows); err != nil {
		log.Println("couldn't fetch query text, so entries without cached text are skipped this window:", err)
	}
	windowStart := PoorMansTime{sec: start.Unix()}
	windowEnd := PoorMansTime{sec: end.Unix()}

	eventHash := make(map[string]QueryEvent)
	hidden, noText := 0, 0
	log.Println("walking stats results")
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
		newEvent := eventFromDelta(d)
		newEvent.observationTimeStart = windowStart
		newEvent.observationTimeEnd = windowEnd
		w.eventCount.Add(1)

		fingerprint, err := fingerprinting.Normalized(newEvent.query, cfg.Fingerprint)
		if err != nil {
			w.recordParseFailure(newEvent.query)
			continue
		}

		// If we have a context for this query, build out a hash for it
		controller_id := w.identities.Controllers.Find(cfg.RottenDB, newEvent.query, cfg.ReController)
		action_id := w.identities.Actions.Find(cfg.RottenDB, newEvent.query, cfg.ReAction)
		job_tag_id := w.identities.JobTags.Find(cfg.RottenDB, newEvent.query, cfg.ReJobTag)

		context_hash := ""
		if controller_id > 0 {
			context_hash = fmt.Sprintf("%scontroller:%d", context_hash, controller_id)
		}
		if action_id > 0 {
			context_hash = fmt.Sprintf("%saction:%d", context_hash, action_id)
		}
		if job_tag_id > 0 {
			context_hash = fmt.Sprintf("%sjob_tag:%d", context_hash, job_tag_id)
		}

		// If we've already seen this fingerprint in this observation window,
		// then merge this event with what we've seen so far.
		// If it's new, make a new entry in our event hash.
		newEvent.context = map[string]uint32{context_hash: uint32(newEvent.calls)}
		if existing, present := eventHash[fingerprint]; present {
			eventHash[fingerprint] = mergeEvent(existing, newEvent)
		} else {
			eventHash[fingerprint] = newEvent
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

	log.Printf("processing %d unique events", w.eventsPending.Load())

	// now that we've hashed all the events by fingerprint, process each one in a goroutine
	for fingerprint, event := range eventHash {
		var eventToBeGCedLater = event
		go w.processEvent(cfg.RottenDB, cfg.LogicalID, cfg.PhysicalID, cfg.ObservationInterval, fingerprint, &eventToBeGCedLater)
		w.eventsPending.Add(^uint32(0)) // decrement
	}
}

func (w *Worker) buildHarvestBatch(ctx context.Context, texts *pgss.TextCache, deltas []pgss.Delta, start, end time.Time) *rottenv1.SubmitHarvestRequest {
	cfg := w.cfg
	picked := topNDeltas(deltas, topDeltasPerMetric)
	rows := make([]pgss.Stat, len(picked))
	for i := range picked {
		rows[i] = picked[i].Stat
	}
	if err := texts.Fill(ctx, rows); err != nil {
		log.Println("couldn't fetch query text, so entries without cached text are skipped this window:", err)
	}
	events := make(map[string]QueryEvent)
	for i, d := range picked {
		if d.Calls <= 0 || d.QueryID == 0 {
			continue
		}
		d.Query = rows[i].Query
		if d.Query == "" {
			continue
		}
		event := eventFromDelta(d)
		event.query = d.Query
		event.observationTimeStart = PoorMansTime{sec: start.Unix()}
		event.observationTimeEnd = PoorMansTime{sec: end.Unix()}
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
		}
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

// mergeEvent folds b into a, for two events with the same fingerprint in the
// same observation window, and returns the result. Counters and times are
// summed, min and max are kept, mean and stddev are combined with
// runningstat, and b's context histogram counts are added into a's. The
// merged stddev is absent (stddev_absent) if either side's is, and min and
// max are lifetime if either side's are. The returned event shares a's
// context map, which is updated in place. Everything else (query, window)
// comes from a.
func mergeEvent(a, b QueryEvent) QueryEvent {
	a.calls += b.calls
	a.total_time += b.total_time
	if a.min_time > b.min_time {
		a.min_time = b.min_time
	}
	if a.max_time < b.max_time {
		a.max_time = b.max_time
	}
	a.minmax_lifetime = a.minmax_lifetime || b.minmax_lifetime

	rs1 := runningstat.RunningStat{}
	rs2 := runningstat.RunningStat{}

	// Note: a.calls already includes b.calls here. That's how it's always
	// worked, so this characterization keeps it.
	rs1.Init(int64(a.calls), a.mean_time, a.stddev_time)
	rs2.Init(int64(b.calls), b.mean_time, b.stddev_time)
	rs1.Merge(rs2)

	a.mean_time = rs1.RunningStatMean()
	a.stddev_time = rs1.RunningStatDeviation()
	// The merged stddev is built from both sides' stddevs, so it's only as
	// good as the worse one. The mean doesn't use them.
	a.stddev_absent = a.stddev_absent || b.stddev_absent
	if a.stddev_absent {
		a.stddev_time = 0
	}

	a.rows += b.rows
	a.shared_blks_hit += b.shared_blks_hit
	a.shared_blks_read += b.shared_blks_read
	a.shared_blks_written += b.shared_blks_written
	a.shared_blks_dirtied += b.shared_blks_dirtied
	a.local_blks_written += b.local_blks_written
	a.local_blks_dirtied += b.local_blks_dirtied
	a.local_blks_read += b.local_blks_read
	a.local_blks_hit += b.local_blks_hit
	a.temp_blks_read += b.temp_blks_read
	a.temp_blks_written += b.temp_blks_written
	a.blk_read_time += b.blk_read_time
	a.blk_write_time += b.blk_write_time

	for hash, count := range b.context {
		a.context[hash] += count
	}

	return a
}

// ReportProgress logs the progress counters every interval seconds. It
// returns when ctx ends. main passes context.Background(), so in production
// it never does. With noIdleHands set, two intervals in a row with no new
// events make it panic, so the process dies ungracefully.
func (w *Worker) ReportProgress(ctx context.Context, noIdleHands bool, interval uint32) {
	observation_interval := w.cfg.ObservationInterval
	almostDead := false
	lastProcessed := w.eventCount.Load()
	w.lastWindowEnd.Store(time.Now().Unix())

	for {
		closed := time.Now().Unix() - w.lastWindowEnd.Load()
		processed := w.eventCount.Load()
		failures, samples := w.parseFailureSnapshot()

		log.Println("Current window closed", closed, "seconds ago,", int64(observation_interval)-closed, "seconds till new window,", w.eventsPending.Load(), "unique events queued,", failures, "fingerprints failed,", w.stillProcessing(), "still being recorded. Overall,", processed, "processed,", w.fingerprintCount(), "fingerprints seen")
		if failures > 0 {
			log.Printf("fingerprint failure samples (up to %d): %q", parseFailureSampleLimit, samples)
		}
		if noIdleHands && lastProcessed == processed {
			if almostDead {
				var m map[string]int

				m["stacktracetime"] = 1
			} else {
				almostDead = true
			}
		} else {
			almostDead = false
		}

		lastProcessed = processed
		if err := (RealClock{}).Sleep(ctx, time.Duration(interval)*time.Second); err != nil {
			return
		}
	}
}
