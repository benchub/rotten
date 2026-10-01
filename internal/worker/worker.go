// Package worker reads pg_stat_statements from an observed database and
// records events, contexts, and fingerprint stats in the rotten DB.
package worker

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	runningstat "github.com/benchub/runningstat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/identity"
	"github.com/benchub/rotten/internal/pgss"
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

	// the fastest time this query ran in this window
	min_time float64

	// the longest time this query ran in this window
	max_time float64

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
	ObservedDBReset     *pgx.Conn
	ObservationInterval uint32
	SanityCheck         string
	LogicalID           uint32
	PhysicalID          uint32
	ReController        *regexp.Regexp
	ReAction            *regexp.Regexp
	ReJobTag            *regexp.Regexp
	Fingerprint         fingerprinting.Options
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
	// another goroutine, so they're atomics. lastWindowEnd holds
	// PoorMansTime.sec.
	eventCount    atomic.Uint64
	lastWindowEnd atomic.Int64
	parseFailures atomic.Uint32
	eventsPending atomic.Uint32

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

// Run is the worker's main loop. It returns only when ctx ends, and only at
// one of its sleeps. Database calls still use context.Background(), and
// errors still exit through log.Fatalln, as they always have. main starts
// ReportProgress before calling Run.
func (w *Worker) Run(ctx context.Context) error {
	cfg := w.cfg
	clk := w.clk
	rottenDB := cfg.RottenDB
	observedDB := cfg.ObservedDB
	observedDBReset := cfg.ObservedDBReset
	observation_interval := cfg.ObservationInterval
	sanity_check := cfg.SanityCheck
	logical_id := cfg.LogicalID
	physical_id := cfg.PhysicalID
	re_controller := cfg.ReController
	re_action := cfg.ReAction
	re_job_tag := cfg.ReJobTag

	reader := pgss.NewReader(observedDB)

	// first things first, reset pg_stat_statement data so that we might have a clean observation window
	var windowStart PoorMansTime

	log.Println("Doing stats window inital reset")
	resetWindow(observedDBReset)
	windowStart.sec = clk.Now().Unix()

	if err := clk.Sleep(ctx, time.Duration(observation_interval)*time.Second); err != nil {
		return err
	}

	for {
		var windowEnd PoorMansTime
		var nextWindowStart PoorMansTime
		var eventHash map[string]QueryEvent
		var doIt bool

		eventHash = make(map[string]QueryEvent)

		windowEnd.sec = clk.Now().Unix()
		w.parseFailures.Store(0)
		w.eventsPending.Store(0)
		doIt = true

		log.Println("Performing sanity check")
		err := observedDB.QueryRow(context.Background(), sanity_check).Scan(&doIt)
		if err != nil {
			log.Fatalln("couldn't run sanity check test", err)
		}

		if doIt == false {
			log.Fatalln("sanity check fails; exiting")
			// will now exit because Fatal
		}

		log.Println("retrieving stats results")

		stats, err := reader.Read(context.Background())
		if err != nil {
			log.Fatalln("couldn't select from pg_stat_statements", err)
			// will now exit because Fatal
		}
		// Now, while we process the results of what we saw, start a new window in the observed db
		log.Println("stats window reset")
		resetWindow(observedDBReset)
		nextWindowStart.sec = clk.Now().Unix()

		log.Println("walking stats results")
		// Keep only the top 100 entries for each metric, as the old SQL did.
		for _, st := range topN(stats, 100) {
			newEvent := eventFromStat(st)
			newEvent.observationTimeStart = windowStart
			newEvent.observationTimeEnd = windowEnd

			w.eventCount.Add(1)
			w.lastWindowEnd.Store(newEvent.observationTimeEnd.sec)

			fingerprint, err := fingerprinting.Normalized(newEvent.query, w.cfg.Fingerprint)
			if err != nil {
				//log.Println("failed to get fingerprint for event, so ignoring it")
				w.parseFailures.Add(1)
				continue
			}

			// If we have a context for this query, build out a hash for it
			controller_id := w.identities.Controllers.Find(rottenDB, newEvent.query, re_controller)
			action_id := w.identities.Actions.Find(rottenDB, newEvent.query, re_action)
			job_tag_id := w.identities.JobTags.Find(rottenDB, newEvent.query, re_job_tag)

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
			existingEvent, present := eventHash[fingerprint]
			if present {
				existingEvent = mergeEvent(existingEvent, newEvent)
				eventHash[fingerprint] = existingEvent
			} else {
				eventHash[fingerprint] = newEvent
				w.eventsPending.Add(1)
			}
		}

		log.Printf("processing %d unique events", w.eventsPending.Load())

		// now that we've hashed all the events by fingerprint, process each one in a goroutine
		for fingerprint, event := range eventHash {
			var eventToBeGCedLater = event
			go w.processEvent(rottenDB, logical_id, physical_id, observation_interval, fingerprint, &eventToBeGCedLater)
			w.eventsPending.Add(^uint32(0)) // decrement
		}

		if int64(observation_interval) > (clk.Now().Unix() - windowEnd.sec) {
			slackoff := int64(observation_interval) - (clk.Now().Unix() - windowEnd.sec)
			log.Println("doing nothing for", slackoff, "more seconds")

			// sleep for the remaining time of the observation window
			if err := clk.Sleep(ctx, time.Duration(slackoff)*time.Second); err != nil {
				return err
			}
		} else {
			log.Println("ruh oh, our observation window was", (clk.Now().Unix()-windowEnd.sec)-int64(observation_interval), "seconds too short to deal with what we saw")
		}

		log.Println("main loop complete")
		windowStart = nextWindowStart
	}
}

// mergeEvent folds b into a, for two events with the same fingerprint in the
// same observation window, and returns the result. Counters and times are
// summed, min and max are kept, mean and stddev are combined with
// runningstat, and b's context histogram counts are added into a's. The
// returned event shares a's context map, which is updated in place. Everything
// else (query, window) comes from a.
func mergeEvent(a, b QueryEvent) QueryEvent {
	a.calls += b.calls
	a.total_time += b.total_time
	if a.min_time > b.min_time {
		a.min_time = b.min_time
	}
	if a.max_time < b.max_time {
		a.max_time = b.max_time
	}

	rs1 := runningstat.RunningStat{}
	rs2 := runningstat.RunningStat{}

	// Note: a.calls already includes b.calls here. That's how it's always
	// worked, so this characterization keeps it.
	rs1.Init(int64(a.calls), a.mean_time, a.stddev_time)
	rs2.Init(int64(b.calls), b.mean_time, b.stddev_time)
	rs1.Merge(rs2)

	a.mean_time = rs1.RunningStatMean()
	a.stddev_time = rs1.RunningStatDeviation()

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

		log.Println("Current window closed", closed, "seconds ago,", int64(observation_interval)-closed, "seconds till new window,", w.eventsPending.Load(), "unique events queued,", w.parseFailures.Load(), "fingerprints failed,", w.stillProcessing(), "still being recorded. Overall,", processed, "processed,", w.fingerprintCount(), "fingerprints seen")
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
