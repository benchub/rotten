package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

import (
	"database/sql"
	runningstat "github.com/benchub/runningstat"
	_ "github.com/lib/pq"
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

// When we find events to process, send them here
var eventsToProcess = make(chan *QueryEvent, 1000)

// Some stats that we won't bother to make concurrency-safe.
// They're never decremented anyway.
var eventCount uint64
var lastWindowEnd PoorMansTime
var parseFailures uint32
var eventsPending uint32

var configFileFlag = flag.String("config", "", "the config file")
var noIdleHandsFlag = flag.Bool("noIdleHands", false, "when set to true, kill us (ungracefully) if we seem to be doing nothing")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

type Configuration struct {
	ObservedDBConn      []string
	RottenDBConn        []string
	StatusInterval      uint32
	ObservationInterval uint32
	SanityCheck         string
	FQDN                string
	Project             string
	Environment         string
	Cluster             string
	Role                string
	ContextController   string
	ContextAction       string
	ContextJob          string
}

func main() {
	var rottenDB *sql.DB
	var observedDB *sql.DB
	var status_interval uint32
	var observation_interval uint32
	var sanity_check string
	var fqdn string
	var project string
	var environment string
	var cluster string
	var role string
	var logical_id uint32
	var physical_id uint32
	var re_controller *regexp.Regexp
	var re_action *regexp.Regexp
	var re_job_tag *regexp.Regexp

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}

	sigs := make(chan os.Signal, 1)
	// catch all signals since not explicitly listing
	signal.Notify(sigs, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// method invoked upon seeing signal
	go func() {
		s := <-sigs
		log.Printf("RECEIVED SIGNAL: %s", s)
		AppCleanup()
		os.Exit(1)
	}()

	if *configFileFlag == "" {
		log.Fatalln("I need a config file!")
		// will now exit because Fatal
	} else {
		configFile, err := os.Open(*configFileFlag)
		if err != nil {
			log.Fatalln("opening config file:", err)
			// will now exit because Fatal
		}

		decoder := json.NewDecoder(configFile)
		configuration := &Configuration{}
		decoder.Decode(&configuration)

		rottenDB, err = sql.Open("postgres", configuration.RottenDBConn[0])
		if err != nil {
			log.Fatalln("couldn't connect to rotten db", err)
			// will now exit because Fatal
		}

		rottenDB.SetMaxOpenConns(5)
		rottenDB.SetConnMaxLifetime(time.Second * 10)

		observedDB, err = sql.Open("postgres", configuration.ObservedDBConn[0])
		if err != nil {
			log.Fatalln("couldn't connect to observed db", err)
			// will now exit because Fatal
		}

		status_interval = configuration.StatusInterval
		observation_interval = configuration.ObservationInterval
		sanity_check = configuration.SanityCheck
		fqdn = configuration.FQDN
		project = configuration.Project
		environment = configuration.Environment
		cluster = configuration.Cluster
		role = configuration.Role
		re_controller, _ = regexp.Compile(configuration.ContextController)
		re_action, _ = regexp.Compile(configuration.ContextAction)
		re_job_tag, _ = regexp.Compile(configuration.ContextJob)

		// find out the logical source ID we will be using
		if err := rottenDB.QueryRow(`select id from logical_sources where project=$1 and environment=$2 and cluster=$3 and role=$4`, project, environment, cluster, role).Scan(&logical_id); err == nil {
			// yay, we have our ID
		} else if err == sql.ErrNoRows {
			if err := rottenDB.QueryRow(`insert into logical_sources(project,environment,cluster,role) values ($1,$2,$3,$4) returning id`, project, environment, cluster, role).Scan(&logical_id); err == nil {
				// yay, we have our ID
			} else {
				log.Fatalln("couldn't insert into logical_sources", err)
				// will now exit because Fatal
			}
		} else {
			log.Fatalln("couldn't select from logical_sources", err)
			// will now exit because Fatal
		}

		// find out the physical source ID we will be using
		if err := rottenDB.QueryRow(`select id from physical_sources where fqdn=$1`, fqdn).Scan(&physical_id); err == nil {
			// yay, we have our ID
		} else if err == sql.ErrNoRows {
			if err := rottenDB.QueryRow(`insert into physical_sources(fqdn) values ($1) returning id`, fqdn).Scan(&physical_id); err == nil {
				// yay, we have our ID
			} else {
				log.Fatalln("couldn't insert into physical_sources", err)
				// will now exit because Fatal
			}
		} else {
			log.Fatalln("couldn't select from physical_sources", err)
			// will now exit because Fatal
		}
	}

	// We like stats
	go reportProgress(*noIdleHandsFlag, status_interval, observation_interval)

	// first things first, reset pg_stat_statement data so that we might have a clean observation window
	var windowStart PoorMansTime

	log.Println("Doing stats window inital reset")
	if _, err := observedDB.Exec(`select dba.pg_stat_statements_user_reset()`); err != nil {
		log.Fatalln("couldn't reset pg_stat_statements", err)
		// will now exit because Fatal
	}
	windowStart.sec = time.Now().Unix()

	time.Sleep(time.Duration(observation_interval) * time.Second)

	for {
		var windowEnd PoorMansTime
		var nextWindowStart PoorMansTime
		var eventHash map[string]QueryEvent
		var doIt bool

		eventHash = make(map[string]QueryEvent)

		windowEnd.sec = time.Now().Unix()
		parseFailures = 0
		eventsPending = 0
		doIt = true

		log.Println("Performing sanity check")
		err := observedDB.QueryRow(sanity_check).Scan(&doIt)
		if err != nil {
			log.Fatalln("couldn't run sanity check test", err)
		}

		if doIt == false {
			log.Fatalln("sanity check fails; exiting")
			// will now exit because Fatal
		}

		log.Println("retrieving stats results")

		// instead of getting all of pg_stat_statements, we get the top 100 queries for each metric
		// (getting everything can take several minutes; this only takes a few seconds)
		queries, err := observedDB.Query(`select query,calls,total_time,min_time,max_time,mean_time,stddev_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written,temp_blks_written,temp_blks_read,blk_write_time,blk_read_time from (
                                        with raw as (select * from dba.pg_stat_statements())
                                        select * from (select * from raw order by calls desc limit 100) calls union distinct 
                                        select * from (select * from raw order by total_time desc limit 100) total_time union distinct 
                                        select * from (select * from raw order by min_time desc limit 100) min_time union distinct 
                                        select * from (select * from raw order by max_time desc limit 100) max_time union distinct 
                                        select * from (select * from raw order by mean_time desc limit 100) mean_time union distinct 
                                        select * from (select * from raw order by stddev_time desc limit 100) stddev_time union distinct 
                                        select * from (select * from raw order by rows desc limit 100) rows union distinct 
                                        select * from (select * from raw order by shared_blks_hit desc limit 100) shared_blks_hit union distinct 
                                        select * from (select * from raw order by shared_blks_read desc limit 100) shared_blks_read union distinct 
                                        select * from (select * from raw order by shared_blks_written desc limit 100) shared_blks_written union distinct 
                                        select * from (select * from raw order by shared_blks_dirtied desc limit 100) shared_blks_dirtied union distinct 
                                        select * from (select * from raw order by local_blks_hit desc limit 100) local_blks_hit union distinct 
                                        select * from (select * from raw order by local_blks_read desc limit 100) local_blks_read union distinct 
                                        select * from (select * from raw order by local_blks_written desc limit 100) local_blks_written union distinct 
                                        select * from (select * from raw order by local_blks_dirtied desc limit 100) local_blks_dirtied union distinct 
                                        select * from (select * from raw order by temp_blks_read desc limit 100) temp_blks_read union distinct 
                                        select * from (select * from raw order by temp_blks_written desc limit 100) temp_blks_written union distinct
                                        select * from (select * from raw order by blk_write_time desc limit 100) blk_write_time union distinct 
                                        select * from (select * from raw order by blk_read_time desc limit 100) blk_read_time) foo`)
		if err != nil {
			log.Fatalln("couldn't select from pg_stat_statements", err)
			// will now exit because Fatal
		}
		// Now, while we process the results of what we saw, start a new window in the observed db
		log.Println("stats window reset")
		if _, err := observedDB.Exec(`select dba.pg_stat_statements_user_reset()`); err != nil {
			log.Fatalln("couldn't reset pg_stat_statements", err)
			// will now exit because Fatal
		}
		nextWindowStart.sec = time.Now().Unix()

		log.Println("walking stats results")
		for queries.Next() {
			newEvent := QueryEvent{}
			if err := queries.Scan(&newEvent.query, &newEvent.calls, &newEvent.total_time, &newEvent.min_time, &newEvent.max_time, &newEvent.mean_time, &newEvent.stddev_time, &newEvent.rows, &newEvent.shared_blks_hit, &newEvent.shared_blks_read, &newEvent.shared_blks_dirtied, &newEvent.shared_blks_written, &newEvent.local_blks_hit, &newEvent.local_blks_read, &newEvent.local_blks_dirtied, &newEvent.local_blks_written, &newEvent.temp_blks_written, &newEvent.temp_blks_read, &newEvent.blk_write_time, &newEvent.blk_read_time); err != nil {
				log.Fatalln("couldn't parse query row", err)
				// will now exit because Fatal
			}
			newEvent.observationTimeStart = windowStart
			newEvent.observationTimeEnd = windowEnd

			eventCount++
			lastWindowEnd = newEvent.observationTimeEnd

			fingerprint, err := normalized_fingerprint(&newEvent)
			if err != nil {
				//log.Println("failed to get fingerprint for event, so ignoring it")
				parseFailures++
				continue
			}

			// If we have a context for this query, build out a hash for it
			controller_id := find_controller_id(rottenDB, &newEvent, re_controller)
			action_id := find_action_id(rottenDB, &newEvent, re_action)
			job_tag_id := find_job_tag_id(rottenDB, &newEvent, re_job_tag)

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
			existingEvent, present := eventHash[fingerprint]
			if present {
				existingEvent.calls += newEvent.calls
				existingEvent.total_time += newEvent.total_time
				if existingEvent.min_time > newEvent.min_time {
					existingEvent.min_time = newEvent.min_time
				}
				if existingEvent.max_time < newEvent.max_time {
					existingEvent.max_time = newEvent.max_time
				}

				rs1 := runningstat.RunningStat{}
				rs2 := runningstat.RunningStat{}

				rs1.Init(int64(existingEvent.calls), existingEvent.mean_time, existingEvent.stddev_time)
				rs2.Init(int64(newEvent.calls), newEvent.mean_time, newEvent.stddev_time)
				rs1.Merge(rs2)

				existingEvent.mean_time = rs1.RunningStatMean()
				existingEvent.stddev_time = rs1.RunningStatDeviation()

				existingEvent.rows += newEvent.rows
				existingEvent.shared_blks_hit += newEvent.shared_blks_hit
				existingEvent.shared_blks_read += newEvent.shared_blks_read
				existingEvent.shared_blks_written += newEvent.shared_blks_written
				existingEvent.shared_blks_dirtied += newEvent.shared_blks_dirtied
				existingEvent.local_blks_written += newEvent.local_blks_written
				existingEvent.local_blks_dirtied += newEvent.local_blks_dirtied
				existingEvent.local_blks_read += newEvent.local_blks_read
				existingEvent.local_blks_hit += newEvent.local_blks_hit
				existingEvent.temp_blks_read += newEvent.temp_blks_read
				existingEvent.temp_blks_written += newEvent.temp_blks_written
				existingEvent.blk_read_time += newEvent.blk_read_time
				existingEvent.blk_write_time += newEvent.blk_write_time

				existingContextCount, present := existingEvent.context[context_hash]
				if present {
					existingEvent.context[context_hash] = existingContextCount + uint32(newEvent.calls)
				} else {
					existingEvent.context[context_hash] = uint32(newEvent.calls)
				}

				eventHash[fingerprint] = existingEvent
			} else {
				newEvent.context = make(map[string]uint32)
				newEvent.context[context_hash] = uint32(newEvent.calls)

				eventHash[fingerprint] = newEvent
				eventsPending++
			}
		}
		queries.Close()

		log.Printf("processing %d unique events", eventsPending)

		// now that we've hashed all the events by fingerprint, process each one
		for fingerprint, event := range eventHash {
			var eventToBeGCedLater = event
			go processEvent(rottenDB, logical_id, physical_id, observation_interval, fingerprint, &eventToBeGCedLater)
			eventsPending--
		}

		if int64(observation_interval) > (time.Now().Unix() - windowEnd.sec) {
			slackoff := int64(observation_interval) - (time.Now().Unix() - windowEnd.sec)
			log.Println("doing nothing for", slackoff, "more seconds")

			// sleep for the remaining time of the observation window
			time.Sleep(time.Duration(slackoff) * time.Second)
		} else {
			log.Println("ruh oh, our observation window was", (time.Now().Unix()-windowEnd.sec)-int64(observation_interval), "seconds too short to deal with what we saw")
		}

		log.Println("main loop complete")
		windowStart = nextWindowStart
	}

	// until we implement graceful exiting, we'll never get here
	// AppCleanup()
}

func reportProgress(noIdleHands bool, interval uint32, observation_interval uint32) {
	almostDead := false
	lastProcessed := eventCount
	lastWindowEnd.sec = time.Now().Unix()

	for {
		closed := time.Now().Unix() - lastWindowEnd.sec

		log.Println("Current window closed", closed, "seconds ago,", int64(observation_interval)-closed, "seconds till new window,", eventsPending, "unique events queued,", parseFailures, "fingerprints failed,", stillProcessing(), "still being recorded. Overall,", eventCount, "processed,", fingerprintCount(), "fingerprints seen")
		if noIdleHands && lastProcessed == eventCount {
			if almostDead {
				var m map[string]int

				m["stacktracetime"] = 1
			} else {
				almostDead = true
			}
		} else {
			almostDead = false
		}

		lastProcessed = eventCount
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func AppCleanup() {
	log.Println("...and that's all folks!")
	pprof.StopCPUProfile()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
