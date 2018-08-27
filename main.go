package main

import (
  "log"
  "time"
  "syscall"
  "os"
  "os/signal"
  "encoding/json"
  "flag"
  "runtime/pprof"
  "runtime"
)

import (
  _ "github.com/lib/pq"
  "database/sql"
)


type PoorMansTime struct
{
  // a pointerless version of time.Time, in an attempt to reduce GC activity.
  // We will assume all times are in UTC (which is just God's Time anyway)
  sec int64
}

type QueryEvent struct
{
  // the query, as seen by pg_stats_statement
  query string

  // how many calls we saw in this window
  calls float64

  // the runtime of the query in this window
  total_time float64

  rows float64
  shared_blks_hit float64
  shared_blks_read float64
  shared_blks_dirtied float64
  shared_blks_written float64
  local_blks_hit float64
  local_blks_read float64
  local_blks_dirtied float64
  local_blks_written float64
  temp_blks_read float64
  temp_blks_written float64
  blk_read_time float64
  blk_write_time float64

  // the syslog time of the first line
  observationTimeStart PoorMansTime
  
  // the syslog time of the last line
  observationTimeEnd PoorMansTime
}


// When we find events to process, send them here
var eventsToProcess = make(chan *QueryEvent,1000)


// Some stats that we won't bother to make concurrency-safe.
// They're never decremented anyway.
var eventCount uint64
var lastEventAt PoorMansTime


var configFileFlag = flag.String("config", "", "the config file")
var noIdleHandsFlag = flag.Bool("noIdleHands", false, "when set to true, kill us (ungracefully) if we seem to be doing nothing")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

type Configuration struct {
  ObservedDBConn []string
  RottenDBConn []string
  StatusInterval uint32
  ObservationInterval uint32
  FQDN string
  Project string
  Environment string
  Cluster string
  Role string
}




func main() {
  var rottenDB *sql.DB
  var observedDB *sql.DB
  var status_interval uint32
  var observation_interval uint32
  var fqdn string
  var project string
  var environment string
  var cluster string
  var role string
  var logical_id uint32
  var physical_id uint32

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
  signal.Notify(sigs,syscall.SIGQUIT,syscall.SIGTERM,syscall.SIGINT)
  // method invoked upon seeing signal
  go func() {
    s := <-sigs
    log.Printf("RECEIVED SIGNAL: %s",s)
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

    observedDB, err = sql.Open("postgres", configuration.ObservedDBConn[0])
    if err != nil {
      log.Fatalln("couldn't connect to observed db", err)
      // will now exit because Fatal
    }
    
    status_interval = configuration.StatusInterval
    observation_interval = configuration.ObservationInterval
    fqdn = configuration.FQDN
    project = configuration.Project
    environment = configuration.Environment
    cluster = configuration.Cluster
    role = configuration.Role


    // find out the logical source ID we will be using
    if err := rottenDB.QueryRow(`select id from logical_sources where project=$1 and environment=$2 and cluster=$3 and role=$4`,project,environment,cluster,role).Scan(&logical_id); err == nil {
      // yay, we have our ID
    } else if err == sql.ErrNoRows {
      if err := rottenDB.QueryRow(`insert into logical_sources(project,environment,cluster,role) values ($1,$2,$3,$4) returning id`,project,environment,cluster,role).Scan(&logical_id); err == nil {
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
    if err := rottenDB.QueryRow(`select id from physical_sources where fqdn=$1`,fqdn).Scan(&physical_id); err == nil {
      // yay, we have our ID
    } else if err == sql.ErrNoRows {
      if err := rottenDB.QueryRow(`insert into physical_sources(fqdn) values ($1) returning id`,fqdn).Scan(&physical_id); err == nil {
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
  go reportProgress(*noIdleHandsFlag, status_interval)

  for {
    var windowStart PoorMansTime
    var windowEnd PoorMansTime

    // first things first, reset pg_stat_statement data so that we might have a clean observation window
    if _, err := observedDB.Exec(`select pg_stat_statements_reset()`); err != nil {
      log.Fatalln("couldn't reset pg_stat_statements", err)
      // will now exit because Fatal
    }

    windowStart.sec = time.Now().Unix()

    time.Sleep(time.Duration(observation_interval) * time.Second)

    windowEnd.sec = time.Now().Unix()

    queries, err := observedDB.Query(`select query,calls,total_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written,temp_blks_written,temp_blks_read,blk_write_time,blk_read_time from pg_stat_statements`)
    if err != nil {
      log.Fatalln("couldn't select from pg_stat_statements", err)
      // will now exit because Fatal
    }
    for queries.Next() {
      newEvent := QueryEvent{}
      if err := queries.Scan(&newEvent.query, &newEvent.calls, &newEvent.total_time, &newEvent.rows, &newEvent.shared_blks_hit, &newEvent.shared_blks_read, &newEvent.shared_blks_dirtied, &newEvent.shared_blks_written, &newEvent.local_blks_hit, &newEvent.local_blks_read, &newEvent.local_blks_dirtied, &newEvent.local_blks_written, &newEvent.temp_blks_written, &newEvent.temp_blks_read, &newEvent.blk_write_time, &newEvent.blk_read_time); err != nil {
        log.Fatalln("couldn't parse query row", err)
        // will now exit because Fatal
      }
      newEvent.observationTimeStart = windowStart
      newEvent.observationTimeEnd = windowEnd

      eventCount++
      lastEventAt = newEvent.observationTimeEnd
      go processEvent(rottenDB, logical_id, physical_id, observation_interval, &newEvent)
    }
    queries.Close()
  }

  // until we implement graceful exiting, we'll never get here
  AppCleanup()
}


func reportProgress(noIdleHands bool, interval uint32) {
  almostDead := false
  lastProcessed := eventCount

  for {
//    log.Println(time.Now(),":",flying,"in flight,",eventCount,"processed so far,",warpTo.Offset,"seek, currently at:",lastEventAt)
    log.Println(eventCount,"processed so far, currently",time.Now().Unix()-lastEventAt.sec,"seconds behind")
    if (noIdleHands && lastProcessed == eventCount ) {
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
