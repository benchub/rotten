package main

import (
  "errors"
  "fmt"
  "log"
  "time"
  "sync"
  "math"
  "database/sql"
  "regexp"

  "github.com/lfittl/pg_query_go"
)

// runningstat is adapted from https://gist.github.com/turnersr/11390535, which in turn credits:
//     1. Numerically Stable, Single-Pass, Parallel Statistics Algorithms - http://www.janinebennett.org/index_files/ParallelStatisticsAlgorithms.pdf
//     2. Accurately computing running variance - http://www.johndcook.com/standard_deviation.html 
type RunningStat struct {
  m_n int64
  m_oldM float64
  m_newM float64
  m_oldS float64
  m_newS float64
}

var knownStatsDomains = [15]string{"calls",
                                   "time",
                                   "rows",
                                   "shared_blks_hit",
                                   "shared_blks_read",
                                   "shared_blks_dirtied",
                                   "shared_blks_written",
                                   "local_blks_hit",
                                   "local_blks_read",
                                   "local_blks_dirtied",
                                   "local_blks_written",
                                   "temp_blks_read",
                                   "temp_blks_written",
                                   "blk_read_time",
                                   "blk_write_time"} 


type Samples struct {
  metrics map[string]float64

  // when
  unixtime int64
}

type Fingerprint struct {
  statsLock sync.RWMutex
  samples chan *Samples
  
  // how many of these we've seen since startup
  count uint64

  // unixtime of when this fingerprint last got stats
  last int64
  
  // some numbers for logging
  calls_since_start float64
  time_since_start float64

  // running statistics for the various metrics we track
  stats map[string]*RunningStat
  allTimeStats map[string]*RunningStat

  // for inserts
  db_id uint64
}

// A dictionary of what fingerprints we've seen since startup and are currently processing
var protectedFingerprints = struct{
  sync.RWMutex
  m map[uint64]*Fingerprint
} {m: make(map[uint64]*Fingerprint)}

var re_controller,_ = regexp.Compile(`.+/\*controller:(.+),action:.+,hostname:.*,pid:\d+,context_id:[\-0-9a-f]+\*/`)
var re_action,_ = regexp.Compile(`.+/\*controller:.+,action:(.+),hostname:.*,pid:\d+,context_id:[\-0-9a-f]+\*/`)




func processEvent(rottenDB *sql.DB, logical_source_id uint32, physical_source_id uint32, observation_interval uint32, event *QueryEvent) {
  var fingerprint_id uint64
  var controller_id uint32
  var action_id uint32

  controller_qid := ""
  controller_description := ""
  action_qid := ""
  action_description := ""

  if re_controller.MatchString(event.query) {
    matches := re_controller.FindStringSubmatch(event.query)
    if err := rottenDB.QueryRow(`select id from controllers where controller=$1`,matches[0]).Scan(&controller_id); err == nil {
      // yay, we have our ID
      controller_qid = fmt.Sprintf(",%d",controller_id)
      controller_description = ",controller_id"
    } else if err == sql.ErrNoRows {
      if err := rottenDB.QueryRow(`insert into controllers(controller) values ($1) returning id`,matches[0]).Scan(&controller_id); err == nil {
        // yay, we have our ID
      controller_qid = fmt.Sprintf(",%d",controller_id)
      controller_description = ",controller_id"
      } else {
        log.Fatalln("couldn't insert into controllers", err)
        // will now exit because Fatal
      }
    } else {
      log.Fatalln("couldn't select from controllers", err)
      // will now exit because Fatal
    }
  }

  if re_action.MatchString(event.query) {
    matches := re_action.FindStringSubmatch(event.query)
    if err := rottenDB.QueryRow(`select id from actions where action=$1`,matches[0]).Scan(&action_id); err == nil {
      // yay, we have our ID
      action_qid = fmt.Sprintf(",%d",action_id)
      action_description = ",action_id"
    } else if err == sql.ErrNoRows {
      if err := rottenDB.QueryRow(`insert into actions(action) values ($1) returning id`,matches[0]).Scan(&action_id); err == nil {
        // yay, we have our ID
        action_qid = fmt.Sprintf(",%d",action_id)
        action_description = ",action_id"
      } else {
        log.Fatalln("couldn't insert into actions", err)
        // will now exit because Fatal
      }
    } else {
      log.Fatalln("couldn't select from actions", err)
      // will now exit because Fatal
    }
  }

  fingerprint_id, err := normalized_id(rottenDB, event)
  if err != nil {
    log.Printf("failed to get fingerprint for event, so ignoring it")
    return
  }

  newQuerySQL := fmt.Sprintf("insert into events (fingerprint_id,logical_source_id,physical_source_id,observed_window,calls,time%s%s) values (%d,%d,%d,tstzrange(to_timestamp(%d), to_timestamp(%d)),%f,%f%s%s)", controller_description, action_description, fingerprint_id, logical_source_id, physical_source_id, event.observationTimeStart.sec,event.observationTimeEnd.sec, event.calls, event.total_time, controller_qid, action_qid) 
  if _, err := rottenDB.Exec(newQuerySQL); err != nil {
    log.Fatalln("couldn't insert into events", err)
    // will now exit because Fatal
  }

  // now that that's done, update our various fingerprint stats
  sample := Samples{}
  sample.metrics = make(map[string]float64)
  sample.unixtime = time.Now().Unix()
  sample.metrics["calls"] = event.calls
  sample.metrics["time"] = event.total_time
  sample.metrics["rows"] = event.rows
  sample.metrics["shared_blks_hit"] = event.shared_blks_hit
  sample.metrics["shared_blks_read"] = event.shared_blks_read
  sample.metrics["shared_blks_dirtied"] = event.shared_blks_dirtied
  sample.metrics["shared_blks_written"] = event.shared_blks_written
  sample.metrics["local_blks_hit"] = event.local_blks_hit
  sample.metrics["local_blks_read"] = event.local_blks_read
  sample.metrics["local_blks_dirtied"] = event.local_blks_dirtied
  sample.metrics["local_blks_written"] = event.local_blks_written
  sample.metrics["temp_blks_read"] = event.temp_blks_read
  sample.metrics["temp_blks_written"] = event.temp_blks_written
  sample.metrics["blk_read_time"] = event.blk_read_time
  sample.metrics["blk_write_time"] = event.blk_write_time

  // If we've already started a goroutine for this fingerprint, send this event to that channel.
  // If not, start a new goroutine and make a channel for it to consume from.
  protectedFingerprints.RLock()
  existingFingerprint, present := protectedFingerprints.m[fingerprint_id]
  protectedFingerprints.RUnlock()
  if present {
    existingFingerprint.samples <- &sample
  } else {
  newFingerprint := Fingerprint{}
  newFingerprint.stats = make(map[string]*RunningStat)
  for _,stats_domain := range knownStatsDomains {
    newStats := RunningStat{}
    newFingerprint.stats[stats_domain] = &newStats
  }
  newFingerprint.db_id = fingerprint_id

  newFingerprint.samples = make(chan *Samples)
    
    protectedFingerprints.Lock()
    protectedFingerprints.m[fingerprint_id] = &newFingerprint
    protectedFingerprints.Unlock()

    // update the statistics for these samples as they come in
    go consumeSamples(&newFingerprint)

    // save the statistics to the db on a different schedule, which should reduce the writes to the rotten DB
    // (multiple updates in memory might get folded into a single update on disk)
    go reportSamples(rottenDB, &newFingerprint, logical_source_id, observation_interval)

    newFingerprint.samples <- &sample
  }

  // now that the event has been recorded and the stats updated, our work is done and this goroutine can end.
}



func normalized_id(rottenDB *sql.DB, event *QueryEvent) (db_id uint64, err error) {
  var fingerprint_id uint64

  normalized, err := pg_query.Normalize(event.query)
  if err != nil {
    log.Printf("couldn't normalize query", event.query, err)
    return 0, errors.New("failed to normalize")
  }
  fingerprint, err := pg_query.FastFingerprint(event.query)
  if err != nil {
    log.Printf("couldn't fingerprint query", event.query, err)
    return 0, errors.New("failed to fingerprint")
  }
  
  if err := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`,fingerprint).Scan(&fingerprint_id); err == nil {
    // yay, we have our ID
  } else if err == sql.ErrNoRows {
    if err := rottenDB.QueryRow(`insert into fingerprints(fingerprint,normalized) values ($1,$2) returning id`,fingerprint,normalized).Scan(&fingerprint_id); err == nil {
      // yay, we have our ID
    } else {
      // we couldn't insert, probably because another session got here first. See what id it got
      if err := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`,fingerprint).Scan(&fingerprint_id); err == nil {
        // yay, we have our ID
      } else {
         log.Fatalln("couldn't select newly inserted fingerprint", err)
         // will now exit because Fatal            
      }
    }
  } else {
    log.Fatalln("couldn't select fingerprint", err)
    // will now exit because Fatal
  }

  return fingerprint_id, nil
}



func reportSamples(rottenDB *sql.DB, f *Fingerprint, logical_source_id uint32, observation_interval uint32) {
  lastReport := f.last
  for {
    time.Sleep(time.Duration(2*observation_interval)*time.Second)
    if f.last > lastReport {
      // Let's record these stats we've been collecting for this fingerprint.
      // As we walk through all of them for both this logical_source_id and logical_source_id=0, we
      // *could* try to only hold a lock on the fingerprint stats block as little as possible.
      // However, with all the looping and rolling back potential, not only is that prone to errors and fragile
      // for code change, but it's unclear what we should should do if there is an error. Keep the stats around
      // for the next pass? Or just jettison them and start over?
      // We're going to just take a simple write lock for this entire loop, and reset the stats when we're done
      // with the loop. That's stronger than is necessary, but it keeps things clean and simple.

      f.statsLock.Lock()
      // do this report both for the logical source id and 0, which is the special logical source of "everywhere"
      LogicalLoop:
      for _,source_id := range [2]uint32{0,logical_source_id} {
        var dbStats map[string]RunningStat

        dbStats = make(map[string]RunningStat)

        tx, err := rottenDB.Begin();
        if err != nil {
          log.Println("couldn't start transaction for (fingerprint_id,logical_source_id)", f.db_id, source_id, err)
          continue
        }

        queryResults, err := tx.Query(`select type,count,mean,deviation from fingerprint_stats where fingerprint_id=$1 and logical_source_id=$2 for update`, f.db_id,source_id)
        if err != nil {
          log.Println("couldn't select from dbstats using fingerprint_id,logical_source_id", f.db_id,source_id,err)
          tx.Rollback()
          continue LogicalLoop
        }
        statsFound := 0
        for queryResults.Next() {
          statsFound++
          var theseStats RunningStat
          var query_stats_domain string

          if err := queryResults.Scan(&query_stats_domain,&theseStats.m_n,&theseStats.m_oldM,&theseStats.m_newS); err != nil {
            log.Println("couldn't parse dbstats row using fingerprint_id,logical_source_id", f.db_id,source_id,err)
            tx.Rollback()
            continue LogicalLoop
          }

          // we've retried the deviation from the db, but the RunningStats structure doesn't keep that number internally
          if theseStats.m_n > 1 {
            theseStats.m_oldS = theseStats.m_newS * theseStats.m_newS * float64(theseStats.m_n) - theseStats.m_newS * theseStats.m_newS
            theseStats.m_newS = theseStats.m_oldS
          } 
          dbStats[query_stats_domain] = theseStats
        }
        queryResults.Close()
        if statsFound == 0 {
          for _,stats_domain := range knownStatsDomains {
              r, err := tx.Query(`INSERT INTO fingerprint_stats(fingerprint_id, logical_source_id, type, last, count, mean, deviation) VALUES ($1, $2, $3, $4, $5, $6, $7)`, f.db_id, source_id, stats_domain, f.last, f.stats[stats_domain].m_n, f.stats[stats_domain].m_oldM, RunningStatDeviation(*(f.stats[stats_domain])))
              if err != nil {
                log.Println("couldn't insert new fingerprint stats for fingerprint, logical_source_id, type", f.db_id, source_id, stats_domain, err)
                tx.Rollback()
                continue LogicalLoop
              }
              r.Close()
          }
        } else if statsFound != 15 {
          log.Printf("Only found", statsFound, "stats to update, not all 15, for fingerprint_id, logical_source_id", statsFound, f.db_id, source_id)
          tx.Rollback()
          continue LogicalLoop
        } else {
          // we found all our stats; iterate over them and update them all for this source_id
          var combined RunningStat

          for _,stats_domain := range knownStatsDomains {
            // verify query_stats_domain is a domain we know about
            _, present := f.stats[stats_domain]
            if present {
               // we had stats before; merge them with what we have now, then zero out what we have so we only merge in new data
              // https://gist.github.com/turnersr/11390535
              oldStats := dbStats[stats_domain]

              oldStats.m_newM = oldStats.m_oldM
              oldStats.m_newS = oldStats.m_oldS

              delta := oldStats.m_oldM - f.stats[stats_domain].m_oldM
              delta2 := delta*delta 
              combined.m_n = f.stats[stats_domain].m_n + oldStats.m_n
              combined.m_oldM = f.stats[stats_domain].m_newM + float64(oldStats.m_n)*delta/float64(combined.m_n)
              combined.m_newM = combined.m_oldM

              q := float64(f.stats[stats_domain].m_n * oldStats.m_n) * delta2 / float64(combined.m_n)
              combined.m_oldS = f.stats[stats_domain].m_newS + oldStats.m_newS + q
              combined.m_newS = combined.m_oldS

              r, err := tx.Query(`update fingerprint_stats set last=$1,count=$2,mean=$3,deviation=$4 where fingerprint_id=$5 and logical_source_id=$6 and type=$7`,f.last,combined.m_n,combined.m_oldM,RunningStatDeviation(combined),f.db_id,source_id,stats_domain)
              if err != nil {
                log.Println("couldn't update fingerprint stats for fingerprint_id,logical_source_id,type", f.db_id, source_id, stats_domain, err)
                tx.Rollback()
                continue LogicalLoop
              }
              r.Close()
              //log.Printf("fingerprint %d has seen %d calls; last at %d, sum at %f (%f), mean %f, deviation %f", f.db_id, combined.m_n, f.last, f.sum, float64(combined.m_n)*combined.m_oldM, RunningStatMean(combined), RunningStatDeviation(combined))
            } else {
              log.Println("dbstats row for fingerprint_id,logical_source_id gave unknown type", f.db_id,source_id,stats_domain)
              tx.Rollback()
              continue LogicalLoop
            }
          }
        }

        err = tx.Commit()
        if err != nil {
          log.Printf("Couldn't commit fingerprint stats update for fingerprint_id, logical_source_id",f.db_id,source_id)
        }
        lastReport = f.last
      }

      for _,stats_domain := range knownStatsDomains {
        RunningStatReset(f.stats[stats_domain])
      }
      f.statsLock.Unlock()
    }
    // it would be slick if we got rid of this fingerprint if it didn't happen again for a while
  }
}

func consumeSamples(f *Fingerprint) {
  for {
    sample := <- f.samples

    // lock the stats block for writing our update
    f.statsLock.Lock()
    f.last = sample.unixtime

    // update some additional numbers for logging
    f.calls_since_start += sample.metrics["calls"]
    f.time_since_start += sample.metrics["time"]

    for _,stats_domain := range knownStatsDomains {
      mergedStat := Push(sample.metrics[stats_domain],*(f.stats[stats_domain]))
      f.stats[stats_domain] = &mergedStat
    }
    f.statsLock.Unlock()

  }
}

// https://www.johndcook.com/blog/standard_deviation/
func Push(x float64, oldRS RunningStat) RunningStat {
  rs := oldRS
  rs.m_n += 1 
  if(rs.m_n == 1) {
    rs.m_oldM = x
    rs.m_newM = x
    rs.m_oldS = 0
  } else {
    rs.m_newM = rs.m_oldM + (x - rs.m_oldM)/float64(rs.m_n)
    rs.m_newS = rs.m_oldS + (x - rs.m_oldM)*(x - rs.m_newM)

    rs.m_oldM = rs.m_newM
    rs.m_oldS = rs.m_newS
  }
  return rs
}

func RunningStatReset(rs *RunningStat) {
  rs.m_n = 0
  rs.m_oldM = 0
  rs.m_newM = 0
  rs.m_oldS = 0
  rs.m_newS = 0
}

func RunningStatMean(rs RunningStat) float64 {
  if rs.m_n > 0 {
    return rs.m_newM
  }

  return 0
}

func RunningStatVariance(rs RunningStat) float64 {
  if rs.m_n > 1 {
    return rs.m_newS/float64(rs.m_n - 1)
  }

  return 0
}

func RunningStatDeviation(rs RunningStat) float64 {

  return math.Sqrt(RunningStatVariance(rs))
}
