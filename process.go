package main

import (
  "fmt"
  "log"
  "time"
  "sync"
  "math"
  "database/sql"
  "regexp"
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

var protectedProcessingCounter = struct{
  sync.RWMutex
  v uint32
} {v: 0}


var re_action_hash,_ = regexp.Compile(`action:([\d]+)`)
var re_controller_hash,_ = regexp.Compile(`controller:([\d]+)`)
var re_job_tag_hash,_ = regexp.Compile(`job_tag:([\d]+)`)


func fingerprintCount() int {
  protectedFingerprints.RLock()
  known := len(protectedFingerprints.m)
  protectedFingerprints.RUnlock()

  return known
}

func stillProcessing() uint32 {
  protectedProcessingCounter.RLock()
  v := protectedProcessingCounter.v
  protectedProcessingCounter.RUnlock()

  return v
}

func processEvent(rottenDB *sql.DB, logical_source_id uint32, physical_source_id uint32, observation_interval uint32, fingerprint string, event *QueryEvent) {
  protectedProcessingCounter.Lock()
  protectedProcessingCounter.v++
  protectedProcessingCounter.Unlock()

  fingerprint_id, err := normalized_fingerprint_id(rottenDB, fingerprint, event)
  if err != nil {
    log.Println("failed to get fingerprint for event, so ignoring it")
    return
  }

  tx, err := rottenDB.Begin();
  if err != nil {
    log.Println("couldn't start transaction for new event (fingerprint_id", fingerprint_id, "logical_source_id", logical_source_id, ")", err)
    return
  }

  var event_id uint32
  newQuerySQL := fmt.Sprintf("insert into events (fingerprint_id,logical_source_id,physical_source_id,observed_window,calls,time) values (%d,%d,%d,tstzrange(to_timestamp(%d), to_timestamp(%d)),%f,%f) returning id", fingerprint_id, logical_source_id, physical_source_id, event.observationTimeStart.sec,event.observationTimeEnd.sec, event.calls, event.total_time) 
  if err := tx.QueryRow(newQuerySQL).Scan(&event_id); err != nil {
    log.Fatalln("couldn't insert into events", newQuerySQL, err)
    // will now exit because Fatal
  }

  for hash, count := range event.context {
    // dehash our context so we know what to put into the db
    // "controller:%d,action:%d,job_tag:%d"
    columns := ""
    values := ""

    if re_action_hash.MatchString(hash) {
      matches := re_action_hash.FindStringSubmatch(hash)
      if len(matches) > 1 {
        columns = columns + ",action_id"
        values = values + "," + matches[1]
      }
    }

    if re_controller_hash.MatchString(hash) {
      matches := re_controller_hash.FindStringSubmatch(hash)
      if len(matches) > 1 {
        columns = columns + ",controller_id"
        values = values + "," + matches[1]
      }
    }

    if re_job_tag_hash.MatchString(hash) {
      matches := re_job_tag_hash.FindStringSubmatch(hash)
      if len(matches) > 1 {
        columns = columns + ",job_tag_id"
        values = values + "," + matches[1]
      }
    }

    newQuerySQL := fmt.Sprintf("insert into event_context (event_id,c%s) values (%d,%d%s)", columns, event_id, count, values) 
    if _, err := tx.Exec(newQuerySQL); err != nil {
      log.Fatalln("couldn't insert into events", err)
      // will now exit because Fatal
    }
  }

  err = tx.Commit()
  if err != nil {
    log.Println("couldn't commit transaction for new event (fingerprint_id", fingerprint_id, "logical_source_id", logical_source_id, ")", err)
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
  protectedProcessingCounter.Lock()
  protectedProcessingCounter.v--
  protectedProcessingCounter.Unlock()
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
          log.Println("Only found", statsFound, "stats to update, not all 15, for fingerprint_id, logical_source_id", statsFound, f.db_id, source_id)
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
              //log.Println("fingerprint %d has seen %d calls; last at %d, sum at %f (%f), mean %f, deviation %f", f.db_id, combined.m_n, f.last, f.sum, float64(combined.m_n)*combined.m_oldM, RunningStatMean(combined), RunningStatDeviation(combined))
            } else {
              log.Println("dbstats row for fingerprint_id,logical_source_id gave unknown type", f.db_id,source_id,stats_domain)
              tx.Rollback()
              continue LogicalLoop
            }
          }
        }

        err = tx.Commit()
        if err != nil {
          log.Println("Couldn't commit fingerprint stats update for fingerprint_id, logical_source_id",f.db_id,source_id)
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
