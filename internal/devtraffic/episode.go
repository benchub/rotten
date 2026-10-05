package devtraffic

import "time"

// Episode is a stretch of time in which one kind of query runs much slower
// than usual, so rotten's outliers report, which scores each window's time
// per call for a fingerprint against its own earlier history, has something
// to find. An episode slows its shape without changing its SQL, so the
// fingerprint stays the same.
type Episode string

const (
	// NoEpisode is normal running.
	NoEpisode Episode = ""
	// SlowRead reads ExportShape's roughly 1 MB result slowly. Postgres
	// counts the time it spends blocked on the full socket as execution
	// time, so pg_stat_statements sees the export slow down.
	SlowRead Episode = "slow_read"
	// LockWait holds the row locks of users 1..HotUsers in an open
	// transaction (LockShape, as the SisImport.process_users job) for about
	// 1.5 seconds at a time, while touch_user picks only those users, so its
	// calls wait on the locks.
	LockWait Episode = "lock_wait"
	// SlowSleep makes course_activity's pg_sleep($2) argument 0.3 to 0.6
	// seconds instead of 0.
	SlowSleep Episode = "sleep"
)

// Episodes lists the episode kinds in the order the schedule runs them.
func Episodes() []Episode { return []Episode{SlowRead, LockWait, SlowSleep} }

// The shapes and hot users the episodes use.
const (
	ExportShape = "export_enrollments"
	LockShape   = "lock_hot_users"
	HotUsers    = 20
)

// Episode timings: the LockWait holder keeps its locks for lockHold, then
// lets go for lockGap; a SlowRead pauses readPause every readEvery rows.
const (
	lockHold  = 1500 * time.Millisecond
	lockGap   = 300 * time.Millisecond
	readEvery = 50
	readPause = 10 * time.Millisecond
)

// ScheduledEpisode returns the episode running at t, and when it started,
// for an episode lasting length at the start of every period of every,
// counted from the Unix epoch so the times are easy to find. Kinds take
// turns in Episodes order, so each kind recurs every 3*every. An every of 0
// or less turns episodes off.
func ScheduledEpisode(every, length time.Duration, t time.Time) (Episode, time.Time) {
	if every <= 0 || length <= 0 {
		return NoEpisode, time.Time{}
	}
	n := t.UnixNano() / int64(every)
	start := time.Unix(0, n*int64(every)).UTC()
	if t.Sub(start) >= length {
		return NoEpisode, time.Time{}
	}
	kinds := Episodes()
	return kinds[n%int64(len(kinds))], start
}

func validEpisode(ep Episode) bool {
	if ep == NoEpisode {
		return true
	}
	for _, k := range Episodes() {
		if ep == k {
			return true
		}
	}
	return false
}
