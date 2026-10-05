package main

import (
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
)

// TestComposeStopShutsDownCleanly runs the compose service's own command and
// does what `docker compose stop` does: SIGTERM to the process it started
// (the container's PID 1) only. The generator must get the signal, log its
// shutdown, and exit 0 within the grace period, during a lock_wait episode
// so lock holders are mid-transaction.
func TestComposeStopShutsDownCleanly(t *testing.T) {
	db := testdb.StartObserved(t, 18)
	proc := testdb.StartDevComposeCommand(t, "traffic", []string{
		"TRAFFIC_ADMIN_DSN=" + db.DSN,
		"TRAFFIC_SHARDS=1",
		"TRAFFIC_SCALE=0.05",
		"TRAFFIC_RATE=5",
		"TRAFFIC_EPISODE=lock_wait",
	})
	// The first start may compile.
	proc.WaitForLog(t, "lock_wait episode for the whole run", 4*time.Minute)
	time.Sleep(3 * time.Second)

	log := proc.ComposeStop(t)
	if !strings.Contains(log, "devtraffic: stopped after") {
		t.Fatalf("no shutdown log line after SIGTERM:\n%s", log)
	}
	if !strings.Contains(log, " 0 errors") {
		t.Errorf("shutdown wasn't clean:\n%s", log)
	}
}
