package worker

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func callsFor(batches []*rottenv1.SubmitHarvestRequest, fp string) (uint64, bool) {
	var calls uint64
	found := false
	for _, b := range batches {
		for _, a := range b.GetAggregates() {
			if a.GetFingerprint() == fp {
				calls += a.GetMetrics().GetCalls()
				found = true
			}
		}
	}
	return calls, found
}

// On 16 there's no stats_since, so a query first run after the baseline is
// held: window 1 sends nothing for it and logs the held count and calls, and
// window 2 sends only its growth.
func TestHarvestHoldsEntryMissingFromSnapshotOn16(t *testing.T) {
	logs := &lockedBuffer{}
	old := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(old) })

	observed := startObservedForWorker(t)
	store := openStore(t, t.TempDir())
	defer store.Close()
	workload := observed.Connect(t)
	rn := startOutboxWorker(t, observed.DSNAs(t, "rotten_observer"), 7, 42, store, store)
	fp := fingerprintOf(t, diffQuery)

	runDiffWorkload(t, workload, 3)
	rn.window(t)
	if calls, found := callsFor(drainOutboxBatches(t, store), fp); found {
		t.Fatalf("window 1 sent %d calls for the held query, want nothing", calls)
	}
	if !strings.Contains(logs.String(), "calls) weren't in the snapshot but may predate it") {
		t.Fatalf("missing held log line: %s", logs.String())
	}

	runDiffWorkload(t, workload, 2)
	rn.window(t)
	rn.stop(t)
	if calls, _ := callsFor(drainOutboxBatches(t, store), fp); calls != 2 {
		t.Fatalf("window 2 sent %d calls, want only the growth of 2", calls)
	}
}

// On 18 a query created after the baseline has stats_since after the
// snapshot, so it's new and counts in full.
func TestHarvestCountsEntryCreatedAfterSnapshotOn18(t *testing.T) {
	observed := startObservedVersionForWorker(t, 18)
	store := openStore(t, t.TempDir())
	defer store.Close()
	workload := observed.Connect(t)
	rn := startOutboxWorker(t, observed.DSNAs(t, "rotten_observer"), 7, 42, store, store)

	runDiffWorkload(t, workload, 3)
	rn.window(t)
	rn.stop(t)
	if calls, _ := callsFor(drainOutboxBatches(t, store), fingerprintOf(t, diffQuery)); calls != 3 {
		t.Fatalf("sent %d calls, want 3", calls)
	}
}
