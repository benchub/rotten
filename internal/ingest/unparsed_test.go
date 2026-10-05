package ingest_test

import (
	"context"
	"testing"
	"time"
)

// FingerprintAggregate.unparsed marks a fallback fingerprint. It's stored on
// the fingerprint when ingest first inserts it; unset, as from a worker
// older than the field, means parsed.
func TestSubmitHarvestStoresUnparsedFlag(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Minute)
	req := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "unparsed", "fp-parsed", "unparsed-0123456789abcdef")
	req.Msg.Aggregates[1].Unparsed = true
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	// A later batch can't flip the flag, as it can't change normalized.
	next := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(30*time.Second), "unparsed-2", "fp-parsed", "unparsed-0123456789abcdef")
	next.Msg.Aggregates[0].Unparsed = true
	if _, err := f.handler.SubmitHarvest(f.ctx, next); err != nil {
		t.Fatalf("second SubmitHarvest: %v", err)
	}
	for fp, want := range map[string]bool{"fp-parsed": false, "unparsed-0123456789abcdef": true} {
		var got bool
		var calls float64
		if err := f.owner.QueryRow(context.Background(), `select f.unparsed, sum(e.calls)
			from rotten.fingerprints f join rotten.events e on e.fingerprint_id = f.id
			where f.fingerprint = $1 group by f.unparsed`, fp).Scan(&got, &calls); err != nil {
			t.Fatalf("%s: %v", fp, err)
		}
		if got != want {
			t.Errorf("%s unparsed = %v, want %v", fp, got, want)
		}
		if calls == 0 {
			t.Errorf("%s has no calls", fp)
		}
	}
}

// A server from before the unparsed flag, running on the migrated schema,
// stores a new worker's fallback fingerprints as parsed. The next batch that
// flags one unparsed marks it, keeping the representative SQL, and a batch
// without the flag doesn't clear it.
func TestSubmitHarvestRepairsUnparsedFlagFromOldServer(t *testing.T) {
	f := setupSubmit(t)
	const fp = "unparsed-fedcba9876543210"
	if _, err := f.owner.Exec(context.Background(),
		"insert into rotten.fingerprints (fingerprint, normalized) values ($1, 'first text')", fp); err != nil {
		t.Fatal(err)
	}
	stored := func() (bool, string) {
		t.Helper()
		var unparsed bool
		var normalized string
		if err := f.owner.QueryRow(context.Background(),
			"select unparsed, normalized from rotten.fingerprints where fingerprint = $1", fp).Scan(&unparsed, &normalized); err != nil {
			t.Fatal(err)
		}
		return unparsed, normalized
	}
	start := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Minute)

	req := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "repair-1", fp)
	req.Msg.Aggregates[0].Unparsed = true
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	if unparsed, normalized := stored(); !unparsed || normalized != "first text" {
		t.Fatalf("after a flagged batch: unparsed = %v, normalized = %q; want true, %q", unparsed, normalized, "first text")
	}

	again := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(30*time.Second), "repair-2", fp)
	if _, err := f.handler.SubmitHarvest(f.ctx, again); err != nil {
		t.Fatalf("second SubmitHarvest: %v", err)
	}
	if unparsed, _ := stored(); !unparsed {
		t.Fatal("a batch without the flag cleared it")
	}

	// The repair's grant covers unparsed alone.
	for _, column := range []string{"fingerprint", "normalized"} {
		if _, err := f.ingest.Exec(context.Background(),
			"update rotten.fingerprints set "+column+" = 'x' where fingerprint = $1", fp); err == nil {
			t.Errorf("rotten_ingest can update fingerprints.%s", column)
		}
	}
}
