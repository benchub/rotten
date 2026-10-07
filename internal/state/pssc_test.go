package state

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pssc"
)

func psscSnapOf(stats ...pssc.Stat) pssc.Snapshot {
	s := pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{}}
	for _, st := range stats {
		s.Entries[pssc.KeyOf(st)] = st
	}
	return s
}

func psscStats() []pssc.Stat {
	since := time.Date(2026, 9, 30, 12, 34, 56, 123456000, time.UTC)
	return []pssc.Stat{
		{UserID: math.MaxUint32, DBID: 7, QueryID: math.MinInt64, TopLevel: true,
			Tags:  map[string]string{"controller": "users", "action": "show"},
			Calls: math.MaxInt64, ExecTime: 0.1 + 0.2, StatsSince: since},
		// Same entry key apart from the tags.
		{UserID: math.MaxUint32, DBID: 7, QueryID: math.MinInt64, TopLevel: true,
			Tags:  map[string]string{"controller": pssc.Capped, "action": ""},
			Calls: 1, ExecTime: math.Copysign(0, -1), StatsSince: since},
		{UserID: 1, DBID: 2, QueryID: 42, TopLevel: false, Tags: map[string]string{},
			Calls: 3, ExecTime: math.NaN(), StatsSince: since.Add(time.Microsecond)},
	}
}

func assertSamePSSC(t *testing.T, want, got pssc.Snapshot) {
	t.Helper()
	if len(want.Entries) != len(got.Entries) {
		t.Fatalf("pssc entries: want %d, got %d", len(want.Entries), len(got.Entries))
	}
	for k, w := range want.Entries {
		g, ok := got.Entries[k]
		if !ok {
			t.Fatalf("missing pssc key %+v", k)
		}
		if g.UserID != w.UserID || g.DBID != w.DBID || g.QueryID != w.QueryID || g.TopLevel != w.TopLevel ||
			g.Calls != w.Calls || math.Float64bits(g.ExecTime) != math.Float64bits(w.ExecTime) ||
			!g.StatsSince.Equal(w.StatsSince) || pssc.CanonicalTags(g.Tags) != pssc.CanonicalTags(w.Tags) ||
			len(g.Tags) != len(w.Tags) {
			t.Errorf("pssc entry %+v: want %+v, got %+v", k, w, g)
		}
	}
}

func saveBoth(t *testing.T, s *Store, takenAt time.Time, p pssc.Snapshot) {
	t.Helper()
	ctx := context.Background()
	err := s.Tx(ctx, func(tx Tx) error {
		if err := SaveSnapshot(ctx, tx, snapOf(testInfo, minimalStat()), takenAt); err != nil {
			return err
		}
		return SavePSSCSnapshot(ctx, tx, p)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPSSCRoundTrip(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	want := psscSnapOf(psscStats()...)
	s := open(t, dir, now)
	ctx := context.Background()
	saveBoth(t, s, now.Add(-time.Minute), want)
	s.Close()

	// Reopen so the data really came off disk.
	s = open(t, dir, now)
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Baseline {
		t.Fatal("loaded as baseline")
	}
	assertSamePSSC(t, want, got.PSSC)

	// A later save replaces, not appends.
	smaller := psscSnapOf(psscStats()[2])
	if err := s.Tx(ctx, func(tx Tx) error { return SavePSSCSnapshot(ctx, tx, smaller) }); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertSamePSSC(t, smaller, got.PSSC)
}

// TestPSSCBaselineFollowsSnapshot: the pssc snapshot is usable only when the
// pgss snapshot is, since both come from the same harvest.
func TestPSSCBaselineFollowsSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s := open(t, t.TempDir(), now)
	ctx := context.Background()
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.PSSC.Entries == nil || len(got.PSSC.Entries) != 0 {
		t.Fatalf("empty store: want empty non-nil pssc entries, got %+v", got.PSSC)
	}
	saveBoth(t, s, now.Add(-2*time.Hour), psscSnapOf(psscStats()...))
	got, err = s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline || got.PSSC.Entries == nil || len(got.PSSC.Entries) != 0 {
		t.Fatalf("stale: want baseline with empty pssc, got %+v", got)
	}
}

// TestPgssSaveClearsPSSC: a pgss-only save (pssc not read this harvest)
// mustn't leave the old pssc rows under a fresh taken_at.
func TestPgssSaveClearsPSSC(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s := open(t, t.TempDir(), now)
	saveBoth(t, s, now.Add(-2*time.Minute), psscSnapOf(psscStats()...))
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Baseline || len(got.PSSC.Entries) != 0 {
		t.Fatalf("want usable snapshot with no pssc entries, got baseline=%v pssc=%d", got.Baseline, len(got.PSSC.Entries))
	}
}

func TestOpenMigratesV4StoreToPSSC(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{schemaV1, schemaV2Outbox, schemaV3SourceRegistration, schemaV4StaleSourceDrops, "PRAGMA user_version = 4"} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s := open(t, dir, now)
	want := psscSnapOf(psscStats()...)
	saveBoth(t, s, now, want)
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertSamePSSC(t, want, got.PSSC)
}
