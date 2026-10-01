package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pgss"
)

func fp(v float64) *float64 { return &v }
func ip(v int64) *int64     { return &v }
func tp(t time.Time) *time.Time {
	return &t
}

// fullStat sets every field, with awkward floats and microsecond timestamps.
func fullStat() pgss.Stat {
	return pgss.Stat{
		UserID: math.MaxUint32, DBID: 7, TopLevel: true, QueryID: math.MinInt64,
		Plans: 1, TotalPlanTime: 0.1 + 0.2, Calls: math.MaxInt64, TotalExecTime: math.SmallestNonzeroFloat64,
		TotalTime: math.MaxFloat64, MinTime: math.Copysign(0, -1), MaxTime: 1e-300, MeanTime: 3.141592653589793, StddevTime: math.Inf(1),
		Rows: 2, SharedBlksHit: 3, SharedBlksRead: 4, SharedBlksDirtied: 5, SharedBlksWritten: 6,
		LocalBlksHit: 7, LocalBlksRead: 8, LocalBlksDirtied: 9, LocalBlksWritten: 10,
		TempBlksRead: 11, TempBlksWritten: 12,
		SharedBlkReadTime: 13.000000000000002, SharedBlkWriteTime: 14.5,
		WALRecords: 15, WALFPI: 16, WALBytes: 1.7976931348623157e308,
		TempBlkReadTime: fp(18.25), TempBlkWriteTime: fp(0),
		LocalBlkReadTime: fp(20.1), LocalBlkWriteTime: fp(21.9),
		StatsSince:       tp(time.Date(2026, 9, 30, 12, 34, 56, 123456000, time.UTC)),
		MinmaxStatsSince: tp(time.Date(2026, 9, 30, 12, 34, 56, 123457000, time.UTC)),
		WALBuffersFull:   ip(0), ParallelWorkersToLaunch: ip(22), ParallelWorkersLaunched: ip(-1),
	}
}

func minimalStat() pgss.Stat {
	return pgss.Stat{UserID: 1, DBID: 2, TopLevel: false, QueryID: 42, Calls: 3, TotalExecTime: 1.5}
}

func snapOf(info pgss.Info, stats ...pgss.Stat) pgss.Snapshot {
	s := pgss.Snapshot{Info: info, Entries: map[pgss.Key]pgss.Stat{}}
	for _, st := range stats {
		s.Entries[pgss.KeyOf(st)] = st
	}
	return s
}

var testInfo = pgss.Info{Dealloc: 9, StatsReset: time.Date(2026, 1, 2, 3, 4, 5, 999999000, time.UTC)}

func open(t *testing.T, dir string, now time.Time) *Store {
	t.Helper()
	s, err := Open(dir, Options{MaxSnapshotAge: time.Hour, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// floatBits compares every float bit for bit (so -0, Inf, and NaN count).
func assertSameSnapshot(t *testing.T, want, got pgss.Snapshot) {
	t.Helper()
	if !want.Info.StatsReset.Equal(got.Info.StatsReset) || want.Info.Dealloc != got.Info.Dealloc {
		t.Errorf("info: want %+v, got %+v", want.Info, got.Info)
	}
	if len(want.Entries) != len(got.Entries) {
		t.Fatalf("entries: want %d, got %d", len(want.Entries), len(got.Entries))
	}
	for k, w := range want.Entries {
		g, ok := got.Entries[k]
		if !ok {
			t.Fatalf("missing key %+v", k)
		}
		compareStruct(t, reflect.ValueOf(w), reflect.ValueOf(g), "Stat")
	}
}

func compareStruct(t *testing.T, w, g reflect.Value, path string) {
	t.Helper()
	switch w.Kind() {
	case reflect.Struct:
		if w.Type() == reflect.TypeOf(time.Time{}) {
			wt, gt := w.Interface().(time.Time), g.Interface().(time.Time)
			if !wt.Equal(gt) {
				t.Errorf("%s: want %v, got %v", path, wt, gt)
			}
			return
		}
		for i := 0; i < w.NumField(); i++ {
			compareStruct(t, w.Field(i), g.Field(i), path+"."+w.Type().Field(i).Name)
		}
	case reflect.Pointer:
		if w.IsNil() != g.IsNil() {
			t.Errorf("%s: nil mismatch, want nil=%v", path, w.IsNil())
			return
		}
		if !w.IsNil() {
			compareStruct(t, w.Elem(), g.Elem(), path)
		}
	case reflect.Float64:
		if math.Float64bits(w.Float()) != math.Float64bits(g.Float()) {
			t.Errorf("%s: want bits %x, got %x", path, math.Float64bits(w.Float()), math.Float64bits(g.Float()))
		}
	case reflect.String:
		// Query text isn't stored.
	default:
		if w.Interface() != g.Interface() {
			t.Errorf("%s: want %v, got %v", path, w.Interface(), g.Interface())
		}
	}
}

func TestRoundTrip(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	nan := minimalStat()
	nan.QueryID = 43
	nan.StddevTime = math.NaN()
	want := snapOf(testInfo, fullStat(), minimalStat(), nan)
	takenAt := now.Add(-time.Minute).Add(123 * time.Microsecond)

	s := open(t, dir, now)
	if err := s.Save(context.Background(), want, takenAt); err != nil {
		t.Fatalf("Save: %v", err)
	}
	s.Close()

	// Reopen so the data really came off disk.
	s = open(t, dir, now)
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Baseline {
		t.Fatalf("fresh snapshot loaded as baseline")
	}
	if !got.TakenAt.Equal(takenAt) {
		t.Errorf("taken_at: want %v, got %v", takenAt, got.TakenAt)
	}
	assertSameSnapshot(t, want, got.Snapshot)
}

func TestEmptyStoreIsBaseline(t *testing.T) {
	s := open(t, t.TempDir(), time.Now())
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline || len(got.Snapshot.Entries) != 0 || !got.TakenAt.IsZero() {
		t.Fatalf("empty store: want baseline with zero snapshot, got %+v", got)
	}
}

func TestStaleSnapshotIsBaseline(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s := open(t, t.TempDir(), now)
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now.Add(-time.Hour-time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline || len(got.Snapshot.Entries) != 0 {
		t.Fatalf("stale snapshot: want baseline with zero snapshot, got %+v", got)
	}

	// Exactly MaxSnapshotAge old is still usable.
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Baseline || len(got.Snapshot.Entries) != 1 {
		t.Fatalf("snapshot at the age limit: want usable, got %+v", got)
	}

	// Taken in the future (clock went backward): baseline.
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now.Add(time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline || len(got.Snapshot.Entries) != 0 || !got.TakenAt.IsZero() {
		t.Fatalf("future snapshot: want baseline, got %+v", got)
	}
}

func TestCorruptFileMovedAside(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	junk := []byte(strings.Repeat("this is not a sqlite database ", 200))
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, sfx := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(path+sfx, []byte("junk"+sfx), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	s := open(t, dir, now)
	if s.MovedAside == "" {
		t.Fatal("MovedAside not set")
	}
	for _, sfx := range []string{"-wal", "-shm"} {
		b, err := os.ReadFile(s.MovedAside + sfx)
		if err != nil || string(b) != "junk"+sfx {
			t.Errorf("%s not moved aside: %q, %v", sfx, b, err)
		}
	}
	moved, err := os.ReadFile(s.MovedAside)
	if err != nil || string(moved) != string(junk) {
		t.Fatalf("corrupt file not preserved at %q: %v", s.MovedAside, err)
	}
	if !strings.HasPrefix(filepath.Base(s.MovedAside), FileName+".corrupt-20261001T100000") {
		t.Errorf("moved-aside name %q has no timestamp", s.MovedAside)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline {
		t.Fatal("corrupt store: want baseline")
	}
	// And the fresh store works.
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now); err != nil {
		t.Fatal(err)
	}
}

func TestFailedSaveKeepsOldSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := open(t, t.TempDir(), now)
	old := snapOf(testInfo, minimalStat())
	if err := s.Save(ctx, old, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	// A failure partway through Save itself: a trigger rejects the second entry.
	if _, err := s.db.Exec(`CREATE TEMP TRIGGER boom BEFORE INSERT ON snapshot
		WHEN NEW.queryid = 99 BEGIN SELECT RAISE(ABORT, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	a, b := fullStat(), minimalStat()
	b.QueryID = 99
	if err := s.Save(ctx, snapOf(pgss.Info{StatsReset: now}, a, b), now); err == nil {
		t.Fatal("Save: want error from trigger")
	}
	check := func() {
		t.Helper()
		got, err := s.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got.Baseline || !got.TakenAt.Equal(now.Add(-time.Minute)) {
			t.Fatalf("old snapshot lost: %+v", got)
		}
		assertSameSnapshot(t, old, got.Snapshot)
	}
	check()

	// A failure after SaveSnapshot in a caller's transaction (the -38 outbox path).
	errLater := errors.New("outbox insert failed")
	err := s.Tx(ctx, func(tx Tx) error {
		if err := SaveSnapshot(ctx, tx, snapOf(pgss.Info{StatsReset: now}, a), now); err != nil {
			return err
		}
		return errLater
	})
	if !errors.Is(err, errLater) {
		t.Fatalf("Tx: want errLater, got %v", err)
	}
	check()
}

func TestSecondOpenFails(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if s2, err := Open(dir, Options{}); err == nil {
		s2.Close()
		t.Fatal("second Open: want error")
	} else if !strings.Contains(err.Error(), "another worker is using this StateDir") {
		t.Fatalf("second Open: unclear error %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s3.Close()
}

// liveWAL saves a snapshot in a store under a fresh dir and, while it's still
// open (so the WAL isn't checkpointed away), copies its main file, -wal, and
// -shm. It returns the copies' contents keyed by suffix ("" is the main file).
func liveWAL(t *testing.T, now time.Time) map[string][]byte {
	t.Helper()
	src := t.TempDir()
	a, err := Open(src, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Save(context.Background(), snapOf(testInfo, minimalStat()), now); err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, sfx := range []string{"", "-wal", "-shm"} {
		b, err := os.ReadFile(filepath.Join(src, FileName+sfx))
		if err != nil {
			t.Fatal(err)
		}
		out[sfx] = b
	}
	if len(out["-wal"]) == 0 {
		t.Fatal("source store has an empty WAL")
	}
	return out
}

func plant(t *testing.T, dir string, files map[string][]byte, sfxs ...string) {
	t.Helper()
	for _, sfx := range sfxs {
		if err := os.WriteFile(filepath.Join(dir, FileName+sfx), files[sfx], 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRemoveStray(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	files := liveWAL(t, now)

	// No main file: both strays go.
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	plant(t, dir, files, "-wal", "-shm")
	if err := removeStray(path); err != nil {
		t.Fatal(err)
	}
	for _, sfx := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + sfx); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stray %s still there: %v", sfx, err)
		}
	}

	// Main file present: nothing is removed.
	dir = t.TempDir()
	path = filepath.Join(dir, FileName)
	plant(t, dir, files, "", "-wal", "-shm")
	if err := removeStray(path); err != nil {
		t.Fatal(err)
	}
	for _, sfx := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(path + sfx); err != nil {
			t.Errorf("%q removed with its main file present: %v", sfx, err)
		}
	}
}

// A real WAL from another store, with no main file, must not be replayed
// into the new store. (SQLite on its own doesn't replay it either, so this
// pins the outcome; TestRemoveStray pins the removal.)
func TestStrayWALRemovedWithoutMainFile(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	files := liveWAL(t, now)
	dir := t.TempDir()
	plant(t, dir, files, "-wal", "-shm")

	s := open(t, dir, now)
	if s.MovedAside != "" {
		t.Errorf("no main file, but MovedAside = %q", s.MovedAside)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Baseline || len(got.Snapshot.Entries) != 0 {
		t.Fatalf("stray WAL was replayed: %+v", got)
	}
	if err := s.Save(context.Background(), snapOf(testInfo, minimalStat()), now); err != nil {
		t.Fatal(err)
	}
}

// When the immutable probe fails but a -wal exists, the store is checked
// again on a copy with the WAL applied. A store that's healthy that way isn't
// moved aside, and the probe never touches the originals.
func TestProbeFailureWithHealthyWALIsNotCorrupt(t *testing.T) {
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	files := liveWAL(t, now)
	dir := t.TempDir()
	plant(t, dir, files, "", "-wal", "-shm")

	orig := probeImmutable
	t.Cleanup(func() { probeImmutable = orig })
	var walDuringCopyCheck []byte
	probeImmutable = func(s *Store) error {
		return fmt.Errorf("%w: simulated mid-checkpoint main file", errCorrupt)
	}
	afterCopyCheck = func(s *Store) {
		walDuringCopyCheck, _ = os.ReadFile(s.path + "-wal")
	}
	t.Cleanup(func() { afterCopyCheck = nil })

	s := open(t, dir, now)
	if s.MovedAside != "" {
		t.Fatalf("healthy store moved aside to %q", s.MovedAside)
	}
	if string(walDuringCopyCheck) != string(files["-wal"]) {
		t.Error("copy check changed the original -wal")
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Baseline || len(got.Snapshot.Entries) != 1 {
		t.Fatalf("want the WAL's snapshot, got %+v", got)
	}
}

// A transaction that fails or panics must roll back, or the single
// connection stays busy and the next call hangs. The timeout makes a hang
// fail fast.
func TestFailedTxReleasesConnection(t *testing.T) {
	s := open(t, t.TempDir(), time.Now())
	_ = s.Tx(context.Background(), func(Tx) error { return errors.New("fail") })
	func() {
		defer func() { _ = recover() }()
		_ = s.Tx(context.Background(), func(Tx) error { panic("boom") })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.Load(ctx); err != nil {
		t.Fatalf("Load after failed Tx: %v", err)
	}
}
