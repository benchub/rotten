// Package state is the worker's local store: one SQLite file in StateDir
// that holds the last harvest's snapshot (and, from task -38, the outbox of
// unsent batches, in the same file so both commit in one transaction).
//
// Encoding. Timestamps are int64 microseconds since the Unix epoch (UTC), the
// precision Postgres uses, so stats_since, minmax_stats_since, and
// stats_reset round-trip exactly; sub-microsecond parts are truncated.
// Float64 counters are stored as their IEEE 754 bits in an INTEGER column,
// because SQLite turns NaN into NULL; that makes them exact bit for bit.
// Optional (version-dependent) fields are nullable columns.
//
// Query text isn't stored. Diff doesn't need it, and the text cache is
// task -23's job; loaded entries have Query == "".
//
// Transactions. The store has a single connection, and every transaction
// defers its rollback, so a failed or panicking one releases it. Inside
// Tx(fn), use only the tx passed to fn; never call Store methods (Load,
// Save, Tx) there. They'd wait for the connection fn already holds and
// deadlock.
//
// Schema changes. Open creates the v1 schema on an empty file and rejects a
// newer one. Task -38 must add a real v1-to-v2 migration step (create the
// outbox table and set user_version = 2 in one transaction) so existing
// stores upgrade in place instead of being treated as unknown.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/pgss"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// FileName is the store's file name inside StateDir.
const FileName = "state.db"

// DefaultOutboxCap is the default maximum queued harvest batches, about one
// day at five-minute windows.
const DefaultOutboxCap = 288

// schemaVersion is PRAGMA user_version.
const schemaVersion = 2

const schemaV1 = `
CREATE TABLE snapshot_meta (
	id          INTEGER PRIMARY KEY CHECK (id = 1),
	taken_at    INTEGER NOT NULL, -- µs since epoch
	stats_reset INTEGER NOT NULL, -- µs since epoch
	dealloc     INTEGER NOT NULL
) STRICT;
CREATE TABLE snapshot (
	userid    INTEGER NOT NULL,
	dbid      INTEGER NOT NULL,
	toplevel  INTEGER NOT NULL,
	queryid   INTEGER NOT NULL,
	plans                INTEGER NOT NULL,
	total_plan_time      INTEGER NOT NULL, -- float64 bits, here and below
	calls                INTEGER NOT NULL,
	total_exec_time      INTEGER NOT NULL,
	total_time           INTEGER NOT NULL,
	min_time             INTEGER NOT NULL,
	max_time             INTEGER NOT NULL,
	mean_time            INTEGER NOT NULL,
	stddev_time          INTEGER NOT NULL,
	rows                 INTEGER NOT NULL,
	shared_blks_hit      INTEGER NOT NULL,
	shared_blks_read     INTEGER NOT NULL,
	shared_blks_dirtied  INTEGER NOT NULL,
	shared_blks_written  INTEGER NOT NULL,
	local_blks_hit       INTEGER NOT NULL,
	local_blks_read      INTEGER NOT NULL,
	local_blks_dirtied   INTEGER NOT NULL,
	local_blks_written   INTEGER NOT NULL,
	temp_blks_read       INTEGER NOT NULL,
	temp_blks_written    INTEGER NOT NULL,
	shared_blk_read_time  INTEGER NOT NULL,
	shared_blk_write_time INTEGER NOT NULL,
	wal_records          INTEGER NOT NULL,
	wal_fpi              INTEGER NOT NULL,
	wal_bytes            INTEGER NOT NULL,
	temp_blk_read_time   INTEGER, -- 15+
	temp_blk_write_time  INTEGER,
	local_blk_read_time  INTEGER, -- 17+
	local_blk_write_time INTEGER,
	stats_since          INTEGER, -- µs since epoch
	minmax_stats_since   INTEGER,
	wal_buffers_full           INTEGER, -- 18+
	parallel_workers_to_launch INTEGER,
	parallel_workers_launched  INTEGER,
	PRIMARY KEY (userid, dbid, toplevel, queryid)
) STRICT;
`

const schemaV2Outbox = `
CREATE TABLE outbox (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	batch_id   TEXT NOT NULL UNIQUE,
	payload    BLOB NOT NULL,
	created_at INTEGER NOT NULL -- µs since epoch
) STRICT;
CREATE TABLE outbox_stats (
	id               INTEGER PRIMARY KEY CHECK (id = 1),
	dropped_cap      INTEGER NOT NULL DEFAULT 0,
	dropped_rejected INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT INTO outbox_stats (id, dropped_cap, dropped_rejected) VALUES (1, 0, 0);
`

// Options configures Open.
type Options struct {
	// MaxSnapshotAge: Load treats an older snapshot as a baseline. Zero
	// means no limit.
	MaxSnapshotAge time.Duration
	// OutboxCap limits queued harvest batches. Zero uses DefaultOutboxCap.
	OutboxCap int
	// Now defaults to time.Now.
	Now func() time.Time
}

// Store is the open state file.
type Store struct {
	db   *sql.DB
	path string
	opts Options
	lock *os.File
	// MovedAside is where Open moved a corrupt file, or "" if it didn't.
	MovedAside string
}

// Tx is a store transaction, for writing the snapshot together with other
// tables (the outbox) via SaveSnapshot.
type Tx = *sql.Tx

// Loaded is what Load returns.
type Loaded struct {
	// Baseline is true when there's no usable snapshot: the store is empty,
	// or the snapshot is older than MaxSnapshotAge (or taken in the
	// future). Then Snapshot is a zero snapshot (empty, non-nil Entries)
	// and TakenAt is zero, so diffing against it treats everything as new.
	Baseline bool
	Snapshot pgss.Snapshot
	TakenAt  time.Time
}

// OutboxBatch is one serialized SubmitHarvest request waiting to be sent.
type OutboxBatch struct {
	ID        int64
	BatchID   string
	Payload   []byte
	CreatedAt time.Time
}

// OutboxCounts reports queue length and durable drop counters.
type OutboxCounts struct {
	Queued          int
	DroppedCap      uint64
	DroppedRejected uint64
}

// OutboxEnqueueResult reports cap drops caused by one enqueue.
type OutboxEnqueueResult struct {
	DroppedCap int
}

// Open opens or creates the store in dir. If the file is corrupt, Open
// renames it to state.db.corrupt-<UTC timestamp> (with any -wal and -shm
// files), sets MovedAside, and starts a fresh, empty store.
func Open(dir string, opts Options) (*Store, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.OutboxCap == 0 {
		opts.OutboxCap = DefaultOutboxCap
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("state dir: %w", err)
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{path: filepath.Join(dir, FileName), opts: opts, lock: lock}
	if err := s.openOrRecover(); err != nil {
		lock.Close()
		return nil, fmt.Errorf("state store %s: %w", s.path, err)
	}
	return s, nil
}

// LockName is the lock file in StateDir. Open holds an exclusive flock on
// it until Close, so two workers can't share one StateDir.
const LockName = "state.lock"

func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, LockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("state lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another worker is using this StateDir (%s): %w", dir, err)
		}
		return nil, fmt.Errorf("state lock: %w", err)
	}
	return f, nil
}

func (s *Store) openOrRecover() error {
	if err := removeStray(s.path); err != nil {
		return err
	}
	// Check the main file read-only first. A normal open of a corrupt file
	// can make SQLite delete its -wal, which we want to keep for inspection;
	// immutable mode never touches -wal or -shm.
	err := probeImmutable(s)
	if isCorrupt(err) {
		if _, serr := os.Stat(s.path + "-wal"); serr == nil {
			// The main file may only look bad because the WAL isn't applied
			// (say, after a crash mid-checkpoint). Recheck on a copy.
			err = s.checkCopy()
			if afterCopyCheck != nil {
				afterCopyCheck(s)
			}
		}
	}
	if err == nil {
		var db *sql.DB
		if db, err = s.open(); err == nil {
			s.db = db
			return nil
		}
		if db != nil {
			db.Close()
		}
	}
	if !isCorrupt(err) {
		return err
	}
	if s.MovedAside, err = s.moveAside(); err != nil {
		return err
	}
	if s.db, err = s.open(); err != nil {
		if s.db != nil {
			s.db.Close()
			s.db = nil
		}
		return err
	}
	return nil
}

// removeStray removes a -wal or -shm without its main file, left over from a
// deleted store, so it can't be mistaken for part of a new one.
func removeStray(path string) error {
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	for _, sfx := range []string{"-wal", "-shm"} {
		if err := os.Remove(path + sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// errCorrupt marks a failed integrity check.
var errCorrupt = errors.New("integrity check failed")

func isCorrupt(err error) bool {
	if errors.Is(err, errCorrupt) {
		return true
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		switch se.Code() & 0xff {
		case sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB:
			return true
		}
	}
	return false
}

// Test hooks.
var (
	probeImmutable = (*Store).probe
	afterCopyCheck func(*Store)
)

// checkCopy copies the main file, -wal, and -shm to a temp dir and runs a
// normal quick_check there, with the WAL applied, leaving the originals
// untouched. A failure to make the copy is returned as is (not corrupt), so
// Open fails instead of moving a possibly healthy store aside.
func (s *Store) checkCopy() error {
	tmp, err := os.MkdirTemp("", "rotten-state-check-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	dst := filepath.Join(tmp, FileName)
	for _, sfx := range []string{"", "-wal", "-shm"} {
		b, err := os.ReadFile(s.path + sfx)
		if errors.Is(err, os.ErrNotExist) && sfx == "-shm" {
			continue
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(dst+sfx, b, 0o600); err != nil {
			return err
		}
	}
	db, err := sql.Open("sqlite", "file:"+dst)
	if err != nil {
		return err
	}
	defer db.Close()
	var res string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("%w: %s", errCorrupt, res)
	}
	return nil
}

// probe runs quick_check on an existing main file, opened read-only and
// immutable, so it never touches -wal or -shm. Immutable mode ignores the
// WAL, so a failure here is rechecked on a copy with the WAL (checkCopy).
// Remaining limit: if the probe passes on the main file alone but the normal
// open then finds corruption that only the WAL introduces, the files are
// still moved aside, but SQLite may already have removed the -wal by then.
func (s *Store) probe() error {
	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	db, err := sql.Open("sqlite", "file:"+s.path+"?mode=ro&immutable=1")
	if err != nil {
		return err
	}
	defer db.Close()
	var res string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("%w: %s", errCorrupt, res)
	}
	return nil
}

func (s *Store) open() (*sql.DB, error) {
	dsn := "file:" + s.path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite has one writer anyway, and it keeps
	// connection-scoped state predictable.
	db.SetMaxOpenConns(1)
	// On error the caller closes db, after moving a corrupt file aside.
	return db, initDB(db)
}

func initDB(db *sql.DB) error {
	var res string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("%w: %s", errCorrupt, res)
	}
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return err
	}
	switch {
	case v == schemaVersion:
		return nil
	case v > schemaVersion:
		return fmt.Errorf("schema version %d is newer than this worker's %d", v, schemaVersion)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch v {
	case 0:
		if _, err := tx.Exec(schemaV1); err != nil {
			return err
		}
		if _, err := tx.Exec(schemaV2Outbox); err != nil {
			return err
		}
	case 1:
		if _, err := tx.Exec(schemaV2Outbox); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported schema version %d", v)
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) moveAside() (string, error) {
	dst := s.path + ".corrupt-" + s.opts.Now().UTC().Format("20060102T150405.000000Z")
	// -wal and -shm first, so a crash midway never leaves a main file
	// paired with no WAL (or the moved-aside main file's WAL in place).
	for _, sfx := range []string{"-wal", "-shm"} {
		if err := os.Rename(s.path+sfx, dst+sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("move corrupt state file aside: %w", err)
		}
	}
	if err := os.Rename(s.path, dst); err != nil {
		return "", fmt.Errorf("move corrupt state file aside: %w", err)
	}
	return dst, nil
}

// Close closes the store.
// It releases the StateDir lock last.
func (s *Store) Close() error {
	err := s.db.Close()
	if lerr := s.lock.Close(); err == nil {
		err = lerr
	}
	return err
}

// Tx runs fn in one transaction, committing if fn returns nil and rolling
// back otherwise.
func (s *Store) Tx(ctx context.Context, fn func(Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Save atomically replaces the stored snapshot.
func (s *Store) Save(ctx context.Context, snap pgss.Snapshot, takenAt time.Time) error {
	return s.Tx(ctx, func(tx Tx) error { return SaveSnapshot(ctx, tx, snap, takenAt) })
}

// SaveSnapshotAndEnqueue saves the next snapshot and queues batch in the
// same transaction. If the outbox exceeds its cap after the enqueue, the
// oldest batches are dropped in that same transaction and counted.
func (s *Store) SaveSnapshotAndEnqueue(ctx context.Context, snap pgss.Snapshot, takenAt time.Time, batch *rottenv1.SubmitHarvestRequest) (OutboxEnqueueResult, error) {
	var result OutboxEnqueueResult
	payload, err := marshalHarvest(batch)
	if err != nil {
		return result, err
	}
	err = s.Tx(ctx, func(tx Tx) error {
		if err := SaveSnapshot(ctx, tx, snap, takenAt); err != nil {
			return err
		}
		var err error
		result, err = s.enqueuePayload(ctx, tx, batch.GetBatchId(), payload, takenAt)
		return err
	})
	return result, err
}

// EnqueueHarvest queues batch without changing the snapshot.
func (s *Store) EnqueueHarvest(ctx context.Context, batch *rottenv1.SubmitHarvestRequest, createdAt time.Time) (OutboxEnqueueResult, error) {
	var result OutboxEnqueueResult
	payload, err := marshalHarvest(batch)
	if err != nil {
		return result, err
	}
	err = s.Tx(ctx, func(tx Tx) error {
		var err error
		result, err = s.enqueuePayload(ctx, tx, batch.GetBatchId(), payload, createdAt)
		return err
	})
	return result, err
}

func marshalHarvest(batch *rottenv1.SubmitHarvestRequest) ([]byte, error) {
	if batch == nil {
		return nil, errors.New("harvest batch is required")
	}
	if batch.GetBatchId() == "" {
		return nil, errors.New("harvest batch_id is required")
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshal harvest batch: %w", err)
	}
	return payload, nil
}

func (s *Store) enqueuePayload(ctx context.Context, tx Tx, batchID string, payload []byte, createdAt time.Time) (OutboxEnqueueResult, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox (batch_id, payload, created_at)
		VALUES (?, ?, ?)
		ON CONFLICT(batch_id) DO UPDATE SET payload = excluded.payload`, batchID, payload, createdAt.UnixMicro()); err != nil {
		return OutboxEnqueueResult{}, err
	}
	return s.enforceOutboxCap(ctx, tx)
}

func (s *Store) enforceOutboxCap(ctx context.Context, tx Tx) (OutboxEnqueueResult, error) {
	if s.opts.OutboxCap < 0 {
		return OutboxEnqueueResult{}, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM outbox ORDER BY id ASC LIMIT (
		SELECT max(count(*) - ?, 0) FROM outbox
	)`, s.opts.OutboxCap)
	if err != nil {
		return OutboxEnqueueResult{}, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return OutboxEnqueueResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return OutboxEnqueueResult{}, err
	}
	if len(ids) == 0 {
		return OutboxEnqueueResult{}, nil
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE id = ?", id); err != nil {
			return OutboxEnqueueResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE outbox_stats SET dropped_cap = dropped_cap + ? WHERE id = 1`, len(ids)); err != nil {
		return OutboxEnqueueResult{}, err
	}
	return OutboxEnqueueResult{DroppedCap: len(ids)}, nil
}

// NextOutboxBatch returns the oldest queued batch, or nil when the queue is empty.
func (s *Store) NextOutboxBatch(ctx context.Context) (*OutboxBatch, error) {
	var batch *OutboxBatch
	err := s.Tx(ctx, func(tx Tx) error {
		row := tx.QueryRowContext(ctx, `SELECT id, batch_id, payload, created_at FROM outbox ORDER BY id ASC LIMIT 1`)
		var b OutboxBatch
		var created int64
		if err := row.Scan(&b.ID, &b.BatchID, &b.Payload, &created); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		b.CreatedAt = micros(created)
		batch = &b
		return nil
	})
	return batch, err
}

// DeleteOutboxBatch deletes a batch after the server acknowledges it.
func (s *Store) DeleteOutboxBatch(ctx context.Context, id int64) error {
	return s.Tx(ctx, func(tx Tx) error {
		_, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE id = ?", id)
		return err
	})
}

// DropRejectedOutboxBatch deletes a non-retryable rejected batch and counts it.
func (s *Store) DropRejectedOutboxBatch(ctx context.Context, id int64) error {
	return s.Tx(ctx, func(tx Tx) error {
		res, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE id = ?", id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox_stats SET dropped_rejected = dropped_rejected + ? WHERE id = 1`, n)
		return err
	})
}

// OutboxCounts returns queue length and drop counters.
func (s *Store) OutboxCounts(ctx context.Context) (OutboxCounts, error) {
	var counts OutboxCounts
	err := s.Tx(ctx, func(tx Tx) error {
		var capDropped, rejectedDropped int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM outbox`).Scan(&counts.Queued); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT dropped_cap, dropped_rejected FROM outbox_stats WHERE id = 1`).Scan(&capDropped, &rejectedDropped); err != nil {
			return err
		}
		counts.DroppedCap = uint64(capDropped)
		counts.DroppedRejected = uint64(rejectedDropped)
		return nil
	})
	return counts, err
}

// SaveSnapshot replaces the stored snapshot inside tx. It's atomic only as
// part of tx.
func SaveSnapshot(ctx context.Context, tx Tx, snap pgss.Snapshot, takenAt time.Time) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM snapshot"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO snapshot_meta (id, taken_at, stats_reset, dealloc)
		VALUES (1, ?, ?, ?)`, takenAt.UnixMicro(), snap.Info.StatsReset.UnixMicro(), snap.Info.Dealloc); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range snap.Entries {
		if _, err := stmt.ExecContext(ctx, row(e)...); err != nil {
			return fmt.Errorf("save snapshot entry %d: %w", e.QueryID, err)
		}
	}
	return nil
}

const columns = `userid, dbid, toplevel, queryid, plans, total_plan_time, calls, total_exec_time,
	total_time, min_time, max_time, mean_time, stddev_time, rows,
	shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
	local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written,
	temp_blks_read, temp_blks_written, shared_blk_read_time, shared_blk_write_time,
	wal_records, wal_fpi, wal_bytes, temp_blk_read_time, temp_blk_write_time,
	local_blk_read_time, local_blk_write_time, stats_since, minmax_stats_since,
	wal_buffers_full, parallel_workers_to_launch, parallel_workers_launched`

const insertSQL = `INSERT INTO snapshot (` + columns + `) VALUES (` +
	`?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

func fbits(f float64) int64 { return int64(math.Float64bits(f)) }

func optF(p *float64) any {
	if p == nil {
		return nil
	}
	return fbits(*p)
}

func optI(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func optT(p *time.Time) any {
	if p == nil {
		return nil
	}
	return p.UnixMicro()
}

func row(e pgss.Stat) []any {
	return []any{
		int64(e.UserID), int64(e.DBID), e.TopLevel, e.QueryID,
		e.Plans, fbits(e.TotalPlanTime), e.Calls, fbits(e.TotalExecTime),
		fbits(e.TotalTime), fbits(e.MinTime), fbits(e.MaxTime), fbits(e.MeanTime), fbits(e.StddevTime), e.Rows,
		e.SharedBlksHit, e.SharedBlksRead, e.SharedBlksDirtied, e.SharedBlksWritten,
		e.LocalBlksHit, e.LocalBlksRead, e.LocalBlksDirtied, e.LocalBlksWritten,
		e.TempBlksRead, e.TempBlksWritten, fbits(e.SharedBlkReadTime), fbits(e.SharedBlkWriteTime),
		e.WALRecords, e.WALFPI, fbits(e.WALBytes), optF(e.TempBlkReadTime), optF(e.TempBlkWriteTime),
		optF(e.LocalBlkReadTime), optF(e.LocalBlkWriteTime), optT(e.StatsSince), optT(e.MinmaxStatsSince),
		optI(e.WALBuffersFull), optI(e.ParallelWorkersToLaunch), optI(e.ParallelWorkersLaunched),
	}
}

func micros(us int64) time.Time { return time.UnixMicro(us).UTC() }

// Load reads the snapshot. See Loaded for when it's a baseline.
func (s *Store) Load(ctx context.Context) (Loaded, error) {
	baseline := Loaded{Baseline: true, Snapshot: pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{}}}
	var out Loaded
	err := s.Tx(ctx, func(tx Tx) error {
		var taken, reset int64
		err := tx.QueryRowContext(ctx, "SELECT taken_at, stats_reset, dealloc FROM snapshot_meta WHERE id = 1").
			Scan(&taken, &reset, &out.Snapshot.Info.Dealloc)
		if errors.Is(err, sql.ErrNoRows) {
			out = baseline
			return nil
		}
		if err != nil {
			return err
		}
		out.TakenAt = micros(taken)
		out.Snapshot.Info.StatsReset = micros(reset)
		age := s.opts.Now().Sub(out.TakenAt)
		if age < 0 || (s.opts.MaxSnapshotAge > 0 && age > s.opts.MaxSnapshotAge) {
			out = baseline
			return nil
		}
		return loadEntries(ctx, tx, &out.Snapshot)
	})
	if err != nil {
		return Loaded{}, fmt.Errorf("load snapshot: %w", err)
	}
	return out, nil
}

func loadEntries(ctx context.Context, tx Tx, snap *pgss.Snapshot) error {
	rows, err := tx.QueryContext(ctx, "SELECT "+columns+" FROM snapshot")
	if err != nil {
		return err
	}
	defer rows.Close()
	snap.Entries = map[pgss.Key]pgss.Stat{}
	for rows.Next() {
		var e pgss.Stat
		var uid, dbid int64
		var tpt, tet, tt, mn, mx, mean, sd, sbr, sbw, wb int64
		var tbr, tbw, lbr, lbw, ss, mss, wbf, pwl, pwd sql.NullInt64
		if err := rows.Scan(&uid, &dbid, &e.TopLevel, &e.QueryID, &e.Plans, &tpt, &e.Calls, &tet,
			&tt, &mn, &mx, &mean, &sd, &e.Rows,
			&e.SharedBlksHit, &e.SharedBlksRead, &e.SharedBlksDirtied, &e.SharedBlksWritten,
			&e.LocalBlksHit, &e.LocalBlksRead, &e.LocalBlksDirtied, &e.LocalBlksWritten,
			&e.TempBlksRead, &e.TempBlksWritten, &sbr, &sbw,
			&e.WALRecords, &e.WALFPI, &wb, &tbr, &tbw, &lbr, &lbw, &ss, &mss, &wbf, &pwl, &pwd); err != nil {
			return err
		}
		f := func(b int64) float64 { return math.Float64frombits(uint64(b)) }
		of := func(n sql.NullInt64) *float64 {
			if !n.Valid {
				return nil
			}
			v := f(n.Int64)
			return &v
		}
		oi := func(n sql.NullInt64) *int64 {
			if !n.Valid {
				return nil
			}
			v := n.Int64
			return &v
		}
		ot := func(n sql.NullInt64) *time.Time {
			if !n.Valid {
				return nil
			}
			v := micros(n.Int64)
			return &v
		}
		e.UserID, e.DBID = uint32(uid), uint32(dbid)
		e.TotalPlanTime, e.TotalExecTime, e.TotalTime = f(tpt), f(tet), f(tt)
		e.MinTime, e.MaxTime, e.MeanTime, e.StddevTime = f(mn), f(mx), f(mean), f(sd)
		e.SharedBlkReadTime, e.SharedBlkWriteTime, e.WALBytes = f(sbr), f(sbw), f(wb)
		e.TempBlkReadTime, e.TempBlkWriteTime = of(tbr), of(tbw)
		e.LocalBlkReadTime, e.LocalBlkWriteTime = of(lbr), of(lbw)
		e.StatsSince, e.MinmaxStatsSince = ot(ss), ot(mss)
		e.WALBuffersFull, e.ParallelWorkersToLaunch, e.ParallelWorkersLaunched = oi(wbf), oi(pwl), oi(pwd)
		snap.Entries[pgss.KeyOf(e)] = e
	}
	return rows.Err()
}
