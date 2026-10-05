package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	runningstat "github.com/benchub/runningstat"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/harvestlimits"
)

// SubmitHarvest records one worker observation window. Duplicate batch IDs
// are acknowledged without writing rows a second time.
func (h *Handler) SubmitHarvest(ctx context.Context, req *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	key, ok := auth.FromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing pass key"))
	}
	msg := req.Msg
	windowStart, windowEnd, err := h.validateHarvestEnvelope(msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	hash, err := harvestContentHash(msg)
	if err != nil {
		h.logger.Error("hash harvest failed", "key_id", key.ID, "batch_id", msg.GetBatchId(), "err", err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("harvest ingest failed"))
	}

	tx, err := h.db.Begin(ctx)
	if err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), submitError{op: "begin", err: err})
	}
	defer tx.Rollback(ctx)

	fqdn, err := checkSource(ctx, tx, msg.GetLogicalSourceId(), msg.GetPhysicalSourceId())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("source is not allowed for this pass key"))
		}
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}
	if !key.AllowsFQDN(fqdn) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("source is not allowed for this pass key"))
	}
	if err := lockSource(ctx, tx, msg.GetPhysicalSourceId()); err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}

	inserted, existingHash, err := insertBatch(ctx, tx, key.ID, msg, windowStart, windowEnd, hash)
	if err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}
	if !inserted {
		if len(existingHash) > 0 && string(existingHash) != string(hash) {
			h.logger.Warn("duplicate batch_id with different content", "key_id", key.ID, "batch_id", msg.GetBatchId())
		}
		if err := tx.Commit(ctx); err != nil {
			return h.submitDBError(key.ID, msg.GetBatchId(), submitError{op: "commit", err: err})
		}
		return connect.NewResponse(&rottenv1.SubmitHarvestResponse{
			BatchId: msg.GetBatchId(),
			Status:  rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE,
		}), nil
	}

	overlap, err := hasOverlappingWindow(ctx, tx, msg.GetBatchId(), msg.GetPhysicalSourceId(), windowStart, windowEnd)
	if err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}
	if overlap {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("harvest window overlaps a previously ingested window"))
	}
	ids, err := resolveHarvestIdentities(ctx, tx, msg.GetAggregates())
	if err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}
	for _, aggregate := range msg.GetAggregates() {
		if err := insertAggregate(ctx, tx, msg, aggregate, ids, windowStart, windowEnd); err != nil {
			if errors.Is(err, errInvalidHarvest) {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("malformed harvest batch"))
			}
			return h.submitDBError(key.ID, msg.GetBatchId(), err)
		}
	}
	if err := mergeFingerprintStats(ctx, tx, msg, ids, windowEnd); err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return h.submitDBError(key.ID, msg.GetBatchId(), submitError{op: "commit", err: err})
	}
	return connect.NewResponse(&rottenv1.SubmitHarvestResponse{
		BatchId: msg.GetBatchId(),
		Status:  rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED,
	}), nil
}

type submitError struct {
	op  string
	err error
}

func (e submitError) Error() string { return e.op + " submit harvest: " + e.err.Error() }
func (e submitError) Unwrap() error { return e.err }

func (h *Handler) submitDBError(keyID int64, batchID string, err error) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	h.logger.Error("submit harvest failed", "key_id", keyID, "batch_id", batchID, "err", err)
	if isUnavailable(err) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("harvest ingest unavailable"))
	}
	return nil, connect.NewError(connect.CodeInternal, errors.New("harvest ingest failed"))
}

func (h *Handler) validateHarvestEnvelope(msg *rottenv1.SubmitHarvestRequest) (time.Time, time.Time, error) {
	return harvestlimits.ValidateHarvest(msg, h.now().UTC(), harvestlimits.CheckFutureSkew)
}

func harvestContentHash(msg *rottenv1.SubmitHarvestRequest) ([]byte, error) {
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

func checkSource(ctx context.Context, tx pgx.Tx, logicalID, physicalID uint32) (string, error) {
	var fqdn string
	err := tx.QueryRow(ctx, `select p.fqdn
		from rotten.logical_physical_sources lp
		join rotten.physical_sources p on p.id = lp.physical_source_id
		where lp.logical_source_id = $1 and lp.physical_source_id = $2`, logicalID, physicalID).Scan(&fqdn)
	return fqdn, err
}

func lockSource(ctx context.Context, tx pgx.Tx, physicalID uint32) error {
	_, err := tx.Exec(ctx, `select pg_advisory_xact_lock($1::bigint)`, int64(physicalID))
	return err
}

func insertBatch(ctx context.Context, tx pgx.Tx, keyID int64, msg *rottenv1.SubmitHarvestRequest, start, end time.Time, hash []byte) (bool, []byte, error) {
	var inserted bool
	err := tx.QueryRow(ctx, `insert into rotten.ingested_batches
		(batch_id, key_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, content_hash)
		values ($1, $2, $3, $4, $5, $6, $7)
		on conflict (batch_id) do nothing
		returning true`, msg.GetBatchId(), keyID, msg.GetLogicalSourceId(), msg.GetPhysicalSourceId(), start, end, hash).Scan(&inserted)
	if err == nil {
		return true, nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, nil, err
	}
	var existingHash []byte
	if err := tx.QueryRow(ctx, `select content_hash from rotten.ingested_batches where batch_id = $1`, msg.GetBatchId()).Scan(&existingHash); err != nil {
		return false, nil, err
	}
	return false, existingHash, nil
}

func hasOverlappingWindow(ctx context.Context, tx pgx.Tx, batchID string, physicalID uint32, start, end time.Time) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `select exists (
		select 1 from rotten.ingested_batches
		where physical_source_id = $1
		  and batch_id <> $2
		  and observed_window_start < $4
		  and observed_window_end > $3
		  and not (observed_window_start = $3 and observed_window_end = $4)
	)`, physicalID, batchID, start, end).Scan(&exists)
	return exists, err
}

var errInvalidHarvest = errors.New("invalid harvest")

var fingerprintStatDomains = [...]string{
	"calls",
	"total_time",
	"min_time",
	"max_time",
	"mean_time",
	"stddev_time",
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
	"blk_write_time",
}

type harvestIDs struct {
	fingerprints map[string]int64
	controllers  map[string]int64
	actions      map[string]int64
	jobTags      map[string]int64
}

func resolveHarvestIdentities(ctx context.Context, tx pgx.Tx, aggregates []*rottenv1.FingerprintAggregate) (harvestIDs, error) {
	ids := harvestIDs{
		fingerprints: map[string]int64{},
		controllers:  map[string]int64{},
		actions:      map[string]int64{},
		jobTags:      map[string]int64{},
	}
	normalized := map[string]string{}
	unparsed := map[string]bool{}
	var fingerprints, controllers, actions, jobTags []string
	for _, aggregate := range aggregates {
		fp := aggregate.GetFingerprint()
		if _, ok := normalized[fp]; !ok {
			normalized[fp] = aggregate.GetNormalized()
			unparsed[fp] = aggregate.GetUnparsed()
			fingerprints = append(fingerprints, fp)
		}
		for _, qc := range aggregate.GetContexts() {
			if value := qc.GetController(); value != "" {
				if _, ok := ids.controllers[value]; !ok {
					ids.controllers[value] = 0
					controllers = append(controllers, value)
				}
			}
			if value := qc.GetAction(); value != "" {
				if _, ok := ids.actions[value]; !ok {
					ids.actions[value] = 0
					actions = append(actions, value)
				}
			}
			if value := qc.GetJobTag(); value != "" {
				if _, ok := ids.jobTags[value]; !ok {
					ids.jobTags[value] = 0
					jobTags = append(jobTags, value)
				}
			}
		}
	}
	sort.Strings(fingerprints)
	sort.Strings(controllers)
	sort.Strings(actions)
	sort.Strings(jobTags)
	for _, fingerprint := range fingerprints {
		id, err := fingerprintID(ctx, tx, fingerprint, normalized[fingerprint], unparsed[fingerprint])
		if err != nil {
			return harvestIDs{}, err
		}
		ids.fingerprints[fingerprint] = id
	}
	for _, controller := range controllers {
		id, err := lookupControllerID(ctx, tx, controller)
		if err != nil {
			return harvestIDs{}, err
		}
		ids.controllers[controller] = id
	}
	for _, action := range actions {
		id, err := lookupActionID(ctx, tx, action)
		if err != nil {
			return harvestIDs{}, err
		}
		ids.actions[action] = id
	}
	for _, jobTag := range jobTags {
		id, err := lookupJobTagID(ctx, tx, jobTag)
		if err != nil {
			return harvestIDs{}, err
		}
		ids.jobTags[jobTag] = id
	}
	return ids, nil
}

func insertAggregate(ctx context.Context, tx pgx.Tx, msg *rottenv1.SubmitHarvestRequest, aggregate *rottenv1.FingerprintAggregate, ids harvestIDs, start, end time.Time) error {
	fingerprintID := ids.fingerprints[aggregate.GetFingerprint()]
	var eventID int64
	metrics := aggregate.GetMetrics()
	if err := tx.QueryRow(ctx, `insert into rotten.events
		(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
		values ($1, $2, $3, $4, $5, $6, $7)
		returning id`, fingerprintID, msg.GetLogicalSourceId(), msg.GetPhysicalSourceId(), start, end, metrics.GetCalls(), metrics.GetTotalTime()).Scan(&eventID); err != nil {
		return err
	}
	var contextTotal uint64
	for _, qc := range aggregate.GetContexts() {
		contextTotal += qc.GetCount()
	}
	for _, qc := range aggregate.GetContexts() {
		controllerID := optionalID(ids.controllers, qc.GetController())
		actionID := optionalID(ids.actions, qc.GetAction())
		jobTagID := optionalID(ids.jobTags, qc.GetJobTag())
		// The same arithmetic, in the same order, as migration 0011's backfill.
		attributedTime := metrics.GetTotalTime() * float64(qc.GetCount()) / float64(contextTotal)
		if _, err := tx.Exec(ctx, `insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c,
			 logical_source_id, attributed_time)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, eventID, start, end, controllerID, actionID, jobTagID, qc.GetCount(),
			msg.GetLogicalSourceId(), attributedTime); err != nil {
			return err
		}
	}
	return nil
}

type fingerprintStatsAccumulator struct {
	fingerprintID int64
	stats         map[string]*runningstat.RunningStat
}

func mergeFingerprintStats(ctx context.Context, tx pgx.Tx, msg *rottenv1.SubmitHarvestRequest, ids harvestIDs, windowEnd time.Time) error {
	accumulators := map[int64]*fingerprintStatsAccumulator{}
	var ordered []int64
	for _, aggregate := range msg.GetAggregates() {
		fingerprintID := ids.fingerprints[aggregate.GetFingerprint()]
		accumulator := accumulators[fingerprintID]
		if accumulator == nil {
			accumulator = newFingerprintStatsAccumulator(fingerprintID)
			accumulators[fingerprintID] = accumulator
			ordered = append(ordered, fingerprintID)
		}
		accumulateFingerprintStats(accumulator, aggregate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i] < ordered[j]
	})
	for _, fingerprintID := range ordered {
		accumulator := accumulators[fingerprintID]
		for _, sourceID := range [2]uint32{0, msg.GetLogicalSourceId()} {
			if err := mergeFingerprintStatsForSource(ctx, tx, accumulator, sourceID, windowEnd.Unix()); err != nil {
				return err
			}
		}
	}
	return nil
}

func newFingerprintStatsAccumulator(fingerprintID int64) *fingerprintStatsAccumulator {
	stats := make(map[string]*runningstat.RunningStat, len(fingerprintStatDomains))
	for _, domain := range fingerprintStatDomains {
		stats[domain] = &runningstat.RunningStat{}
	}
	return &fingerprintStatsAccumulator{fingerprintID: fingerprintID, stats: stats}
}

func accumulateFingerprintStats(accumulator *fingerprintStatsAccumulator, aggregate *rottenv1.FingerprintAggregate) {
	metrics := aggregate.GetMetrics()
	calls := float64(metrics.GetCalls())
	accumulator.stats["calls"].Push(calls)
	accumulator.stats["total_time"].Push(metrics.GetTotalTime())
	if !aggregate.GetMinmaxLifetime() {
		accumulator.stats["min_time"].Push(metrics.GetMinTime())
		accumulator.stats["max_time"].Push(metrics.GetMaxTime())
	}
	if calls > 0 {
		accumulator.stats["mean_time"].Push(metrics.GetTotalTime() / calls)
	} else {
		accumulator.stats["mean_time"].Push(metrics.GetMeanTime())
	}
	if metrics != nil && metrics.StddevTime != nil {
		accumulator.stats["stddev_time"].Push(metrics.GetStddevTime())
	}
	accumulator.stats["rows"].Push(float64(metrics.GetRows()))
	accumulator.stats["shared_blks_hit"].Push(float64(metrics.GetSharedBlksHit()))
	accumulator.stats["shared_blks_read"].Push(float64(metrics.GetSharedBlksRead()))
	accumulator.stats["shared_blks_dirtied"].Push(float64(metrics.GetSharedBlksDirtied()))
	accumulator.stats["shared_blks_written"].Push(float64(metrics.GetSharedBlksWritten()))
	accumulator.stats["local_blks_hit"].Push(float64(metrics.GetLocalBlksHit()))
	accumulator.stats["local_blks_read"].Push(float64(metrics.GetLocalBlksRead()))
	accumulator.stats["local_blks_dirtied"].Push(float64(metrics.GetLocalBlksDirtied()))
	accumulator.stats["local_blks_written"].Push(float64(metrics.GetLocalBlksWritten()))
	accumulator.stats["temp_blks_read"].Push(float64(metrics.GetTempBlksRead()))
	accumulator.stats["temp_blks_written"].Push(float64(metrics.GetTempBlksWritten()))
	accumulator.stats["blk_read_time"].Push(metrics.GetBlkReadTime())
	accumulator.stats["blk_write_time"].Push(metrics.GetBlkWriteTime())
}

func mergeFingerprintStatsForSource(ctx context.Context, tx pgx.Tx, accumulator *fingerprintStatsAccumulator, sourceID uint32, last int64) error {
	if err := ensureFingerprintStatsRows(ctx, tx, accumulator.fingerprintID, sourceID, last); err != nil {
		return err
	}
	existing, err := lockedFingerprintStats(ctx, tx, accumulator.fingerprintID, sourceID)
	if err != nil {
		return err
	}
	if len(existing) != len(fingerprintStatDomains) {
		return fmt.Errorf("fingerprint_stats has %d rows for fingerprint_id %d logical_source_id %d, want %d", len(existing), accumulator.fingerprintID, sourceID, len(fingerprintStatDomains))
	}
	for _, domain := range fingerprintStatDomains {
		next := accumulator.stats[domain]
		if next.RunningStatCount() == 0 {
			continue
		}
		combined := runningstat.RunningStat{}
		combined.Init(next.RunningStatCount(), next.RunningStatMean(), next.RunningStatDeviation())
		combined.Merge(existing[domain])
		if _, err := tx.Exec(ctx, `update rotten.fingerprint_stats
			set last = $1, count = $2, mean = $3, deviation = $4
			where fingerprint_id = $5 and logical_source_id = $6 and type = $7`,
			last, combined.RunningStatCount(), combined.RunningStatMean(), combined.RunningStatDeviation(), accumulator.fingerprintID, sourceID, domain); err != nil {
			return err
		}
	}
	return nil
}

func ensureFingerprintStatsRows(ctx context.Context, tx pgx.Tx, fingerprintID int64, sourceID uint32, last int64) error {
	for _, domain := range fingerprintStatDomains {
		if _, err := tx.Exec(ctx, `insert into rotten.fingerprint_stats
			(fingerprint_id, logical_source_id, type, last, count, mean, deviation)
			values ($1, $2, $3, $4, 0, 0, 0)
			on conflict (fingerprint_id, logical_source_id, type) do nothing`, fingerprintID, sourceID, domain, last); err != nil {
			return err
		}
	}
	return nil
}

func lockedFingerprintStats(ctx context.Context, tx pgx.Tx, fingerprintID int64, sourceID uint32) (map[string]runningstat.RunningStat, error) {
	rows, err := tx.Query(ctx, `select type::text, count, mean, deviation
		from rotten.fingerprint_stats
		where fingerprint_id = $1 and logical_source_id = $2
		order by type
		for update`, fingerprintID, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := map[string]runningstat.RunningStat{}
	for rows.Next() {
		var domain string
		var count int64
		var mean float64
		var deviation float64
		if err := rows.Scan(&domain, &count, &mean, &deviation); err != nil {
			return nil, err
		}
		stat := runningstat.RunningStat{}
		stat.Init(count, mean, deviation)
		existing[domain] = stat
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return existing, nil
}

func optionalID(ids map[string]int64, value string) *int64 {
	if value == "" {
		return nil
	}
	id := ids[value]
	return &id
}

// fingerprintID stores normalized only on first insert, like the
// fingerprint itself. An unset unparsed, from an older worker, is false.
//
// A server from before the unparsed column stores a new worker's fallback
// fingerprints as parsed, so a flagged aggregate for an existing fallback
// fingerprint marks it unparsed. Only fallback fingerprints, which the
// prefix tells apart, are marked, and the flag is never cleared. The update
// runs, and locks the row, only when the flag actually changes.
func fingerprintID(ctx context.Context, tx pgx.Tx, fingerprint, normalized string, unparsed bool) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `insert into rotten.fingerprints(fingerprint, normalized, unparsed)
		values ($1, $2, $3) on conflict (fingerprint) do nothing returning id`, fingerprint, normalized, unparsed).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	var stored bool
	if err := tx.QueryRow(ctx, `select id, unparsed from rotten.fingerprints where fingerprint = $1`, fingerprint).Scan(&id, &stored); err != nil {
		return 0, err
	}
	if unparsed && !stored && strings.HasPrefix(fingerprint, harvestlimits.FallbackFingerprintPrefix) {
		if _, err := tx.Exec(ctx, `update rotten.fingerprints set unparsed = true where id = $1 and not unparsed`, id); err != nil {
			return 0, err
		}
	}
	return id, nil
}

func controllerID(ctx context.Context, tx pgx.Tx, controller string) (*int64, error) {
	if controller == "" {
		return nil, nil
	}
	id, err := lookupControllerID(ctx, tx, controller)
	return &id, err
}

func actionID(ctx context.Context, tx pgx.Tx, action string) (*int64, error) {
	if action == "" {
		return nil, nil
	}
	id, err := lookupActionID(ctx, tx, action)
	return &id, err
}

func jobTagID(ctx context.Context, tx pgx.Tx, jobTag string) (*int64, error) {
	if jobTag == "" {
		return nil, nil
	}
	id, err := lookupJobTagID(ctx, tx, jobTag)
	return &id, err
}

func lookupControllerID(ctx context.Context, tx pgx.Tx, controller string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `insert into rotten.controllers(controller)
		values ($1) on conflict (controller) do nothing returning id`, controller).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	return id, tx.QueryRow(ctx, `select id from rotten.controllers where controller = $1`, controller).Scan(&id)
}

func lookupActionID(ctx context.Context, tx pgx.Tx, action string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `insert into rotten.actions(action)
		values ($1) on conflict (action) do nothing returning id`, action).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	return id, tx.QueryRow(ctx, `select id from rotten.actions where action = $1`, action).Scan(&id)
}

func lookupJobTagID(ctx context.Context, tx pgx.Tx, jobTag string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `insert into rotten.job_tags(job_tag)
		values ($1) on conflict (job_tag) do nothing returning id`, jobTag).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	return id, tx.QueryRow(ctx, `select id from rotten.job_tags where job_tag = $1`, jobTag).Scan(&id)
}
