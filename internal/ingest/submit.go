package ingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
)

// SubmitHarvest records one worker observation window. Duplicate batch IDs
// are acknowledged without writing rows a second time.
func (h *Handler) SubmitHarvest(ctx context.Context, req *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	key, ok := auth.FromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing pass key"))
	}
	msg := req.Msg
	windowStart, windowEnd, err := validateHarvestEnvelope(msg)
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

func validateHarvestEnvelope(msg *rottenv1.SubmitHarvestRequest) (time.Time, time.Time, error) {
	if msg.GetLogicalSourceId() == 0 {
		return time.Time{}, time.Time{}, errors.New("logical_source_id is empty")
	}
	if msg.GetPhysicalSourceId() == 0 {
		return time.Time{}, time.Time{}, errors.New("physical_source_id is empty")
	}
	if msg.GetBatchId() == "" {
		return time.Time{}, time.Time{}, errors.New("batch_id is empty")
	}
	if msg.GetWindowStart() == nil || msg.GetWindowEnd() == nil {
		return time.Time{}, time.Time{}, errors.New("window timestamps are required")
	}
	if err := msg.GetWindowStart().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window_start is invalid: %w", err)
	}
	if err := msg.GetWindowEnd().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window_end is invalid: %w", err)
	}
	start := msg.GetWindowStart().AsTime()
	end := msg.GetWindowEnd().AsTime()
	if !end.After(start) {
		return time.Time{}, time.Time{}, errors.New("window_end must be after window_start")
	}
	wantBatch := fmt.Sprintf("%d:%d:%d", msg.GetPhysicalSourceId(), start.UnixMicro(), end.UnixMicro())
	if msg.GetBatchId() != wantBatch {
		return time.Time{}, time.Time{}, errors.New("batch_id does not match source and window")
	}
	for _, aggregate := range msg.GetAggregates() {
		if aggregate.GetFingerprint() == "" || aggregate.GetNormalized() == "" || aggregate.GetMetrics() == nil {
			return time.Time{}, time.Time{}, errors.New("aggregate is missing required fields")
		}
		if aggregate.GetMetrics().GetCalls() > math.MaxInt32 {
			return time.Time{}, time.Time{}, errors.New("aggregate calls exceed event storage range")
		}
		for _, qc := range aggregate.GetContexts() {
			if qc.GetCount() == 0 || qc.GetCount() > math.MaxInt32 {
				return time.Time{}, time.Time{}, errors.New("context count is out of range")
			}
		}
	}
	return start, end, nil
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
	var fingerprints, controllers, actions, jobTags []string
	for _, aggregate := range aggregates {
		fp := aggregate.GetFingerprint()
		if _, ok := normalized[fp]; !ok {
			normalized[fp] = aggregate.GetNormalized()
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
		id, err := fingerprintID(ctx, tx, fingerprint, normalized[fingerprint])
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
	for _, qc := range aggregate.GetContexts() {
		controllerID := optionalID(ids.controllers, qc.GetController())
		actionID := optionalID(ids.actions, qc.GetAction())
		jobTagID := optionalID(ids.jobTags, qc.GetJobTag())
		if _, err := tx.Exec(ctx, `insert into rotten.event_context
			(event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
			values ($1, $2, $3, $4, $5, $6, $7)`, eventID, start, end, controllerID, actionID, jobTagID, qc.GetCount()); err != nil {
			return err
		}
	}
	return nil
}

func optionalID(ids map[string]int64, value string) *int64 {
	if value == "" {
		return nil
	}
	id := ids[value]
	return &id
}

func fingerprintID(ctx context.Context, tx pgx.Tx, fingerprint, normalized string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `insert into rotten.fingerprints(fingerprint, normalized)
		values ($1, $2) on conflict (fingerprint) do nothing returning id`, fingerprint, normalized).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	return id, tx.QueryRow(ctx, `select id from rotten.fingerprints where fingerprint = $1`, fingerprint).Scan(&id)
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
