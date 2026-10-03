package worker

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/serverclient"
	"github.com/benchub/rotten/internal/state"
)

// HarvestSubmitter is implemented by *serverclient.Client.
type HarvestSubmitter interface {
	SubmitHarvest(context.Context, *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestResponse, serverclient.ClipCounts, error)
}

// OutboxStore is the durable queue API used by OutboxSender.
type OutboxStore interface {
	NextOutboxBatch(context.Context) (*state.OutboxBatch, error)
	DeleteOutboxBatch(context.Context, int64) error
	DropRejectedOutboxBatch(context.Context, int64) error
}

// OutboxSender drains durable harvest batches oldest first. It never sends a
// newer batch while an older one is retrying, because the server rejects
// overlapping windows. InvalidArgument and FailedPrecondition are
// non-retryable: the sender logs, counts, and drops those batches so one bad
// window cannot block the queue forever. Auth errors, source permission errors,
// and server ResourceExhausted stop the drain and keep the batch; only a
// client-side permanent oversize harvest is dropped for ResourceExhausted.
type OutboxSender struct {
	store  OutboxStore
	client HarvestSubmitter
	logger *slog.Logger
}

func NewOutboxSender(store OutboxStore, client HarvestSubmitter, logger *slog.Logger) *OutboxSender {
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxSender{store: store, client: client, logger: logger}
}

// Drain sends queued batches until the outbox is empty, ctx ends, or the
// oldest batch hits a retryable failure. It returns the number acknowledged.
func (s *OutboxSender) Drain(ctx context.Context) (int, error) {
	sent := 0
	for {
		batch, err := s.store.NextOutboxBatch(ctx)
		if err != nil {
			return sent, err
		}
		if batch == nil {
			return sent, nil
		}
		var msg rottenv1.SubmitHarvestRequest
		if err := proto.Unmarshal(batch.Payload, &msg); err != nil {
			s.logger.Error("dropping corrupt outbox harvest", "batch_id", batch.BatchID, "err", err)
			if dropErr := s.store.DropRejectedOutboxBatch(ctx, batch.ID); dropErr != nil {
				return sent, dropErr
			}
			continue
		}
		if _, _, err := s.client.SubmitHarvest(ctx, &msg); err != nil {
			if isNonRetryableSubmit(err) {
				s.logger.Warn("dropping non-retryable outbox harvest", "batch_id", batch.BatchID, "code", connect.CodeOf(err), "err", err)
				if dropErr := s.store.DropRejectedOutboxBatch(ctx, batch.ID); dropErr != nil {
					return sent, dropErr
				}
				continue
			}
			s.logger.Warn("outbox harvest retrying later", "batch_id", batch.BatchID, "err", err)
			return sent, err
		}
		if err := s.store.DeleteOutboxBatch(ctx, batch.ID); err != nil {
			return sent, err
		}
		sent++
	}
}

func isNonRetryableSubmit(err error) bool {
	if err == nil {
		return false
	}
	if serverclient.IsHarvestTooLarge(err) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument,
		connect.CodeFailedPrecondition,
		connect.CodeAlreadyExists:
		return true
	default:
		return false
	}
}
