package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

type pruneDB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// RunPruner calls rotten.prune_ingested_batches on interval until ctx ends.
func RunPruner(ctx context.Context, db pruneDB, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pruned, err := PruneIngestedBatches(ctx, db)
			if err != nil {
				logger.Warn("prune ingested batches failed", "err", err)
				continue
			}
			logger.Info("pruned ingested batches", "rows", pruned)
		}
	}
}

// PruneIngestedBatches removes dedupe rows older than the database retention
// cutoff through the SECURITY DEFINER function.
func PruneIngestedBatches(ctx context.Context, db pruneDB) (int64, error) {
	var pruned int64
	err := db.QueryRow(ctx, "select rotten.prune_ingested_batches()").Scan(&pruned)
	return pruned, err
}
