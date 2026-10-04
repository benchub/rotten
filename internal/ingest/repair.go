package ingest

import (
	"context"
	"log/slog"
	"time"
)

// contextRepairBatch is how many events each repair_context_utilization call
// fills in, so no single statement holds many row locks.
const contextRepairBatch = 1000

// RunContextRepair fills in context rows a server from before migration 0011
// wrote, once at startup and then on interval, until ctx ends.
func RunContextRepair(ctx context.Context, db pruneDB, interval time.Duration, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	repair := func() {
		repaired, err := RepairContextUtilization(ctx, db, contextRepairBatch)
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("repair context utilization failed", "rows", repaired, "err", err)
			}
			return
		}
		logger.Info("repaired context utilization", "rows", repaired)
	}
	repair()
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			repair()
		}
	}
}

// RepairContextUtilization calls rotten.repair_context_utilization, batchEvents
// events at a time, until a call repairs nothing. It returns the rows repaired.
func RepairContextUtilization(ctx context.Context, db pruneDB, batchEvents int) (int64, error) {
	var total int64
	for {
		var n int64
		if err := db.QueryRow(ctx, "select rotten.repair_context_utilization($1)", batchEvents).Scan(&n); err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}
