package ingest

import (
	"context"
	"log/slog"
	"time"
)

// HomeStatsInterval is how often the server refreshes the landing page's
// stats. The page shows how old they are.
const HomeStatsInterval = 5 * time.Minute

// RunHomeStatsRefresher refreshes the landing page's stats once at start,
// then on interval, until ctx ends. Each refresh covers the week before the
// current time. When several servers run, only one refreshes at a time; the
// others skip that round.
func RunHomeStatsRefresher(ctx context.Context, db pruneDB, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	refresh := func() {
		start := time.Now()
		refreshed, err := RefreshHomeStats(ctx, db, start)
		switch {
		case ctx.Err() != nil:
		case err != nil:
			logger.Warn("refresh home stats failed", "err", err)
		case refreshed:
			logger.Info("refreshed home stats", "duration", time.Since(start).String())
		default:
			logger.Info("skipped home stats refresh: another server is refreshing")
		}
	}
	refresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

// RefreshHomeStats recomputes rotten.home_stats and rotten.home_heatmap as of
// asOf through the SECURITY DEFINER function rotten.refresh_home_stats. It
// returns false, without error, when another session is already refreshing.
func RefreshHomeStats(ctx context.Context, db pruneDB, asOf time.Time) (bool, error) {
	var refreshed bool
	err := db.QueryRow(ctx, "select rotten.refresh_home_stats($1)", asOf).Scan(&refreshed)
	return refreshed, err
}
