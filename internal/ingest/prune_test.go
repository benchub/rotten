package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

func TestRunPrunerRemovesOldBatches(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	ingestPool, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingestPool.Close)
	key, err := auth.CreateKey(ctx, owner, "prune-worker", "db-prune.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, `
		insert into rotten.ingested_batches(batch_id, key_id, received_at)
		values ('old', $1, now() - interval '31 days'), ('new', $1, now())`, key.ID); err != nil {
		t.Fatal(err)
	}

	pruneCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ingest.RunPruner(pruneCtx, ingestPool, 10*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("pruner did not stop")
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		var left string
		if err := owner.QueryRow(ctx, "select string_agg(batch_id, ',' order by batch_id) from rotten.ingested_batches").Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == "new" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pruner left batches %q, want only new", left)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
