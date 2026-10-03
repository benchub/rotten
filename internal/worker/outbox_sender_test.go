package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/serverclient"
	"github.com/benchub/rotten/internal/state"
)

func TestOutboxSenderDrainsOldestFirstAfterServerRecovers(t *testing.T) {
	ctx := context.Background()
	store := openSenderStore(t)
	for i := 0; i < 3; i++ {
		msg := senderBatch(i)
		if _, err := store.EnqueueHarvest(ctx, msg, msg.GetWindowStart().AsTime()); err != nil {
			t.Fatal(err)
		}
	}
	client := &fakeSubmitter{err: connect.NewError(connect.CodeUnavailable, errors.New("down"))}
	sender := NewOutboxSender(store, client, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err == nil || sent != 0 {
		t.Fatalf("down Drain sent=%d err=%v, want retryable error before sending anything", sent, err)
	}
	client.err = nil
	if sent, err := sender.Drain(ctx); err != nil || sent != 3 {
		t.Fatalf("recovered Drain sent=%d err=%v, want sent=3 nil", sent, err)
	}
	if got := client.batchIDs; fmt.Sprint(got) != "[42:1:2 42:2:3 42:3:4]" {
		t.Fatalf("sent order = %v", got)
	}
	counts, err := store.OutboxCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Queued != 0 {
		t.Fatalf("queued = %d, want 0", counts.Queued)
	}
}

func TestOutboxSenderDropsNonRetryableRejection(t *testing.T) {
	ctx := context.Background()
	store := openSenderStore(t)
	msg := senderBatch(0)
	if _, err := store.EnqueueHarvest(ctx, msg, msg.GetWindowStart().AsTime()); err != nil {
		t.Fatal(err)
	}
	client := &fakeSubmitter{err: connect.NewError(connect.CodeInvalidArgument, errors.New("bad batch"))}
	sender := NewOutboxSender(store, client, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err != nil || sent != 0 {
		t.Fatalf("Drain sent=%d err=%v, want dropped non-retryable without error", sent, err)
	}
	counts, err := store.OutboxCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Queued != 0 || counts.DroppedRejected != 1 {
		t.Fatalf("counts = %+v, want queued=0 dropped_rejected=1", counts)
	}
}

func TestOutboxSenderOperationalErrorsKeepQueue(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := context.Background()
			store := openSenderStore(t)
			for i := 0; i < 2; i++ {
				msg := senderBatch(i)
				if _, err := store.EnqueueHarvest(ctx, msg, msg.GetWindowStart().AsTime()); err != nil {
					t.Fatal(err)
				}
			}
			client := &fakeSubmitter{err: connect.NewError(code, errors.New("try later"))}
			sender := NewOutboxSender(store, client, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
			if sent, err := sender.Drain(ctx); err == nil || sent != 0 {
				t.Fatalf("Drain sent=%d err=%v, want stop with retryable operational error", sent, err)
			}
			counts, err := store.OutboxCounts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if counts.Queued != 2 || counts.DroppedRejected != 0 {
				t.Fatalf("counts = %+v, want queued=2 dropped_rejected=0", counts)
			}
			first, err := store.NextOutboxBatch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if first == nil || first.BatchID != "42:1:2" {
				t.Fatalf("oldest batch = %+v, want 42:1:2", first)
			}
		})
	}
}

func TestOutboxSenderDropsClientOversizeOnly(t *testing.T) {
	ctx := context.Background()
	store := openSenderStore(t)
	msg := senderBatch(0)
	if _, err := store.EnqueueHarvest(ctx, msg, msg.GetWindowStart().AsTime()); err != nil {
		t.Fatal(err)
	}
	client := &fakeSubmitter{err: serverclient.NewHarvestTooLargeError(errors.New("message exceeds client limit"))}
	sender := NewOutboxSender(store, client, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err != nil || sent != 0 {
		t.Fatalf("Drain sent=%d err=%v, want client oversize dropped without error", sent, err)
	}
	counts, err := store.OutboxCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Queued != 0 || counts.DroppedRejected != 1 {
		t.Fatalf("counts = %+v, want queued=0 dropped_rejected=1", counts)
	}
}

func TestOutboxSenderResendsStoredProtoIdentically(t *testing.T) {
	ctx := context.Background()
	store := openSenderStore(t)
	msg := senderBatch(0)
	if _, err := store.EnqueueHarvest(ctx, msg, msg.GetWindowStart().AsTime()); err != nil {
		t.Fatal(err)
	}
	first, err := store.NextOutboxBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), first.Payload...)
	client := &fakeSubmitter{errs: []error{connect.NewError(connect.CodeDeadlineExceeded, errors.New("ack lost"))}, recordOnError: true}
	sender := NewOutboxSender(store, client, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err == nil || sent != 0 {
		t.Fatalf("first Drain sent=%d err=%v, want retryable error after server saw bytes", sent, err)
	}
	if sent, err := sender.Drain(ctx); err != nil || sent != 1 {
		t.Fatalf("second Drain sent=%d err=%v", sent, err)
	}
	if len(client.payloads) != 2 {
		t.Fatalf("payload count = %d, want 2", len(client.payloads))
	}
	for i, payload := range client.payloads {
		if string(payload) != string(want) {
			t.Fatalf("payload %d was not byte-identical to stored deterministic proto", i)
		}
	}

}

func TestOutboxSenderCrashAfterServerCommitBeforeAckDedupesOnRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(".test-artifacts", "outbox-crash-"+sanitizeName(t.Name()))
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	store, err := state.Open(dir, state.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	msg := senderBatch(0)
	if _, err := store.SaveSnapshotAndEnqueue(ctx, pgssSnapshotForSender(), now, msg); err != nil {
		t.Fatal(err)
	}
	server := &dedupingSubmitter{loseAckFor: map[string]bool{msg.GetBatchId(): true}}
	sender := NewOutboxSender(store, server, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err == nil || sent != 0 {
		t.Fatalf("first Drain sent=%d err=%v, want lost ack error", sent, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = state.Open(dir, state.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	sender = NewOutboxSender(store, server, slog.New(slog.NewTextHandler(testDiscard{}, nil)))
	if sent, err := sender.Drain(ctx); err != nil || sent != 1 {
		t.Fatalf("second Drain sent=%d err=%v, want duplicate ack", sent, err)
	}
	if got := server.commits[msg.GetBatchId()]; got != 1 {
		t.Fatalf("server commits for %s = %d, want 1", msg.GetBatchId(), got)
	}
	counts, err := store.OutboxCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Queued != 0 {
		t.Fatalf("queued = %d, want 0", counts.Queued)
	}
}

func openSenderStore(t *testing.T) *state.Store {
	t.Helper()
	dir := filepath.Join(".test-artifacts", "outbox-sender-"+sanitizeName(t.Name()))
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	store, err := state.Open(dir, state.Options{Now: func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func senderBatch(i int) *rottenv1.SubmitHarvestRequest {
	start := time.UnixMicro(int64(i + 1)).UTC()
	end := time.UnixMicro(int64(i + 2)).UTC()
	return &rottenv1.SubmitHarvestRequest{
		BatchId:          fmt.Sprintf("42:%d:%d", i+1, i+2),
		LogicalSourceId:  7,
		PhysicalSourceId: 42,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates: []*rottenv1.FingerprintAggregate{{
			Fingerprint: fmt.Sprintf("%040d", i+1),
			Normalized:  "select $1",
			Contexts:    []*rottenv1.QueryContext{{Controller: "c", Action: "a", JobTag: "j", Count: 1}},
			Metrics:     &rottenv1.Metrics{Calls: 1, TotalTime: 1, MinTime: 1, MaxTime: 1, MeanTime: 1},
		}},
	}
}

func pgssSnapshotForSender() pgss.Snapshot {
	return pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{}}
}

func sanitizeName(s string) string {
	return regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(s, "_")
}

type testDiscard struct{}

func (testDiscard) Write(p []byte) (int, error) { return len(p), nil }

type fakeSubmitter struct {
	err           error
	errs          []error
	recordOnError bool
	batchIDs      []string
	payloads      [][]byte
}

func (f *fakeSubmitter) SubmitHarvest(_ context.Context, msg *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestResponse, serverclient.ClipCounts, error) {
	submitErr := f.nextErr()
	if submitErr != nil && !f.recordOnError {
		return nil, serverclient.ClipCounts{}, submitErr
	}
	f.batchIDs = append(f.batchIDs, msg.GetBatchId())
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return nil, serverclient.ClipCounts{}, err
	}
	f.payloads = append(f.payloads, payload)
	if submitErr != nil {
		return nil, serverclient.ClipCounts{}, submitErr
	}
	return &rottenv1.SubmitHarvestResponse{BatchId: msg.GetBatchId(), Status: rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED}, serverclient.ClipCounts{}, nil
}

func (f *fakeSubmitter) nextErr() error {
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return f.err
}

type dedupingSubmitter struct {
	loseAckFor map[string]bool
	commits    map[string]int
}

func (d *dedupingSubmitter) SubmitHarvest(_ context.Context, msg *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestResponse, serverclient.ClipCounts, error) {
	if d.commits == nil {
		d.commits = map[string]int{}
	}
	status := rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE
	if d.commits[msg.GetBatchId()] == 0 {
		status = rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED
		d.commits[msg.GetBatchId()] = 1
	}
	if d.loseAckFor[msg.GetBatchId()] {
		delete(d.loseAckFor, msg.GetBatchId())
		return nil, serverclient.ClipCounts{}, connect.NewError(connect.CodeDeadlineExceeded, errors.New("ack lost"))
	}
	return &rottenv1.SubmitHarvestResponse{BatchId: msg.GetBatchId(), Status: status}, serverclient.ClipCounts{}, nil
}
