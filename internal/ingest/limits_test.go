package ingest

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
)

func TestWorstCaseHarvestFitsTransportCapAndValidates(t *testing.T) {
	batch := worstCaseHarvestBatch(t)
	size := proto.Size(batch)
	headroom := MaxIngestMessageBytes / 4
	if size >= MaxIngestMessageBytes-headroom {
		t.Fatalf("worst-case batch proto size = %d, want below transport cap %d with %d bytes headroom", size, MaxIngestMessageBytes, headroom)
	}
	handler := NewHandler(nil, Options{Now: func() time.Time { return time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC) }})
	if _, _, err := handler.validateHarvestEnvelope(batch); err != nil {
		t.Fatalf("validate worst-case batch: %v", err)
	}

	stub := &limitStub{}
	path, transportHandler := rottenv1connect.NewIngestServiceHandler(stub, connect.WithReadMaxBytes(MaxIngestMessageBytes))
	mux := http.NewServeMux()
	mux.Handle(path, transportHandler)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := rottenv1connect.NewIngestServiceClient(srv.Client(), srv.URL)
	if _, err := client.SubmitHarvest(context.Background(), connect.NewRequest(batch)); err != nil {
		t.Fatalf("transport rejected worst-case batch: %v", err)
	}
	if stub.gotBatch == nil {
		t.Fatal("transport did not deliver worst-case batch to handler")
	}
}

func TestHarvestBoundaryLimitsValidate(t *testing.T) {
	batch := worstCaseHarvestBatch(t)
	handler := NewHandler(nil, Options{Now: func() time.Time { return time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC) }})
	if _, _, err := handler.validateHarvestEnvelope(batch); err != nil {
		t.Fatalf("validate boundary batch: %v", err)
	}
}

type limitStub struct {
	gotBatch *rottenv1.SubmitHarvestRequest
}

func (s *limitStub) Register(context.Context, *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	return connect.NewResponse(&rottenv1.RegisterResponse{}), nil
}

func (s *limitStub) SubmitHarvest(_ context.Context, req *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	s.gotBatch = req.Msg
	return connect.NewResponse(&rottenv1.SubmitHarvestResponse{BatchId: req.Msg.GetBatchId(), Status: rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED}), nil
}

func worstCaseHarvestBatch(t *testing.T) *rottenv1.SubmitHarvestRequest {
	t.Helper()
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(MaxHarvestWindowDuration)
	msg := &rottenv1.SubmitHarvestRequest{
		LogicalSourceId:  7,
		PhysicalSourceId: 42,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates:       make([]*rottenv1.FingerprintAggregate, 0, MaxHarvestAggregates),
	}
	msg.BatchId = fmt.Sprintf("%d:%d:%d", msg.GetPhysicalSourceId(), start.UnixMicro(), end.UnixMicro())
	contextsLeft := MaxHarvestContexts
	for i := range MaxHarvestAggregates {
		contexts := 0
		if contextsLeft > 0 {
			contexts = 1
			contextsLeft--
		}
		aggregate := &rottenv1.FingerprintAggregate{
			Fingerprint: fmt.Sprintf("%0*d", MaxFingerprintBytes, i),
			Normalized:  strings.Repeat("n", MaxNormalizedBytes),
			Contexts:    make([]*rottenv1.QueryContext, 0, contexts),
			Metrics: &rottenv1.Metrics{
				Calls:             MaxContextCount,
				TotalTime:         MaxFloatMetricValue,
				MinTime:           MaxFloatMetricValue,
				MaxTime:           MaxFloatMetricValue,
				MeanTime:          MaxFloatMetricValue,
				StddevTime:        proto.Float64(MaxFloatMetricValue),
				Rows:              math.MaxUint64,
				SharedBlksHit:     math.MaxUint64,
				SharedBlksRead:    math.MaxUint64,
				SharedBlksDirtied: math.MaxUint64,
				SharedBlksWritten: math.MaxUint64,
				LocalBlksHit:      math.MaxUint64,
				LocalBlksRead:     math.MaxUint64,
				LocalBlksDirtied:  math.MaxUint64,
				LocalBlksWritten:  math.MaxUint64,
				TempBlksRead:      math.MaxUint64,
				TempBlksWritten:   math.MaxUint64,
				BlkReadTime:       MaxFloatMetricValue,
				BlkWriteTime:      MaxFloatMetricValue,
			},
			MinmaxLifetime: true,
		}
		for j := range contexts {
			aggregate.Contexts = append(aggregate.Contexts, &rottenv1.QueryContext{
				Controller: fmt.Sprintf("%0*d", MaxContextStringBytes, i),
				Action:     fmt.Sprintf("%0*d", MaxContextStringBytes, j),
				JobTag:     strings.Repeat("j", MaxContextStringBytes),
				Count:      MaxContextCount,
			})
		}
		msg.Aggregates = append(msg.Aggregates, aggregate)
	}
	return msg
}
