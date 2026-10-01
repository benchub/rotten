package rpc_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
)

// stubServer records what it got and echoes fixed replies.
type stubServer struct {
	gotInfo  *rottenv1.RegisterRequest
	gotBatch *rottenv1.SubmitHarvestRequest
	gotProto string
}

func (s *stubServer) Register(_ context.Context, req *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	s.gotInfo = req.Msg
	return connect.NewResponse(&rottenv1.RegisterResponse{LogicalSourceId: 7, PhysicalSourceId: 42}), nil
}

func (s *stubServer) SubmitHarvest(_ context.Context, req *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	s.gotBatch = req.Msg
	return connect.NewResponse(&rottenv1.SubmitHarvestResponse{BatchId: req.Msg.GetBatchId(), Status: rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE}), nil
}

func sampleBatch() *rottenv1.SubmitHarvestRequest {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	return &rottenv1.SubmitHarvestRequest{
		BatchId:          "42:1790856000000000:1790856060000000",
		LogicalSourceId:  7,
		PhysicalSourceId: 42,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(start.Add(time.Minute)),
		Aggregates: []*rottenv1.FingerprintAggregate{
			{
				Fingerprint: "02a281c251c3a43d2fe7457dff01f76c5cc523f8c8",
				Normalized:  "select * from t where id = $1",
				Contexts: []*rottenv1.QueryContext{
					{Controller: "users", Action: "show", Count: 3},
					{JobTag: "nightly", Count: 1},
				},
				Metrics: &rottenv1.Metrics{
					Calls: 4, TotalTime: 10.5, MinTime: 0.5, MaxTime: 6, MeanTime: 2.625,
					StddevTime: proto.Float64(2.1), Rows: 4,
					SharedBlksHit: 1, SharedBlksRead: 2, SharedBlksDirtied: 3, SharedBlksWritten: 4,
					LocalBlksHit: 5, LocalBlksRead: 6, LocalBlksDirtied: 7, LocalBlksWritten: 8,
					TempBlksRead: 9, TempBlksWritten: 10, BlkReadTime: 1.25, BlkWriteTime: 2.5,
				},
				MinmaxLifetime: true,
			},
			{
				// Unreliable stddev: absent, which must differ from 0.
				Fingerprint: "ff00",
				Normalized:  "select $1",
				Metrics:     &rottenv1.Metrics{Calls: 1e9, StddevTime: nil},
			},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		h2c  bool
		opts []connect.ClientOption
	}{
		{"connect over HTTP/1.1", false, nil},
		{"connect over h2c", true, nil},
		{"grpc over h2c", true, []connect.ClientOption{connect.WithGRPC()}},
		{"grpc-web over HTTP/1.1", false, []connect.ClientOption{connect.WithGRPCWeb()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubServer{}
			path, handler := rottenv1connect.NewIngestServiceHandler(stub)
			mux := http.NewServeMux()
			mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				stub.gotProto = r.Proto
				handler.ServeHTTP(w, r)
			}))

			srv := httptest.NewUnstartedServer(mux)
			var protocols http.Protocols
			protocols.SetHTTP1(true)
			protocols.SetUnencryptedHTTP2(true)
			srv.Config.Protocols = &protocols
			srv.Start()
			defer srv.Close()

			var cp http.Protocols
			if tc.h2c {
				cp.SetUnencryptedHTTP2(true)
			} else {
				cp.SetHTTP1(true)
			}
			httpClient := &http.Client{Transport: &http.Transport{Protocols: &cp}}
			client := rottenv1connect.NewIngestServiceClient(httpClient, srv.URL, tc.opts...)
			ctx := context.Background()

			info := &rottenv1.RegisterRequest{Project: "p", Environment: "prod", Cluster: "c1", Role: "primary", Fqdn: "db1.example.com", WorkerVersion: proto.String("v1.2.3")}
			reg, err := client.Register(ctx, connect.NewRequest(info))
			if err != nil {
				t.Fatalf("Register: %v", err)
			}
			if !proto.Equal(stub.gotInfo, info) {
				t.Errorf("server got WorkerInfo %v, want %v", stub.gotInfo, info)
			}
			if reg.Msg.GetLogicalSourceId() != 7 || reg.Msg.GetPhysicalSourceId() != 42 {
				t.Errorf("Registration = %v", reg.Msg)
			}

			batch := sampleBatch()
			ack, err := client.SubmitHarvest(ctx, connect.NewRequest(batch))
			if err != nil {
				t.Fatalf("SubmitHarvest: %v", err)
			}
			if !proto.Equal(stub.gotBatch, batch) {
				t.Errorf("server got batch %v, want %v", stub.gotBatch, batch)
			}
			if got := stub.gotBatch.GetAggregates()[1].GetMetrics(); got.StddevTime != nil {
				t.Errorf("absent stddev arrived as %v", *got.StddevTime)
			}
			if got := stub.gotBatch.GetAggregates()[0].GetMetrics().StddevTime; got == nil || *got != 2.1 {
				t.Errorf("stddev = %v, want 2.1", got)
			}
			if ack.Msg.GetBatchId() != batch.GetBatchId() || ack.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE {
				t.Errorf("Ack = %v", ack.Msg)
			}

			wantProto := "HTTP/1.1"
			if tc.h2c {
				wantProto = "HTTP/2.0"
			}
			if stub.gotProto != wantProto {
				t.Errorf("server saw %s, want %s", stub.gotProto, wantProto)
			}
		})
	}
}
