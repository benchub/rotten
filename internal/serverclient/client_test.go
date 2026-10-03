package serverclient

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/harvestlimits"
)

func TestServerClientDoesNotDependOnIngestOrPGX(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/benchub/rotten/internal/ingest" || strings.HasPrefix(dep, "github.com/jackc/pgx/") || dep == "github.com/jackc/pgx/v5" {
			t.Fatalf("serverclient must not depend on %s; deps:\n%s", dep, out)
		}
	}
}

func TestRequestsReachServerWithBearerHeader(t *testing.T) {
	svc := &recordingService{}
	srv := startTLSServer(t, svc)
	passFile := writeTestFile(t, "pass.key", "  rotten_test_secret\n")

	client, err := New(Config{
		ServerURL:   srv.url,
		PassKeyFile: passFile,
		CAFile:      srv.caFile,
		Logger:      slog.New(slog.NewTextHandler(discardWriter{}, nil)),
		Backoff:     fixedBackoff(0),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := client.Register(context.Background(), &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "primary", Fqdn: "db.example.com"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := svc.authorization(0); got != "Bearer rotten_test_secret" {
		t.Fatalf("Authorization = %q, want bearer key", got)
	}
	if got := svc.proto(0); got != "HTTP/2.0" {
		t.Fatalf("request protocol = %s, want HTTP/2.0", got)
	}
}

func TestRetryPolicy(t *testing.T) {
	cases := []struct {
		name     string
		errs     []error
		wantCode connect.Code
		wantHits int
	}{
		{
			name:     "retries unavailable",
			errs:     []error{connect.NewError(connect.CodeUnavailable, errors.New("try again"))},
			wantHits: 2,
		},
		{
			name:     "does not retry unauthenticated",
			errs:     []error{connect.NewError(connect.CodeUnauthenticated, errors.New("bad key"))},
			wantCode: connect.CodeUnauthenticated,
			wantHits: 1,
		},
		{
			name:     "does not retry invalid argument",
			errs:     []error{connect.NewError(connect.CodeInvalidArgument, errors.New("bad batch"))},
			wantCode: connect.CodeInvalidArgument,
			wantHits: 1,
		},
		{
			name:     "retries server resource exhausted",
			errs:     []error{connect.NewError(connect.CodeResourceExhausted, errors.New("rate limited"))},
			wantHits: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{registerErrs: tc.errs}
			srv := startTLSServer(t, svc)
			client := newTestClient(t, srv)

			_, err := client.Register(context.Background(), &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "primary", Fqdn: "db.example.com"})
			if tc.wantCode == 0 {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
			} else if connect.CodeOf(err) != tc.wantCode {
				t.Fatalf("Register code = %v, want %v; err=%v", connect.CodeOf(err), tc.wantCode, err)
			}
			if got := svc.hits(); got != tc.wantHits {
				t.Fatalf("attempts = %d, want %d", got, tc.wantHits)
			}
		})
	}
}

func TestReconnectsAfterServerRestart(t *testing.T) {
	ca := newTestCA(t)
	cert, key := ca.serverPair(t)
	addr := reserveAddr(t)
	svc1 := &recordingService{}
	stop1 := serveTLSOnAddr(t, addr, cert, key, svc1)
	caFile := writeTestFile(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}))
	passFile := writeTestFile(t, "pass.key", "rotten_reconnect_secret\n")
	client, err := New(Config{
		ServerURL:   "https://" + addr,
		PassKeyFile: passFile,
		CAFile:      caFile,
		Backoff:     fixedBackoff(0),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := client.Register(context.Background(), &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "primary", Fqdn: "db.example.com"}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	stop1()

	svc2 := &recordingService{}
	stop2 := serveTLSOnAddr(t, addr, cert, key, svc2)
	defer stop2()
	if _, err := client.Register(context.Background(), &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "primary", Fqdn: "db.example.com"}); err != nil {
		t.Fatalf("second Register after restart: %v", err)
	}
	if got := svc2.hits(); got != 1 {
		t.Fatalf("new server hits = %d, want 1", got)
	}
}

func TestSubmitHarvestTruncatesNormalizedAtUTF8Boundary(t *testing.T) {
	svc := &recordingService{validateHarvest: true}
	srv := startTLSServer(t, svc)
	client := newTestClient(t, srv)
	batch := sampleBatch()
	prefix := strings.Repeat("a", harvestlimits.MaxNormalizedBytes-1)
	batch.Aggregates[0].Normalized = prefix + "€" + "tail"

	_, clips, err := client.SubmitHarvest(context.Background(), batch)
	if err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	got := svc.batch().GetAggregates()[0].GetNormalized()
	if len(got) > harvestlimits.MaxNormalizedBytes {
		t.Fatalf("normalized length = %d, want <= %d", len(got), harvestlimits.MaxNormalizedBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("normalized is not valid UTF-8: %q", got)
	}
	if got != prefix {
		t.Fatalf("normalized cut = %q, want prefix before multibyte char", got[len(got)-min(len(got), 8):])
	}
	if clips.Normalized != 1 {
		t.Fatalf("normalized clips = %d, want 1", clips.Normalized)
	}
}

func TestSubmitHarvestPreflightRejectsPredictableInvalidBatches(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*rottenv1.SubmitHarvestRequest)
	}{
		{"missing logical source", func(b *rottenv1.SubmitHarvestRequest) { b.LogicalSourceId = 0 }},
		{"missing physical source", func(b *rottenv1.SubmitHarvestRequest) { b.PhysicalSourceId = 0 }},
		{"bad batch id", func(b *rottenv1.SubmitHarvestRequest) { b.BatchId = "not the derived id" }},
		{"missing timestamp", func(b *rottenv1.SubmitHarvestRequest) { b.WindowStart = nil }},
		{"end before start", func(b *rottenv1.SubmitHarvestRequest) { b.WindowEnd = b.GetWindowStart() }},
		{"window too long", func(b *rottenv1.SubmitHarvestRequest) {
			start := b.GetWindowStart().AsTime()
			end := start.Add(harvestlimits.MaxHarvestWindowDuration + time.Second)
			b.WindowEnd = timestamppb.New(end)
			b.BatchId = fmt.Sprintf("%d:%d:%d", b.GetPhysicalSourceId(), start.UnixMicro(), end.UnixMicro())
		}},
		{"duplicate fingerprints", func(b *rottenv1.SubmitHarvestRequest) {
			dup := proto.Clone(b.GetAggregates()[0]).(*rottenv1.FingerprintAggregate)
			b.Aggregates = append(b.Aggregates, dup)
		}},
		{"negative metric", func(b *rottenv1.SubmitHarvestRequest) { b.GetAggregates()[0].GetMetrics().TotalTime = -1 }},
		{"nan metric", func(b *rottenv1.SubmitHarvestRequest) { b.GetAggregates()[0].GetMetrics().TotalTime = math.NaN() }},
		{"infinite metric", func(b *rottenv1.SubmitHarvestRequest) { b.GetAggregates()[0].GetMetrics().TotalTime = math.Inf(1) }},
		{"too large metric", func(b *rottenv1.SubmitHarvestRequest) {
			b.GetAggregates()[0].GetMetrics().TotalTime = harvestlimits.MaxFloatMetricValue + 1
		}},
		{"too many calls", func(b *rottenv1.SubmitHarvestRequest) {
			b.GetAggregates()[0].GetMetrics().Calls = harvestlimits.MaxContextCount + 1
		}},
		{"zero context count", func(b *rottenv1.SubmitHarvestRequest) { b.GetAggregates()[0].GetContexts()[0].Count = 0 }},
		{"too large context count", func(b *rottenv1.SubmitHarvestRequest) {
			b.GetAggregates()[0].GetContexts()[0].Count = harvestlimits.MaxContextCount + 1
		}},
		{"duplicate contexts", func(b *rottenv1.SubmitHarvestRequest) {
			dup := proto.Clone(b.GetAggregates()[0].GetContexts()[0]).(*rottenv1.QueryContext)
			b.GetAggregates()[0].Contexts = append(b.GetAggregates()[0].GetContexts(), dup)
		}},
		{"context counts exceed calls", func(b *rottenv1.SubmitHarvestRequest) {
			b.GetAggregates()[0].GetMetrics().Calls = 1
			b.GetAggregates()[0].GetContexts()[0].Count = 2
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{}
			srv := startTLSServer(t, svc)
			client := newTestClient(t, srv)
			batch := sampleBatch()
			tc.mutate(batch)

			_, _, err := client.SubmitHarvest(context.Background(), batch)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("SubmitHarvest code = %v, want InvalidArgument; err=%v", connect.CodeOf(err), err)
			}
			if got := svc.hits(); got != 0 {
				t.Fatalf("server hits = %d, want 0", got)
			}
		})
	}
}

func TestSubmitHarvestRepairsInvalidUTF8BeforeClipping(t *testing.T) {
	cases := []struct {
		name       string
		normalized string
		want       string
	}{
		{
			name:       "bad byte at start",
			normalized: "\xffselect 1",
			want:       "\uFFFDselect 1",
		},
		{
			name:       "bad byte under limit",
			normalized: "select \xff 1",
			want:       "select \uFFFD 1",
		},
		{
			name:       "bad byte near cut",
			normalized: strings.Repeat("a", harvestlimits.MaxNormalizedBytes-2) + "\xff€tail",
			want:       strings.Repeat("a", harvestlimits.MaxNormalizedBytes-2),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recordingService{validateHarvest: true}
			srv := startTLSServer(t, svc)
			client := newTestClient(t, srv)
			batch := sampleBatch()
			batch.Aggregates[0].Normalized = tc.normalized

			_, clips, err := client.SubmitHarvest(context.Background(), batch)
			if err != nil {
				t.Fatalf("SubmitHarvest: %v", err)
			}
			if got := svc.batch().GetAggregates()[0].GetNormalized(); got != tc.want {
				t.Fatalf("normalized = %q, want %q", got, tc.want)
			}
			if clips.Normalized != 1 {
				t.Fatalf("normalized clips = %d, want 1", clips.Normalized)
			}
		})
	}
}

type recordingService struct {
	mu              sync.Mutex
	headers         []string
	protos          []string
	httpProtos      []string
	registerErrs    []error
	gotBatch        *rottenv1.SubmitHarvestRequest
	validateHarvest bool
}

func (s *recordingService) Register(_ context.Context, req *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headers = append(s.headers, req.Header().Get("Authorization"))
	s.protos = append(s.protos, req.Peer().Protocol)
	if len(s.registerErrs) > 0 {
		err := s.registerErrs[0]
		s.registerErrs = s.registerErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&rottenv1.RegisterResponse{LogicalSourceId: 7, PhysicalSourceId: 42}), nil
}

func (s *recordingService) SubmitHarvest(_ context.Context, req *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.validateHarvest {
		if _, _, err := harvestlimits.ValidateHarvest(req.Msg, time.Date(2026, 10, 1, 13, 0, 0, 0, time.UTC), harvestlimits.CheckFutureSkew); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	s.headers = append(s.headers, req.Header().Get("Authorization"))
	s.protos = append(s.protos, req.Peer().Protocol)
	s.gotBatch = proto.Clone(req.Msg).(*rottenv1.SubmitHarvestRequest)
	return connect.NewResponse(&rottenv1.SubmitHarvestResponse{BatchId: req.Msg.GetBatchId(), Status: rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED}), nil
}

func (s *recordingService) authorization(i int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers[i]
}

func (s *recordingService) proto(i int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.httpProtos[i]
}

func (s *recordingService) hits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.headers)
}

func (s *recordingService) batch() *rottenv1.SubmitHarvestRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.gotBatch).(*rottenv1.SubmitHarvestRequest)
}

func (s *recordingService) recordHTTPProto(proto string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpProtos = append(s.httpProtos, proto)
}

type testServer struct {
	url    string
	caFile string
	stop   func()
}

func startTLSServer(t *testing.T, svc *recordingService) testServer {
	t.Helper()
	ca := newTestCA(t)
	cert, key := ca.serverPair(t)
	addr := reserveAddr(t)
	stop := serveTLSOnAddr(t, addr, cert, key, svc)
	caFile := writeTestFile(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}))
	t.Cleanup(stop)
	return testServer{url: "https://" + addr, caFile: caFile, stop: stop}
}

func serveTLSOnAddr(t *testing.T, addr string, certPEM []byte, keyPEM []byte, svc *recordingService) func() {
	t.Helper()
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("parse server key pair: %v", err)
	}
	path, handler := rottenv1connect.NewIngestServiceHandler(svc)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svc.recordHTTPProto(r.Proto)
		handler.ServeHTTP(w, r)
	}))
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{pair},
			NextProtos:   []string{"h2", "http/1.1"},
		},
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := srv.ServeTLS(ln, "", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("ServeTLS: %v", err)
		}
	}()
	return func() {
		srv.Close()
		<-done
	}
}

type testCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return testCA{cert: cert, key: key}
}

func (ca testCA) serverPair(t *testing.T) ([]byte, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func reserveAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

func newTestClient(t *testing.T, srv testServer) *Client {
	t.Helper()
	passFile := writeTestFile(t, "pass.key", "rotten_test_secret\n")
	client, err := New(Config{
		ServerURL:   srv.url,
		PassKeyFile: passFile,
		CAFile:      srv.caFile,
		Logger:      slog.New(slog.NewTextHandler(discardWriter{}, nil)),
		Backoff:     fixedBackoff(0),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func writeTestFile(t *testing.T, name string, contents any) string {
	t.Helper()
	dir := filepath.Join(".test-artifacts", sanitize(t.Name()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir test workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, name)
	var data []byte
	switch v := contents.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		t.Fatalf("unsupported test file contents %T", contents)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func sanitize(s string) string {
	re := regexp.MustCompile(`[^A-Za-z0-9_.-]+`)
	return re.ReplaceAllString(s, "_")
}

func sampleBatch() *rottenv1.SubmitHarvestRequest {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	return &rottenv1.SubmitHarvestRequest{
		BatchId:          fmt.Sprintf("%d:%d:%d", 42, start.UnixMicro(), end.UnixMicro()),
		LogicalSourceId:  7,
		PhysicalSourceId: 42,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates: []*rottenv1.FingerprintAggregate{{
			Fingerprint: "02a281c251c3a43d2fe7457dff01f76c5cc523f8c8",
			Normalized:  "select * from t where id = $1",
			Contexts: []*rottenv1.QueryContext{{
				Controller: "users",
				Action:     "show",
				JobTag:     "nightly",
				Count:      1,
			}},
			Metrics: &rottenv1.Metrics{Calls: 1, TotalTime: 2, MinTime: 2, MaxTime: 2, MeanTime: 2},
		}},
	}
}

type fixedBackoff time.Duration

func (b fixedBackoff) Next(int) time.Duration { return time.Duration(b) }

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
