package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
)

type countingStore struct {
	mu      sync.Mutex
	rows    map[int64]Row
	lookups map[int64]int
	err     error
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newCountingStore() *countingStore {
	return &countingStore{rows: map[int64]Row{}, lookups: map[int64]int{}}
}

func (s *countingStore) Lookup(_ context.Context, id int64) (Row, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups[id]++
	if s.err != nil {
		return Row{}, false, s.err
	}
	row, ok := s.rows[id]
	return row, ok, nil
}

func (s *countingStore) Touch(context.Context, int64, time.Time) error { return nil }

func (s *countingStore) Preload(context.Context) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	out := make([]Row, 0, len(s.rows))
	for _, row := range s.rows {
		if !row.Revoked {
			out = append(out, row)
		}
	}
	return out, nil
}

func (s *countingStore) set(row Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[row.ID] = row
}

func (s *countingStore) count(id int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups[id]
}

func (s *countingStore) totalLookups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int
	for _, n := range s.lookups {
		total += n
	}
	return total
}

func newLimitTestAuthenticator(store Store, clk *testClock, burst int) *Authenticator {
	return New(store, Options{
		Now:                    clk.Now,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailedAuthBurst:        burst,
		FailedAuthRefill:       time.Minute,
		GlobalFailedAuthBurst:  1000,
		GlobalFailedAuthRefill: time.Minute,
		FailedAuthMaxClients:   16,
	})
}

func unknownHeader(t *testing.T, id int64) string {
	t.Helper()
	_, secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + FormatKey(id, secret)
}

func TestFailedAuthTokenBucketBoundsDistinctParallelLookups(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	const burst = 5
	a := newLimitTestAuthenticator(store, clk, burst)
	start := make(chan struct{})
	errs := make(chan error, 40)
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			_, err := a.authenticate(ctx, unknownHeader(t, id), "192.0.2.10:54321")
			errs <- err
		}(1000 + int64(i))
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("unknown key error = %v, want Unauthenticated", err)
		}
	}
	if got := store.totalLookups(); got > burst {
		t.Fatalf("parallel unknown lookups = %d, want at most %d", got, burst)
	}
}

func TestGlobalFailedAuthBucketBoundsClientKeyRotation(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := New(store, Options{
		Now:                    clk.Now,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailedAuthBurst:        5,
		FailedAuthRefill:       time.Minute,
		FailedAuthMaxClients:   2,
		GlobalFailedAuthBurst:  7,
		GlobalFailedAuthRefill: time.Second,
	})
	for i := range 40 {
		addr := net.JoinHostPort(net.IPv6loopback.String(), "12345")
		if i > 0 {
			addr = net.JoinHostPort(fmt.Sprintf("2001:db8:%x:%x::1", i, i), "12345")
		}
		if _, err := a.authenticate(ctx, unknownHeader(t, 9000+int64(i)), addr); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("rotating client error = %v, want Unauthenticated", err)
		}
	}
	if got := store.totalLookups(); got > 7 {
		t.Fatalf("rotating client lookups = %d, want at most global burst 7", got)
	}
	clk.Add(3 * time.Second)
	for i := range 10 {
		addr := net.JoinHostPort(fmt.Sprintf("2001:db8:feed:%x::1", i), "12345")
		if _, err := a.authenticate(ctx, unknownHeader(t, 9100+int64(i)), addr); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("rotating client after refill error = %v, want Unauthenticated", err)
		}
	}
	if got := store.totalLookups(); got > 10 {
		t.Fatalf("rotating client lookups after 3s refill = %d, want at most 10", got)
	}
}

func TestFailedAuthTokenBucketAllowsNewKeyAfterRefill(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 2)
	for _, id := range []int64{2000, 2001} {
		if _, err := a.authenticate(ctx, unknownHeader(t, id), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("unknown key error = %v, want Unauthenticated", err)
		}
	}
	goodSecret, goodHash, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	store.set(Row{ID: 3000, Name: "new-key", SecretHash: goodHash, FQDN: "db.example"})
	goodHeader := "Bearer " + FormatKey(3000, goodSecret)
	if _, err := a.authenticate(ctx, goodHeader, "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("new key before refill error = %v, want Unauthenticated", err)
	}
	if got := store.count(3000); got != 0 {
		t.Fatalf("new key lookups before refill = %d, want 0", got)
	}

	clk.Add(time.Minute + time.Nanosecond)
	got, err := a.authenticate(ctx, goodHeader, "192.0.2.10:54321")
	if err != nil {
		t.Fatalf("new key after refill: %v", err)
	}
	if got.ID != 3000 || got.Name != "new-key" {
		t.Fatalf("new key = %+v, want id 3000 name new-key", got)
	}
}

func TestNewKeyDuringAttackWorksImmediatelyFromDifferentIP(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 2)
	for _, id := range []int64{3100, 3101} {
		if _, err := a.authenticate(ctx, unknownHeader(t, id), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("attacker unknown key error = %v, want Unauthenticated", err)
		}
	}
	goodSecret, goodHash, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	store.set(Row{ID: 3102, Name: "new-key", SecretHash: goodHash})
	if _, err := a.authenticate(ctx, "Bearer "+FormatKey(3102, goodSecret), "198.51.100.20:54321"); err != nil {
		t.Fatalf("new key from different IP during attack: %v", err)
	}
}

func TestPreloadLetsConcurrentValidKeysBypassLimiter(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 2)
	headers := make([]string, 8)
	for i := range headers {
		secret, hash, err := NewSecret()
		if err != nil {
			t.Fatal(err)
		}
		id := int64(3200 + i)
		store.set(Row{ID: id, Name: "preloaded", SecretHash: hash})
		headers[i] = "Bearer " + FormatKey(id, secret)
	}
	if err := a.Preload(ctx); err != nil {
		t.Fatal(err)
	}
	before := store.totalLookups()
	start := make(chan struct{})
	errs := make(chan error, len(headers))
	var wg sync.WaitGroup
	for _, header := range headers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := a.authenticate(ctx, header, "192.0.2.10:54321")
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("preloaded key refused: %v", err)
		}
	}
	if got := store.totalLookups(); got != before {
		t.Fatalf("preloaded auth lookups = %d, want unchanged %d", got, before)
	}
}

func TestSuccessfulAuthDoesNotChargeFailedAuthBucket(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 1)
	goodSecret, goodHash, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	store.set(Row{ID: 4000, Name: "good", SecretHash: goodHash})
	if _, err := a.authenticate(ctx, "Bearer "+FormatKey(4000, goodSecret), "192.0.2.10:54321"); err != nil {
		t.Fatalf("good key: %v", err)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 4001), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("unknown key error = %v, want Unauthenticated", err)
	}
	if got := store.count(4001); got != 1 {
		t.Fatalf("unknown lookup after good auth = %d, want 1", got)
	}
}

func TestCachedWrongSecretAndRevokedKeysChargeWithoutBlocking(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 2)
	goodSecret, hash, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	otherSecret, _, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	store.set(Row{ID: 4100, Name: "cached", SecretHash: hash})
	if _, err := a.authenticate(ctx, "Bearer "+FormatKey(4100, goodSecret), "192.0.2.10:54321"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.authenticate(ctx, "Bearer "+FormatKey(4100, otherSecret), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("wrong cached secret error = %v, want Unauthenticated", err)
	}
	store.set(Row{ID: 4101, Name: "revoked", SecretHash: hash, Revoked: true})
	if err := a.Preload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.authenticate(ctx, "Bearer "+FormatKey(4101, goodSecret), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("revoked cached key error = %v, want Unauthenticated", err)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 4102), "192.0.2.10:54321"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("post-cache-failure unknown error = %v, want Unauthenticated", err)
	}
	if got := store.count(4102); got != 0 {
		t.Fatalf("unknown lookup after cached failures = %d, want 0", got)
	}
}

func TestFailedAuthTokenBucketIsPerIPAndRefills(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 1)
	if _, err := a.authenticate(ctx, unknownHeader(t, 5000), "192.0.2.10:1111"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("first IP error = %v, want Unauthenticated", err)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 5001), "192.0.2.10:1111"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("limited first IP error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 1 {
		t.Fatalf("first IP lookups = %d, want 1", got)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 5002), "198.51.100.20:2222"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("second IP error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 2 {
		t.Fatalf("lookups after second IP = %d, want 2", got)
	}

	clk.Add(time.Minute + time.Nanosecond)
	if _, err := a.authenticate(ctx, unknownHeader(t, 5003), "192.0.2.10:1111"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("after refill error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 3 {
		t.Fatalf("lookups after refill = %d, want 3", got)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 5004), "192.0.2.10:1111"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("after refill limited error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 3 {
		t.Fatalf("lookups after spending refill = %d, want 3", got)
	}
}

func TestFailedAuthTokenBucketKeysIPv6ClientsBy64(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := newLimitTestAuthenticator(store, clk, 1)
	if _, err := a.authenticate(ctx, unknownHeader(t, 6000), net.JoinHostPort("2001:db8:abcd:1::1", "1111")); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("first IPv6 error = %v, want Unauthenticated", err)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 6001), net.JoinHostPort("2001:db8:abcd:1::2", "1111")); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("same /64 IPv6 error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 1 {
		t.Fatalf("same IPv6 /64 lookups = %d, want 1", got)
	}
	if _, err := a.authenticate(ctx, unknownHeader(t, 6002), net.JoinHostPort("2001:db8:abcd:2::1", "1111")); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("different /64 IPv6 error = %v, want Unauthenticated", err)
	}
	if got := store.totalLookups(); got != 2 {
		t.Fatalf("different IPv6 /64 lookups = %d, want 2", got)
	}
}

func TestFailedAuthTokenBucketClientMapStaysBounded(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := New(store, Options{
		Now:                    clk.Now,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailedAuthBurst:        1,
		FailedAuthRefill:       time.Minute,
		FailedAuthMaxClients:   2,
		GlobalFailedAuthBurst:  1000,
		GlobalFailedAuthRefill: time.Minute,
	})
	for _, addr := range []string{"192.0.2.1:1", "192.0.2.2:1", "192.0.2.3:1", "192.0.2.4:1"} {
		if _, err := a.authenticate(ctx, unknownHeader(t, 7000), addr); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("%s error = %v, want Unauthenticated", addr, err)
		}
	}
	if got := a.failedAuthClientCount(); got > 2 {
		t.Fatalf("failed auth client buckets = %d, want at most 2", got)
	}
}

func TestInterceptorUsesPeerAddressForLimiter(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	a := New(store, Options{
		Now:                    clk.Now,
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailedAuthBurst:        1,
		FailedAuthRefill:       time.Minute,
		FailedAuthMaxClients:   16,
		GlobalFailedAuthBurst:  1000,
		GlobalFailedAuthRefill: time.Minute,
	})
	path, handler := rottenv1connect.NewIngestServiceHandler(&limitStub{}, connect.WithInterceptors(a.Interceptor()))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := rottenv1connect.NewIngestServiceClient(srv.Client(), srv.URL)
	for i := range 2 {
		req := connect.NewRequest(&rottenv1.RegisterRequest{})
		req.Header().Set("Authorization", unknownHeader(t, 10000+int64(i)))
		if _, err := client.Register(ctx, req); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("interceptor request %d error = %v, want Unauthenticated", i, err)
		}
	}
	if got := store.totalLookups(); got != 1 {
		t.Fatalf("interceptor lookups = %d, want 1", got)
	}
}

type limitStub struct {
	rottenv1connect.UnimplementedIngestServiceHandler
}

func (s *limitStub) Register(context.Context, *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	return connect.NewResponse(&rottenv1.RegisterResponse{}), nil
}

func TestClientKeyFromPeerAddr(t *testing.T) {
	if got := clientKeyFromPeerAddr("203.0.113.5:443"); got != "203.0.113.5" {
		t.Fatalf("IPv4 peer key = %q", got)
	}
	if got := clientKeyFromPeerAddr(net.JoinHostPort("2001:db8::1", "443")); got != "2001:db8::/64" {
		t.Fatalf("IPv6 peer key = %q", got)
	}
	if got := clientKeyFromPeerAddr("not-an-ip:443"); got != "" {
		t.Fatalf("invalid peer key = %q, want empty", got)
	}
}

func TestLookupErrorsStayUnavailableAndAreNotCharged(t *testing.T) {
	ctx := context.Background()
	clk := &testClock{t: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)}
	store := newCountingStore()
	store.err = errors.New("database down")
	a := newLimitTestAuthenticator(store, clk, 1)
	for _, id := range []int64{8000, 8001} {
		if _, err := a.authenticate(ctx, unknownHeader(t, id), "192.0.2.10:1111"); connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("lookup error code = %v, want Unavailable", err)
		}
	}
	if got := store.totalLookups(); got != 2 {
		t.Fatalf("lookups after DB errors = %d, want 2 because errors are refunded", got)
	}
}
