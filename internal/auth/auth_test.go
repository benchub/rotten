package auth_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/testdb"
)

// stub records the key it saw in the request context.
type stub struct {
	rottenv1connect.UnimplementedIngestServiceHandler
	mu  sync.Mutex
	got []auth.Key
}

func (s *stub) Register(ctx context.Context, _ *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	k, ok := auth.FromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, errors.New("no key in context"))
	}
	s.mu.Lock()
	s.got = append(s.got, k)
	s.mu.Unlock()
	return connect.NewResponse(&rottenv1.RegisterResponse{}), nil
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type fixture struct {
	db     *testdb.DB
	owner  *pgxpool.Pool
	clk    *clock
	logs   *bytes.Buffer
	stub   *stub
	client rottenv1connect.IngestServiceClient
}

type syncBuf struct {
	mu sync.Mutex
	b  *bytes.Buffer
}

func (w *syncBuf) Write(p []byte) (int, error) { w.mu.Lock(); defer w.mu.Unlock(); return w.b.Write(p) }

func setup(t *testing.T) *fixture {
	t.Helper()
	db := testdb.StartRotten(t)
	ctx := context.Background()
	ingest, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingest.Close)
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)

	f := &fixture{db: db, owner: owner, clk: &clock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}, logs: &bytes.Buffer{}, stub: &stub{}}
	logger := slog.New(slog.NewTextHandler(&syncBuf{b: f.logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := auth.New(auth.NewPGStore(ingest), auth.Options{TTL: 30 * time.Second, Now: f.clk.Now, Logger: logger, FailedAuthBurst: 10000, GlobalFailedAuthBurst: 10000})
	path, h := rottenv1connect.NewIngestServiceHandler(f.stub, connect.WithInterceptors(a.Interceptor()))
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	f.client = rottenv1connect.NewIngestServiceClient(srv.Client(), srv.URL)
	return f
}

func (f *fixture) call(token string) error {
	req := connect.NewRequest(&rottenv1.RegisterRequest{})
	if token != "" {
		req.Header().Set("Authorization", token)
	}
	_, err := f.client.Register(context.Background(), req)
	return err
}

func wantCode(t *testing.T, err error, code connect.Code, what string) {
	t.Helper()
	if connect.CodeOf(err) != code {
		t.Errorf("%s: got %v, want %v", what, err, code)
	}
}

func TestAuthRejectsBadKeys(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	good, err := auth.CreateKey(ctx, f.owner, "good", "db1.example.com", "test")
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := auth.CreateKey(ctx, f.owner, "revoked", "", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RevokeKey(ctx, f.owner, "revoked", "test"); err != nil {
		t.Fatal(err)
	}
	gid, gsec, _ := auth.ParseKey(good.Token)
	_, rsec, _ := auth.ParseKey(revoked.Token)

	if err := f.call("Bearer " + good.Token); err != nil {
		t.Fatalf("good key: %v", err)
	}
	for _, scheme := range []string{"bearer ", "BEARER ", "bEaReR "} {
		if err := f.call(scheme + good.Token); err != nil {
			t.Errorf("scheme %q: %v", scheme, err)
		}
	}
	if len(f.stub.got) != 4 || f.stub.got[0].ID != gid || f.stub.got[0].FQDN != "db1.example.com" || f.stub.got[0].Name != "good" {
		t.Errorf("context key = %+v, want id %d pinned to db1.example.com", f.stub.got, gid)
	}

	for what, tok := range map[string]string{
		"no header":         "",
		"not bearer":        "Basic " + good.Token,
		"bare token":        good.Token,
		"malformed":         "Bearer rotten_notanid_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"garbage":           "Bearer hunter2",
		"unknown id":        "Bearer " + auth.FormatKey(gid+1000, gsec),
		"wrong secret":      "Bearer " + auth.FormatKey(gid, rsec),
		"revoked":           "Bearer " + revoked.Token,
		"empty bearer":      "Bearer ",
		"secret, wrong key": "Bearer " + auth.FormatKey(gid+1, gsec),
	} {
		wantCode(t, f.call(tok), connect.CodeUnauthenticated, what)
	}
	if strings.Contains(f.logs.String(), gsec) || strings.Contains(f.logs.String(), rsec) {
		t.Errorf("logs contain a secret:\n%s", f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "key_id=") {
		t.Errorf("logs don't name the failing key id:\n%s", f.logs.String())
	}
}

func TestRevokedMidConnectionRejectedWithinTTL(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	k, err := auth.CreateKey(ctx, f.owner, "w1", "", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.call("Bearer " + k.Token); err != nil {
		t.Fatalf("before revoke: %v", err)
	}
	if err := auth.RevokeKey(ctx, f.owner, "w1", "test"); err != nil {
		t.Fatal(err)
	}
	// Still cached: the TTL bounds how long a revoked key keeps working.
	f.clk.Add(10 * time.Second)
	if err := f.call("Bearer " + k.Token); err != nil {
		t.Fatalf("inside TTL, cached: %v", err)
	}
	f.clk.Add(21 * time.Second)
	wantCode(t, f.call("Bearer "+k.Token), connect.CodeUnauthenticated, "after TTL")
}

func TestLastUsedThrottled(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	k, err := auth.CreateKey(ctx, f.owner, "w1", "", "test")
	if err != nil {
		t.Fatal(err)
	}
	lastUsed := func() *time.Time {
		var ts *time.Time
		if err := f.owner.QueryRow(ctx, "select last_used_at from rotten.api_keys where name = 'w1'").Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}
	if lastUsed() != nil {
		t.Fatal("new key already has last_used_at")
	}
	if err := f.call("Bearer " + k.Token); err != nil {
		t.Fatal(err)
	}
	first := lastUsed()
	if first == nil {
		t.Fatal("last_used_at not set after a call")
	}
	if _, err := f.owner.Exec(ctx, "update rotten.api_keys set last_used_at = '2000-01-01 12:00:00+00' where name = 'w1'"); err != nil {
		t.Fatal(err)
	}
	f.clk.Add(40 * time.Second) // past the cache TTL, inside the touch interval
	if err := f.call("Bearer " + k.Token); err != nil {
		t.Fatal(err)
	}
	if got := lastUsed(); got.Year() != 2000 {
		t.Errorf("last_used_at updated again within a minute: %v", got)
	}
	f.clk.Add(30 * time.Second)
	if err := f.call("Bearer " + k.Token); err != nil {
		t.Fatal(err)
	}
	if got := lastUsed(); got.Year() == 2000 {
		t.Error("last_used_at not updated after a minute")
	}
}

// Good and bad secrets race while the clock pushes entries past the TTL.
// Run under -race. Every good call passes and every bad one is refused.
func TestConcurrentAuthAcrossTTL(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	k, err := auth.CreateKey(ctx, f.owner, "w1", "db1.example.com", "test")
	if err != nil {
		t.Fatal(err)
	}
	id, _, _ := auth.ParseKey(k.Token)
	other, _, _ := auth.NewSecret()
	bad := auth.FormatKey(id, other)

	var wg sync.WaitGroup
	errs := make(chan string, 64*10)
	for g := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				if g == 0 {
					f.clk.Add(7 * time.Second)
				}
				good := (g+i)%2 == 0
				tok := k.Token
				if !good {
					tok = bad
				}
				err := f.call("Bearer " + tok)
				if good && err != nil {
					errs <- "good secret refused: " + err.Error()
				}
				if !good && connect.CodeOf(err) != connect.CodeUnauthenticated {
					errs <- "bad secret not Unauthenticated"
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
