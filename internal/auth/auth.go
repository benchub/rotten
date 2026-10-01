package auth

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultTTL bounds how long a revoked key keeps working.
const DefaultTTL = 30 * time.Second

// DefaultTouchEvery throttles last_used_at writes per key.
const DefaultTouchEvery = time.Minute

// Row is what the ingest role may read from api_keys.
type Row struct {
	ID         int64
	Name       string
	SecretHash string
	FQDN       string
	Revoked    bool
}

// Store looks up keys and records use.
type Store interface {
	// Lookup returns the row for id, or found=false.
	Lookup(ctx context.Context, id int64) (row Row, found bool, err error)
	Touch(ctx context.Context, id int64, at time.Time) error
}

// PGStore is a Store on the rotten_ingest connection. It names only the
// columns rotten_ingest can read.
type PGStore struct{ pool *pgxpool.Pool }

func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

func (s *PGStore) Lookup(ctx context.Context, id int64) (Row, bool, error) {
	var r Row
	var fqdn *string
	var revoked *time.Time
	err := s.pool.QueryRow(ctx,
		`select id, name, secret_hash, fqdn, revoked_at from rotten.api_keys where id = $1`, id).
		Scan(&r.ID, &r.Name, &r.SecretHash, &fqdn, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return Row{}, false, nil
	}
	if err != nil {
		return Row{}, false, err
	}
	if fqdn != nil {
		r.FQDN = *fqdn
	}
	r.Revoked = revoked != nil
	return r, true, nil
}

func (s *PGStore) Touch(ctx context.Context, id int64, at time.Time) error {
	_, err := s.pool.Exec(ctx, `update rotten.api_keys set last_used_at = $2 where id = $1`, id, at)
	return err
}

// Options configures an Authenticator. Zero values take the defaults.
type Options struct {
	TTL        time.Duration
	TouchEvery time.Duration
	Now        func() time.Time
	Logger     *slog.Logger
}

// Authenticator checks bearer pass keys, caching lookups for TTL.
type Authenticator struct {
	store Store
	opt   Options

	mu      sync.Mutex
	cache   map[int64]cached
	touched map[int64]time.Time
}

type cached struct {
	row Row
	at  time.Time
}

func New(store Store, opt Options) *Authenticator {
	if opt.TTL <= 0 {
		opt.TTL = DefaultTTL
	}
	if opt.TouchEvery <= 0 {
		opt.TouchEvery = DefaultTouchEvery
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Authenticator{store: store, opt: opt, cache: map[int64]cached{}, touched: map[int64]time.Time{}}
}

var errUnauth = connect.NewError(connect.CodeUnauthenticated, errors.New("invalid pass key"))

// Authenticate checks an Authorization header value. Failures return a
// generic Unauthenticated error; the log names the key id at most.
func (a *Authenticator) Authenticate(ctx context.Context, header string) (Key, error) {
	// The auth scheme is case-insensitive (RFC 9110, section 11.1).
	scheme, tok, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		a.opt.Logger.Info("auth: missing bearer token")
		return Key{}, errUnauth
	}
	id, secret, err := ParseKey(strings.TrimSpace(tok))
	if err != nil {
		a.opt.Logger.Info("auth: malformed pass key")
		return Key{}, errUnauth
	}
	row, found, err := a.lookup(ctx, id)
	if err != nil {
		a.opt.Logger.Error("auth: key lookup failed", "key_id", id, "err", err)
		return Key{}, connect.NewError(connect.CodeUnavailable, errors.New("key lookup failed"))
	}
	if !found {
		a.opt.Logger.Info("auth: unknown pass key", "key_id", id)
		return Key{}, errUnauth
	}
	want, err := hex.DecodeString(row.SecretHash)
	sum := HashSecret(secret)
	got, _ := hex.DecodeString(sum)
	if err != nil || subtle.ConstantTimeCompare(got, want) != 1 {
		a.opt.Logger.Info("auth: wrong secret", "key_id", id)
		return Key{}, errUnauth
	}
	if row.Revoked {
		a.opt.Logger.Info("auth: revoked pass key", "key_id", id)
		return Key{}, errUnauth
	}
	a.touch(ctx, id)
	return Key{ID: row.ID, Name: row.Name, FQDN: row.FQDN}, nil
}

// lookup caches found rows only. Not caching misses lets a just-created key
// work right away, at the cost of a query per unknown-id attempt.
func (a *Authenticator) lookup(ctx context.Context, id int64) (Row, bool, error) {
	now := a.opt.Now()
	a.mu.Lock()
	c, ok := a.cache[id]
	a.mu.Unlock()
	if ok && now.Sub(c.at) < a.opt.TTL {
		return c.row, true, nil
	}
	row, found, err := a.store.Lookup(ctx, id)
	if err != nil {
		return Row{}, false, err
	}
	a.mu.Lock()
	if found {
		a.cache[id] = cached{row: row, at: now}
	} else {
		delete(a.cache, id)
	}
	a.mu.Unlock()
	return row, found, nil
}

func (a *Authenticator) touch(ctx context.Context, id int64) {
	now := a.opt.Now()
	a.mu.Lock()
	last, ok := a.touched[id]
	due := !ok || now.Sub(last) >= a.opt.TouchEvery
	if due {
		a.touched[id] = now
	}
	a.mu.Unlock()
	if !due {
		return
	}
	if err := a.store.Touch(ctx, id, now); err != nil {
		a.opt.Logger.Warn("auth: update last_used_at failed", "key_id", id, "err", err)
	}
}

type ctxKey struct{}

// FromContext returns the key the interceptor authenticated.
func FromContext(ctx context.Context) (Key, bool) {
	k, ok := ctx.Value(ctxKey{}).(Key)
	return k, ok
}

// NewContext attaches k to ctx. Handlers' tests may use it directly.
func NewContext(ctx context.Context, k Key) context.Context {
	return context.WithValue(ctx, ctxKey{}, k)
}

// Interceptor returns a Connect interceptor that authenticates every call.
func (a *Authenticator) Interceptor() connect.Interceptor { return interceptor{a} }

type interceptor struct{ a *Authenticator }

func (i interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if req.Spec().IsClient {
			return next(ctx, req)
		}
		k, err := i.a.Authenticate(ctx, req.Header().Get("Authorization"))
		if err != nil {
			return nil, err
		}
		return next(NewContext(ctx, k), req)
	}
}

func (i interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		k, err := i.a.Authenticate(ctx, conn.RequestHeader().Get("Authorization"))
		if err != nil {
			return err
		}
		return next(NewContext(ctx, k), conn)
	}
}
