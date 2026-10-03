package auth

import (
	"container/list"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
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

// DefaultFailedAuthBurst is how many failed key lookups one client can burst.
const DefaultFailedAuthBurst = 5

// DefaultFailedAuthRefill is how often one failed-lookup token refills.
const DefaultFailedAuthRefill = 10 * time.Second

// DefaultFailedAuthMaxClients bounds the per-client failed-auth map.
const DefaultFailedAuthMaxClients = 1024

// DefaultGlobalFailedAuthBurst bounds failed key lookups across all clients.
const DefaultGlobalFailedAuthBurst = 50

// DefaultGlobalFailedAuthRefill is how often one global failed-lookup token refills.
const DefaultGlobalFailedAuthRefill = time.Second

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
	// Preload returns all non-revoked keys the ingest role may cache.
	Preload(ctx context.Context) ([]Row, error)
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

func (s *PGStore) Preload(ctx context.Context) ([]Row, error) {
	rows, err := s.pool.Query(ctx,
		`select id, name, secret_hash, fqdn, revoked_at from rotten.api_keys where revoked_at is null`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		var fqdn *string
		var revoked *time.Time
		if err := rows.Scan(&r.ID, &r.Name, &r.SecretHash, &fqdn, &revoked); err != nil {
			return nil, err
		}
		if fqdn != nil {
			r.FQDN = *fqdn
		}
		r.Revoked = revoked != nil
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PGStore) Touch(ctx context.Context, id int64, at time.Time) error {
	_, err := s.pool.Exec(ctx, `update rotten.api_keys set last_used_at = $2 where id = $1`, id, at)
	return err
}

// Options configures an Authenticator. Zero values take the defaults.
type Options struct {
	TTL                    time.Duration
	TouchEvery             time.Duration
	FailedAuthBurst        int
	FailedAuthRefill       time.Duration
	FailedAuthMaxClients   int
	GlobalFailedAuthBurst  int
	GlobalFailedAuthRefill time.Duration
	Now                    func() time.Time
	Logger                 *slog.Logger
}

// Authenticator checks bearer pass keys, caching lookups for TTL.
type Authenticator struct {
	store Store
	opt   Options

	mu      sync.Mutex
	cache   map[int64]cached
	touched map[int64]time.Time
	limiter *failedAuthLimiter
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
	if opt.FailedAuthBurst <= 0 {
		opt.FailedAuthBurst = DefaultFailedAuthBurst
	}
	if opt.FailedAuthRefill <= 0 {
		opt.FailedAuthRefill = DefaultFailedAuthRefill
	}
	if opt.FailedAuthMaxClients <= 0 {
		opt.FailedAuthMaxClients = DefaultFailedAuthMaxClients
	}
	if opt.GlobalFailedAuthBurst <= 0 {
		opt.GlobalFailedAuthBurst = DefaultGlobalFailedAuthBurst
	}
	if opt.GlobalFailedAuthRefill <= 0 {
		opt.GlobalFailedAuthRefill = DefaultGlobalFailedAuthRefill
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Authenticator{store: store, opt: opt, cache: map[int64]cached{}, touched: map[int64]time.Time{}, limiter: newFailedAuthLimiter(opt)}
}

var errUnauth = connect.NewError(connect.CodeUnauthenticated, errors.New("invalid pass key"))

// Authenticate checks an Authorization header value. Failures return a
// generic Unauthenticated error; the log names the key id at most.
func (a *Authenticator) Authenticate(ctx context.Context, header string) (Key, error) {
	return a.authenticate(ctx, header, "")
}

func (a *Authenticator) Preload(ctx context.Context) error {
	rows, err := a.store.Preload(ctx)
	if err != nil {
		a.opt.Logger.Warn("auth: preload keys failed", "err", err)
		return err
	}
	now := a.opt.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, row := range rows {
		if row.Revoked {
			continue
		}
		a.cache[row.ID] = cached{row: row, at: now}
	}
	return nil
}

func (a *Authenticator) authenticate(ctx context.Context, header, peerAddr string) (Key, error) {
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
	clientKey := clientKeyFromPeerAddr(peerAddr)
	row, found, reservation, err := a.lookup(ctx, id, clientKey)
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
		if !reservation.used() {
			a.limiter.chargeCachedFailure(clientKey)
		}
		a.opt.Logger.Info("auth: wrong secret", "key_id", id)
		return Key{}, errUnauth
	}
	if row.Revoked {
		if !reservation.used() {
			a.limiter.chargeCachedFailure(clientKey)
		}
		a.opt.Logger.Info("auth: revoked pass key", "key_id", id)
		return Key{}, errUnauth
	}
	if reservation.used() {
		a.limiter.refund(reservation)
	}
	a.touch(ctx, id)
	return Key{ID: row.ID, Name: row.Name, FQDN: row.FQDN}, nil
}

// lookup caches found rows only. Uncached, previously unseen IDs spend from a
// per-client-IP bucket before hitting the database. Unknown keys keep the spent
// token; found valid keys and database errors refund it.
func (a *Authenticator) lookup(ctx context.Context, id int64, clientKey string) (Row, bool, failedAuthReservation, error) {
	now := a.opt.Now()
	a.mu.Lock()
	c, ok := a.cache[id]
	a.mu.Unlock()
	if ok && now.Sub(c.at) < a.opt.TTL {
		return c.row, true, failedAuthReservation{}, nil
	}
	var reservation failedAuthReservation
	if clientKey != "" && !ok {
		var allowed bool
		reservation, allowed = a.limiter.reserve(clientKey)
		if !allowed {
			return Row{}, false, failedAuthReservation{}, nil
		}
	}
	row, found, err := a.store.Lookup(ctx, id)
	if err != nil {
		if reservation.used() {
			a.limiter.refund(reservation)
		}
		return Row{}, false, failedAuthReservation{}, err
	}
	a.mu.Lock()
	if found {
		a.cache[id] = cached{row: row, at: now}
	} else {
		delete(a.cache, id)
	}
	a.mu.Unlock()
	return row, found, reservation, nil
}

func clientKeyFromPeerAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	v6 := ip.To16()
	if v6 == nil {
		return ""
	}
	prefix := make(net.IP, net.IPv6len)
	copy(prefix, v6)
	for i := 8; i < net.IPv6len; i++ {
		prefix[i] = 0
	}
	return (&net.IPNet{IP: prefix, Mask: net.CIDRMask(64, 128)}).String()
}

func (a *Authenticator) failedAuthClientCount() int {
	return a.limiter.count()
}

type failedAuthLimiter struct {
	mu           sync.Mutex
	now          func() time.Time
	burst        int
	refill       time.Duration
	maxClients   int
	idleTTL      time.Duration
	globalBurst  int
	globalRefill time.Duration
	buckets      map[string]*list.Element
	lru          *list.List
	global       authBucket
}

type authBucket struct {
	key        string
	tokens     int
	lastRefill time.Time
	lastSeen   time.Time
}

type failedAuthReservation struct {
	key    string
	client bool
	global bool
}

func (r failedAuthReservation) used() bool {
	return r.client || r.global
}

func newFailedAuthLimiter(opt Options) *failedAuthLimiter {
	return &failedAuthLimiter{
		now:          opt.Now,
		burst:        opt.FailedAuthBurst,
		refill:       opt.FailedAuthRefill,
		maxClients:   opt.FailedAuthMaxClients,
		idleTTL:      time.Duration(opt.FailedAuthBurst) * opt.FailedAuthRefill * 2,
		globalBurst:  opt.GlobalFailedAuthBurst,
		globalRefill: opt.GlobalFailedAuthRefill,
		buckets:      map[string]*list.Element{},
		lru:          list.New(),
		global: authBucket{
			key:        "global",
			tokens:     opt.GlobalFailedAuthBurst,
			lastRefill: opt.Now(),
			lastSeen:   opt.Now(),
		},
	}
}

func (l *failedAuthLimiter) reserve(key string) (failedAuthReservation, bool) {
	if key == "" {
		return failedAuthReservation{}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.bucketLocked(key, now)
	l.refillLocked(b, now)
	l.refillGlobalLocked(now)
	b.lastSeen = now
	if b.tokens <= 0 {
		return failedAuthReservation{}, false
	}
	if l.global.tokens <= 0 {
		return failedAuthReservation{}, false
	}
	b.tokens--
	l.global.tokens--
	l.global.lastSeen = now
	return failedAuthReservation{key: key, client: true, global: true}, true
}

func (l *failedAuthLimiter) refund(r failedAuthReservation) {
	if !r.used() {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if r.client {
		elem := l.buckets[r.key]
		if elem != nil {
			b := elem.Value.(*authBucket)
			l.refillLocked(b, now)
			if b.tokens < l.burst {
				b.tokens++
			}
			b.lastSeen = now
			l.lru.MoveToFront(elem)
		}
	}
	if r.global {
		l.refillGlobalLocked(now)
		if l.global.tokens < l.globalBurst {
			l.global.tokens++
		}
		l.global.lastSeen = now
	}
}

func (l *failedAuthLimiter) chargeCachedFailure(key string) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.bucketLocked(key, now)
	l.refillLocked(b, now)
	b.lastSeen = now
	if b.tokens > 0 {
		b.tokens--
	}
}

func (l *failedAuthLimiter) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evictExpiredTailLocked(l.now())
	return len(l.buckets)
}

func (l *failedAuthLimiter) bucketLocked(key string, now time.Time) *authBucket {
	if elem := l.buckets[key]; elem != nil {
		l.lru.MoveToFront(elem)
		return elem.Value.(*authBucket)
	}
	l.evictExpiredTailLocked(now)
	b := &authBucket{key: key, tokens: l.burst, lastRefill: now, lastSeen: now}
	elem := l.lru.PushFront(b)
	l.buckets[key] = elem
	l.enforceCapacityLocked()
	return b
}

func (l *failedAuthLimiter) refillLocked(b *authBucket, now time.Time) {
	l.refillBucketLocked(b, now, l.refill, l.burst)
}

func (l *failedAuthLimiter) refillGlobalLocked(now time.Time) {
	l.refillBucketLocked(&l.global, now, l.globalRefill, l.globalBurst)
}

func (l *failedAuthLimiter) refillBucketLocked(b *authBucket, now time.Time, refill time.Duration, burst int) {
	if now.Before(b.lastRefill) {
		b.lastRefill = now
		return
	}
	elapsed := now.Sub(b.lastRefill)
	if elapsed < refill {
		return
	}
	add := int(elapsed / refill)
	if add <= 0 {
		return
	}
	b.tokens += add
	if b.tokens > burst {
		b.tokens = burst
	}
	b.lastRefill = b.lastRefill.Add(time.Duration(add) * refill)
}

func (l *failedAuthLimiter) evictExpiredTailLocked(now time.Time) {
	if l.idleTTL <= 0 {
		return
	}
	for {
		elem := l.lru.Back()
		if elem == nil {
			return
		}
		b := elem.Value.(*authBucket)
		if now.Sub(b.lastSeen) < l.idleTTL {
			return
		}
		l.removeElementLocked(elem)
	}
}

func (l *failedAuthLimiter) enforceCapacityLocked() {
	if l.maxClients < 1 {
		l.maxClients = 1
	}
	for len(l.buckets) > l.maxClients {
		l.removeElementLocked(l.lru.Back())
	}
}

func (l *failedAuthLimiter) removeElementLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	b := elem.Value.(*authBucket)
	delete(l.buckets, b.key)
	l.lru.Remove(elem)
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
		k, err := i.a.authenticate(ctx, req.Header().Get("Authorization"), req.Peer().Addr)
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
		k, err := i.a.authenticate(ctx, conn.RequestHeader().Get("Authorization"), conn.Peer().Addr)
		if err != nil {
			return err
		}
		return next(NewContext(ctx, k), conn)
	}
}
