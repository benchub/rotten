package serverclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/harvestlimits"
)

const (
	DefaultCallTimeout     = 30 * time.Second
	DefaultMaxAttempts     = 5
	DefaultInitialBackoff  = 200 * time.Millisecond
	DefaultMaxBackoff      = 5 * time.Second
	DefaultKeepaliveIdle   = 30 * time.Second
	DefaultKeepalivePing   = 10 * time.Second
	DefaultIdleConnTimeout = 90 * time.Second
)

// Config builds a worker-to-server Connect client.
type Config struct {
	ServerURL       string
	PassKeyFile     string
	CAFile          string
	CallTimeout     time.Duration
	MaxAttempts     int
	Backoff         Backoff
	Logger          *slog.Logger
	HTTPClient      *http.Client
	KeepaliveIdle   time.Duration
	KeepalivePing   time.Duration
	IdleConnTimeout time.Duration
}

// Backoff returns the delay before retry attempt n, where n starts at 1.
type Backoff interface {
	Next(attempt int) time.Duration
}

// ClipCounts reports client-side text truncation performed before sending.
type ClipCounts struct {
	Normalized     int
	ContextStrings int
}

// Client sends worker RPCs to rotten-server.
type Client struct {
	key         string
	callTimeout time.Duration
	maxAttempts int
	backoff     Backoff
	logger      *slog.Logger
	httpClient  *http.Client
	rpc         rottenv1connect.IngestServiceClient
}

func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.ServerURL) == "" {
		return nil, errors.New("server URL is required")
	}
	keyBytes, err := os.ReadFile(cfg.PassKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read pass key file: %w", err)
	}
	key := strings.TrimSpace(string(keyBytes))
	if key == "" {
		return nil, errors.New("pass key file is empty")
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.MaxAttempts < 1 {
		return nil, errors.New("max attempts must be at least 1")
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff{Initial: DefaultInitialBackoff, Max: DefaultMaxBackoff, Jitter: 0.2}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient, err = newHTTPClient(cfg)
		if err != nil {
			return nil, err
		}
	}
	c := &Client{
		key:         key,
		callTimeout: cfg.CallTimeout,
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		logger:      cfg.Logger,
		httpClient:  httpClient,
	}
	c.rpc = rottenv1connect.NewIngestServiceClient(httpClient, cfg.ServerURL)
	return c, nil
}

func newHTTPClient(cfg Config) (*http.Client, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read server CA file: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("server CA file contained no certificates")
		}
	}
	if cfg.KeepaliveIdle == 0 {
		cfg.KeepaliveIdle = DefaultKeepaliveIdle
	}
	if cfg.KeepalivePing == 0 {
		cfg.KeepalivePing = DefaultKeepalivePing
	}
	if cfg.IdleConnTimeout == 0 {
		cfg.IdleConnTimeout = DefaultIdleConnTimeout
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    roots,
		},
		Protocols:             new(http.Protocols),
		HTTP2:                 &http.HTTP2Config{SendPingTimeout: cfg.KeepaliveIdle, PingTimeout: cfg.KeepalivePing},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	tr.Protocols.SetHTTP1(true)
	tr.Protocols.SetHTTP2(true)
	return &http.Client{Transport: tr}, nil
}

func (c *Client) Register(ctx context.Context, msg *rottenv1.RegisterRequest) (*rottenv1.RegisterResponse, error) {
	return callWithRetry(ctx, c, func(ctx context.Context) (*rottenv1.RegisterResponse, error) {
		resp, err := c.rpc.Register(ctx, request(c, msg))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	})
}

func (c *Client) SubmitHarvest(ctx context.Context, msg *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestResponse, ClipCounts, error) {
	prepared, clips := prepareHarvest(msg)
	if err := preflightHarvest(prepared); err != nil {
		return nil, clips, err
	}
	if clips.Normalized > 0 || clips.ContextStrings > 0 {
		c.logger.Warn("clipped harvest text to server limits", "normalized", clips.Normalized, "context_strings", clips.ContextStrings, "batch_id", prepared.GetBatchId())
	}
	resp, err := callWithRetry(ctx, c, func(ctx context.Context) (*rottenv1.SubmitHarvestResponse, error) {
		resp, err := c.rpc.SubmitHarvest(ctx, request(c, prepared))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	})
	if err != nil {
		return nil, clips, err
	}
	return resp, clips, nil
}

// HarvestTooLargeError marks a locally detected permanent oversize harvest.
// Server-sent ResourceExhausted is operational and retryable; this client-side
// case is safe for the outbox sender to drop because the same stored bytes will
// never fit.
type HarvestTooLargeError struct {
	Err error
}

func (e *HarvestTooLargeError) Error() string {
	if e == nil || e.Err == nil {
		return "harvest message is too large"
	}
	return e.Err.Error()
}

func (e *HarvestTooLargeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewHarvestTooLargeError(err error) error {
	return connect.NewError(connect.CodeResourceExhausted, &HarvestTooLargeError{Err: err})
}

func IsHarvestTooLarge(err error) bool {
	var tooLarge *HarvestTooLargeError
	return errors.As(err, &tooLarge)
}

func request[M any](c *Client, msg *M) *connect.Request[M] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+c.key)
	return req
}

func callWithRetry[T any](ctx context.Context, c *Client, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	var last error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		callCtx := ctx
		cancel := func() {}
		if c.callTimeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, c.callTimeout)
		}
		v, err := fn(callCtx)
		cancel()
		if err == nil {
			return v, nil
		}
		last = err
		if attempt == c.maxAttempts || !retryable(err) {
			return zero, err
		}
		delay := c.backoff.Next(attempt)
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
	return zero, last
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	switch code := connect.CodeOf(err); code {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeAborted, connect.CodeResourceExhausted:
		return true
	case connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeAlreadyExists:
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func prepareHarvest(msg *rottenv1.SubmitHarvestRequest) (*rottenv1.SubmitHarvestRequest, ClipCounts) {
	if msg == nil {
		return nil, ClipCounts{}
	}
	out := proto.Clone(msg).(*rottenv1.SubmitHarvestRequest)
	var clips ClipCounts
	for _, aggregate := range out.GetAggregates() {
		if aggregate == nil {
			continue
		}
		if clipped, ok := clipText(aggregate.GetNormalized(), harvestlimits.MaxNormalizedBytes); ok {
			aggregate.Normalized = clipped
			clips.Normalized++
		}
		for _, qc := range aggregate.GetContexts() {
			if qc == nil {
				continue
			}
			if clipped, ok := clipText(qc.GetController(), harvestlimits.MaxContextStringBytes); ok {
				qc.Controller = clipped
				clips.ContextStrings++
			}
			if clipped, ok := clipText(qc.GetAction(), harvestlimits.MaxContextStringBytes); ok {
				qc.Action = clipped
				clips.ContextStrings++
			}
			if clipped, ok := clipText(qc.GetJobTag(), harvestlimits.MaxContextStringBytes); ok {
				qc.JobTag = clipped
				clips.ContextStrings++
			}
		}
	}
	return out, clips
}

func preflightHarvest(msg *rottenv1.SubmitHarvestRequest) error {
	if msg == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("harvest request is required"))
	}
	if size := proto.Size(msg); size > harvestlimits.MaxIngestMessageBytes {
		return NewHarvestTooLargeError(fmt.Errorf("harvest message exceeds %d bytes", harvestlimits.MaxIngestMessageBytes))
	}
	// Future-skew validation depends on the server's clock, so the client
	// deliberately skips only that shared check.
	if _, _, err := harvestlimits.ValidateHarvest(msg, time.Now().UTC(), harvestlimits.SkipFutureSkew); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

func clipText(s string, maxBytes int) (string, bool) {
	repaired := strings.ToValidUTF8(s, "\uFFFD")
	changed := repaired != s
	if len(repaired) <= maxBytes {
		return repaired, changed
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(repaired[cut]) {
		cut--
	}
	return repaired[:cut], true
}

type ExponentialBackoff struct {
	Initial time.Duration
	Max     time.Duration
	Jitter  float64
}

func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if b.Initial <= 0 {
		b.Initial = DefaultInitialBackoff
	}
	if b.Max <= 0 {
		b.Max = DefaultMaxBackoff
	}
	delay := b.Initial
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= b.Max {
			delay = b.Max
			break
		}
	}
	if b.Jitter <= 0 {
		return delay
	}
	jitter := b.Jitter
	if jitter > 1 {
		jitter = 1
	}
	span := int64(float64(delay) * jitter)
	if span == 0 {
		return delay
	}
	return delay - time.Duration(span) + time.Duration(rand.Int64N(span*2+1))
}
