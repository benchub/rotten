package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/ingest"
)

const (
	defaultServeListen         = ":8443"
	defaultShutdownTimeout     = 10 * time.Second
	defaultHealthCheckTimeout  = time.Second
	defaultServeConfigFilename = ""
)

// ServeFileConfig is the JSON configuration file format for rotten-server.
// Durations are in seconds, matching the worker config's interval fields.
type ServeFileConfig struct {
	DSN                    string
	Listen                 string
	TLSCert                string
	TLSKey                 string
	ShutdownTimeout        uint32
	HealthTimeout          uint32
	FailedAuthBurst        uint32
	FailedAuthRefill       uint32
	GlobalFailedAuthBurst  uint32
	GlobalFailedAuthRefill uint32
}

type serveConfig struct {
	DSN                    string
	Listen                 string
	TLSCert                string
	TLSKey                 string
	ShutdownTimeout        time.Duration
	HealthTimeout          time.Duration
	FailedAuthBurst        int
	FailedAuthRefill       time.Duration
	GlobalFailedAuthBurst  int
	GlobalFailedAuthRefill time.Duration
}

func runServe(args []string, stdout, stderr io.Writer) int {
	cfg, err := loadServeConfig(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 2
		}
		fmt.Fprintf(stderr, "rotten-server serve: %v\n", err)
		return 2
	}
	if cfg.TLSCert == "" || cfg.TLSKey == "" {
		fmt.Fprintln(stderr, "rotten-server serve: both -tls-cert and -tls-key are required (or ROTTEN_SERVER_TLS_CERT and ROTTEN_SERVER_TLS_KEY)")
		return 2
	}
	certificate, err := loadServerCertificate(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: load TLS certificate: %v\n", err)
		return 1
	}
	if cfg.DSN == "" {
		fmt.Fprintln(stderr, "rotten-server serve: no DSN; pass -dsn, set ROTTEN_SERVER_DSN, or set DSN in the config file")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: database configuration: %v\n", err)
		return 1
	}
	skipPoolClose := false
	defer func() {
		if !skipPoolClose {
			pool.Close()
		}
	}()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = pool.Ping(connectCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: connect database: %v\n", err)
		return 1
	}
	authenticator := auth.New(auth.NewPGStore(pool), auth.Options{
		Logger:                 logger,
		FailedAuthBurst:        cfg.FailedAuthBurst,
		FailedAuthRefill:       cfg.FailedAuthRefill,
		GlobalFailedAuthBurst:  cfg.GlobalFailedAuthBurst,
		GlobalFailedAuthRefill: cfg.GlobalFailedAuthRefill,
	})
	preloadCtx, cancelPreload := context.WithTimeout(ctx, 10*time.Second)
	_ = authenticator.Preload(preloadCtx)
	cancelPreload()
	path, handler := rottenv1connect.NewIngestServiceHandler(
		ingest.NewHandler(pool, ingest.Options{Logger: logger}),
		connect.WithInterceptors(authenticator.Interceptor()),
		connect.WithReadMaxBytes(ingest.MaxIngestMessageBytes),
	)
	mux := http.NewServeMux()
	mux.Handle("/healthz", healthHandler(pool, cfg.HealthTimeout, logger))
	mux.Handle(path, handler)
	tracker := &requestTracker{handler: mux}
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	server := &http.Server{
		Handler: tracker, TLSConfig: certificate.config(),
		BaseContext: func(net.Listener) context.Context {
			return baseCtx
		},
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		ErrorLog: log.New(slogErrorWriter{logger: logger}, "", 0),
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: listen: %v\n", err)
		return 1
	}
	defer listener.Close()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		certificate.watch(watchCtx, hup, logger)
	}()
	defer func() { cancelWatch(); <-watchDone }()
	pruneCtx, cancelPrune := context.WithCancel(ctx)
	pruneDone := make(chan struct{})
	go func() {
		defer close(pruneDone)
		ingest.RunPruner(pruneCtx, pool, time.Hour, logger)
	}()
	defer func() { cancelPrune(); <-pruneDone }()
	repairCtx, cancelRepair := context.WithCancel(ctx)
	repairDone := make(chan struct{})
	go func() {
		defer close(repairDone)
		ingest.RunContextRepair(repairCtx, pool, time.Hour, logger)
	}()
	defer func() { cancelRepair(); <-repairDone }()
	served := make(chan error, 1)
	go func() { served <- server.ServeTLS(listener, "", "") }()
	fmt.Fprintf(stdout, "listening on https://%s\n", listener.Addr())
	select {
	case err = <-served:
	case <-ctx.Done():
		stop()
		logger.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		closeErr := server.Shutdown(shutdownCtx)
		cancelShutdown()
		if closeErr != nil {
			logger.Error("shutdown timed out; forcing close", "err", closeErr)
			cancelBase()
			if err := server.Close(); err != nil {
				logger.Error("force close failed", "err", err)
			}
			if !tracker.wait(5 * time.Second) {
				logger.Error("handlers did not stop after forced close")
				skipPoolClose = true
			}
			return 1
		}
		err = <-served
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serve failed", "err", err)
		return 1
	}
	return 0
}

type requestTracker struct {
	handler http.Handler
	wg      sync.WaitGroup
}

func (t *requestTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.wg.Add(1)
	defer t.wg.Done()
	t.handler.ServeHTTP(w, r)
}

func (t *requestTracker) wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.wg.Wait()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func loadServeConfig(args []string, stderr io.Writer) (serveConfig, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configFile := fs.String("config", defaultServeConfigFilename, "JSON config file")
	dsn := fs.String("dsn", "", serveFlagHelp("rotten_ingest DSN", "ROTTEN_SERVER_DSN", "DSN", ""))
	listen := fs.String("listen", "", serveFlagHelp("HTTPS listen address", "ROTTEN_SERVER_LISTEN", "Listen", defaultServeListen))
	certFile := fs.String("tls-cert", "", serveFlagHelp("PEM certificate chain file", "ROTTEN_SERVER_TLS_CERT", "TLSCert", ""))
	keyFile := fs.String("tls-key", "", serveFlagHelp("PEM private key file", "ROTTEN_SERVER_TLS_KEY", "TLSKey", ""))
	shutdownTimeout := fs.Uint("shutdown-timeout", 0, serveFlagHelp("graceful shutdown timeout in seconds", "ROTTEN_SERVER_SHUTDOWN_TIMEOUT", "ShutdownTimeout", wholeSeconds(defaultShutdownTimeout)))
	healthTimeout := fs.Uint("health-timeout", 0, serveFlagHelp("health database ping timeout in seconds", "ROTTEN_SERVER_HEALTH_TIMEOUT", "HealthTimeout", wholeSeconds(defaultHealthCheckTimeout)))
	failedAuthBurst := fs.Uint("failed-auth-burst", 0, serveFlagHelp("failed auth lookup burst per client", "ROTTEN_SERVER_FAILED_AUTH_BURST", "FailedAuthBurst", strconv.Itoa(auth.DefaultFailedAuthBurst)))
	failedAuthRefill := fs.Uint("failed-auth-refill", 0, serveFlagHelp("failed auth token refill in seconds", "ROTTEN_SERVER_FAILED_AUTH_REFILL", "FailedAuthRefill", wholeSeconds(auth.DefaultFailedAuthRefill)))
	globalFailedAuthBurst := fs.Uint("global-failed-auth-burst", 0, serveFlagHelp("global failed auth lookup burst", "ROTTEN_SERVER_GLOBAL_FAILED_AUTH_BURST", "GlobalFailedAuthBurst", strconv.Itoa(auth.DefaultGlobalFailedAuthBurst)))
	globalFailedAuthRefill := fs.Uint("global-failed-auth-refill", 0, serveFlagHelp("global failed auth token refill in seconds", "ROTTEN_SERVER_GLOBAL_FAILED_AUTH_REFILL", "GlobalFailedAuthRefill", wholeSeconds(auth.DefaultGlobalFailedAuthRefill)))
	if err := fs.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if fs.NArg() != 0 {
		return serveConfig{}, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	cfg := serveConfig{}
	if *configFile != "" {
		b, err := os.ReadFile(*configFile)
		if err != nil {
			return serveConfig{}, fmt.Errorf("read config file: %w", err)
		}
		var file ServeFileConfig
		if err := json.Unmarshal(b, &file); err != nil {
			return serveConfig{}, fmt.Errorf("decode config file: %w", err)
		}
		cfg = serveConfig{
			DSN:     file.DSN,
			Listen:  file.Listen,
			TLSCert: file.TLSCert,
			TLSKey:  file.TLSKey,
		}
		cfg.ShutdownTimeout = secondsDuration(file.ShutdownTimeout)
		cfg.HealthTimeout = secondsDuration(file.HealthTimeout)
		cfg.FailedAuthBurst = int(file.FailedAuthBurst)
		cfg.FailedAuthRefill = secondsDuration(file.FailedAuthRefill)
		cfg.GlobalFailedAuthBurst = int(file.GlobalFailedAuthBurst)
		cfg.GlobalFailedAuthRefill = secondsDuration(file.GlobalFailedAuthRefill)
	}
	if !explicit["dsn"] {
		if v := firstEnv("ROTTEN_SERVER_DSN", legacyEnv(*configFile, "ROTTEN_INGEST_DSN")); v != "" {
			cfg.DSN = v
		}
	} else {
		cfg.DSN = *dsn
	}
	if !explicit["listen"] {
		if v := firstEnv("ROTTEN_SERVER_LISTEN", legacyEnv(*configFile, "ROTTEN_LISTEN")); v != "" {
			cfg.Listen = v
		}
	} else {
		cfg.Listen = *listen
	}
	if !explicit["tls-cert"] {
		if v := firstEnv("ROTTEN_SERVER_TLS_CERT", legacyEnv(*configFile, "ROTTEN_TLS_CERT")); v != "" {
			cfg.TLSCert = v
		}
	} else {
		cfg.TLSCert = *certFile
	}
	if !explicit["tls-key"] {
		if v := firstEnv("ROTTEN_SERVER_TLS_KEY", legacyEnv(*configFile, "ROTTEN_TLS_KEY")); v != "" {
			cfg.TLSKey = v
		}
	} else {
		cfg.TLSKey = *keyFile
	}
	if !explicit["shutdown-timeout"] {
		if v, ok, err := envSeconds("ROTTEN_SERVER_SHUTDOWN_TIMEOUT"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.ShutdownTimeout = v
		}
	} else {
		cfg.ShutdownTimeout = time.Duration(*shutdownTimeout) * time.Second
	}
	if !explicit["health-timeout"] {
		if v, ok, err := envSeconds("ROTTEN_SERVER_HEALTH_TIMEOUT"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.HealthTimeout = v
		}
	} else {
		cfg.HealthTimeout = time.Duration(*healthTimeout) * time.Second
	}
	if !explicit["failed-auth-burst"] {
		if v, ok, err := envUint("ROTTEN_SERVER_FAILED_AUTH_BURST"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.FailedAuthBurst = int(v)
		}
	} else {
		cfg.FailedAuthBurst = int(*failedAuthBurst)
	}
	if !explicit["failed-auth-refill"] {
		if v, ok, err := envSeconds("ROTTEN_SERVER_FAILED_AUTH_REFILL"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.FailedAuthRefill = v
		}
	} else {
		cfg.FailedAuthRefill = time.Duration(*failedAuthRefill) * time.Second
	}
	if !explicit["global-failed-auth-burst"] {
		if v, ok, err := envUint("ROTTEN_SERVER_GLOBAL_FAILED_AUTH_BURST"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.GlobalFailedAuthBurst = int(v)
		}
	} else {
		cfg.GlobalFailedAuthBurst = int(*globalFailedAuthBurst)
	}
	if !explicit["global-failed-auth-refill"] {
		if v, ok, err := envSeconds("ROTTEN_SERVER_GLOBAL_FAILED_AUTH_REFILL"); err != nil {
			return serveConfig{}, err
		} else if ok {
			cfg.GlobalFailedAuthRefill = v
		}
	} else {
		cfg.GlobalFailedAuthRefill = time.Duration(*globalFailedAuthRefill) * time.Second
	}
	if cfg.Listen == "" {
		cfg.Listen = defaultServeListen
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.HealthTimeout == 0 {
		cfg.HealthTimeout = defaultHealthCheckTimeout
	}
	if cfg.FailedAuthBurst == 0 {
		cfg.FailedAuthBurst = auth.DefaultFailedAuthBurst
	}
	if cfg.FailedAuthRefill == 0 {
		cfg.FailedAuthRefill = auth.DefaultFailedAuthRefill
	}
	if cfg.GlobalFailedAuthBurst == 0 {
		cfg.GlobalFailedAuthBurst = auth.DefaultGlobalFailedAuthBurst
	}
	if cfg.GlobalFailedAuthRefill == 0 {
		cfg.GlobalFailedAuthRefill = auth.DefaultGlobalFailedAuthRefill
	}
	return cfg, nil
}

// serveFlagHelp states the order loadServeConfig applies: an explicit flag,
// then the environment variable, then the config file key, then the default.
func serveFlagHelp(desc, env, key, def string) string {
	s := desc + " (default $" + env + ", then config " + key
	if def != "" {
		s += ", else " + def
	}
	return s + ")"
}

func wholeSeconds(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10)
}

func secondsDuration(seconds uint32) time.Duration {
	return time.Duration(seconds) * time.Second
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if name == "" {
			continue
		}
		if v := os.Getenv(name); v != "" {
			return v
		}
	}
	return ""
}

func legacyEnv(configFile, name string) string {
	if configFile != "" {
		return ""
	}
	return name
}

func envSeconds(name string) (time.Duration, bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false, nil
	}
	seconds, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be whole seconds: %w", name, err)
	}
	return time.Duration(seconds) * time.Second, true, nil
}

func envUint(name string) (uint64, bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be a whole number: %w", name, err)
	}
	return n, true, nil
}

type slogErrorWriter struct {
	logger *slog.Logger
}

func (w slogErrorWriter) Write(p []byte) (int, error) {
	w.logger.Error("http server error", "message", strings.TrimSpace(string(p)))
	return len(p), nil
}

type pingDB interface {
	Ping(context.Context) error
}

func healthHandler(db pingDB, timeout time.Duration, logger *slog.Logger) http.Handler {
	if timeout <= 0 {
		timeout = defaultHealthCheckTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		err := db.Ping(ctx)
		cancel()
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err != nil {
			logger.Warn("health check failed")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "unhealthy\n")
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
}
