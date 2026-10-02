package main

import (
	"context"
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
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/ingest"
)

func runServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("dsn", "", "rotten_ingest DSN (default $ROTTEN_INGEST_DSN)")
	listen := fs.String("listen", "", "HTTPS listen address (default $ROTTEN_LISTEN, else :8443)")
	certFile := fs.String("tls-cert", "", "PEM certificate chain file (default $ROTTEN_TLS_CERT)")
	keyFile := fs.String("tls-key", "", "PEM private key file (default $ROTTEN_TLS_KEY)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "rotten-server serve: unexpected arguments %q\n", fs.Args())
		return 2
	}
	// Visit distinguishes an explicit empty flag from an absent flag. No env
	// values appear in flag help, which could otherwise expose a DSN password.
	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	for _, setting := range []struct {
		name, env string
		value     *string
	}{
		{"dsn", "ROTTEN_INGEST_DSN", dsn},
		{"listen", "ROTTEN_LISTEN", listen},
		{"tls-cert", "ROTTEN_TLS_CERT", certFile},
		{"tls-key", "ROTTEN_TLS_KEY", keyFile},
	} {
		if !explicit[setting.name] {
			*setting.value = os.Getenv(setting.env)
		}
	}
	if *listen == "" {
		*listen = ":8443"
	}
	if *certFile == "" || *keyFile == "" {
		fmt.Fprintln(stderr, "rotten-server serve: both -tls-cert and -tls-key are required (or ROTTEN_TLS_CERT and ROTTEN_TLS_KEY)")
		return 2
	}
	certificate, err := loadServerCertificate(*certFile, *keyFile)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: load TLS certificate: %v\n", err)
		return 1
	}
	if *dsn == "" {
		fmt.Fprintln(stderr, "rotten-server serve: no DSN; pass -dsn or set ROTTEN_INGEST_DSN")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: database configuration: %v\n", err)
		return 1
	}
	defer pool.Close()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = pool.Ping(connectCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server serve: connect database: %v\n", err)
		return 1
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	authenticator := auth.New(auth.NewPGStore(pool), auth.Options{Logger: logger})
	path, handler := rottenv1connect.NewIngestServiceHandler(
		ingest.NewHandler(pool, ingest.Options{Logger: logger}),
		connect.WithInterceptors(authenticator.Interceptor()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{
		Handler: mux, TLSConfig: certificate.config(),
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		ErrorLog: log.New(stderr, "rotten-server: ", log.LstdFlags),
	}
	listener, err := net.Listen("tcp", *listen)
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
	served := make(chan error, 1)
	go func() { served <- server.ServeTLS(listener, "", "") }()
	fmt.Fprintf(stdout, "listening on https://%s\n", listener.Addr())
	select {
	case err = <-served:
	case <-ctx.Done():
		// Basic lifecycle cleanup only; draining in-flight RPCs belongs to -36.
		if closeErr := server.Close(); closeErr != nil {
			fmt.Fprintf(stderr, "rotten-server serve: close: %v\n", closeErr)
			return 1
		}
		err = <-served
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "rotten-server serve: %v\n", err)
		return 1
	}
	return 0
}
