// Command rotten-worker watches an observed database's pg_stat_statements and
// records what it sees in the rotten DB.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/serverclient"
	"github.com/benchub/rotten/internal/state"
	"github.com/benchub/rotten/internal/worker"
)

var configFileFlag = flag.String("config", "", "the config file")
var noIdleHandsFlag = flag.Bool("noIdleHands", false, "when set to true, enable a watchdog that exits nonzero if the worker stops making progress")
var debugFlag = flag.Bool("debug", false, "when set to true, turn on debugging")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

type Configuration struct {
	ObservedDBConn      []string
	ServerURL           string
	PassKeyFile         string
	ServerCAFile        string
	StateDir            string
	MaxSnapshotAge      uint32
	StatusInterval      uint32
	ObservationInterval uint32
	SanityCheck         string
	FQDN                string
	Project             string
	Environment         string
	Cluster             string
	Role                string
	ContextController   string
	ContextAction       string
	ContextJob          string
	// KeepSchemas is optional. Leaving it out (false) collapses schema
	// names in fingerprints. True keeps them apart.
	KeepSchemas bool
	// CursorPattern and TempTablePattern are optional. Leaving them out
	// keeps the built-in patterns. See the README.
	CursorPattern    string
	TempTablePattern string
	// MinmaxResetSchema is optional. It's the schema schema/observer.sql
	// put the 17+ min/max reset wrapper in. Leaving it out means "rotten".
	MinmaxResetSchema string
}

// stateSettings returns the state dir and max snapshot age for c.
func stateSettings(c *Configuration) (dir string, maxAge time.Duration) {
	return c.StateDir, time.Duration(c.MaxSnapshotAge) * time.Second
}

func remakeSSLCertConfig(connectionString string, host string) (*tls.Config, error) {
	// hacky hack solution to get the rootca files, as well as the client certs, so that we can build up a cert chain with all the intermediate certs.
	connectionStringSettings := make(map[string]string)

	// Split the string by spaces to get each key-value pair
	pairs := strings.Split(connectionString, " ")

	for _, pair := range pairs {
		// Split each pair by the equals sign to separate the key from the value
		kv := strings.Split(pair, "=")
		if len(kv) == 2 {
			// Insert the key and value into the map
			connectionStringSettings[kv[0]] = kv[1]
		}
	}

	// If we didn't pass in a host we want to explicitly use, just
	// use the first host in our list of hosts (i.e. host=host1[,host2[,host3]])
	if host == "" {
		host = strings.Split(connectionStringSettings["host"], ",")[0]
	}

	// Load root CA cert
	rootCertPool := x509.NewCertPool()
	rootCert, err := os.ReadFile(connectionStringSettings["sslrootcert"])
	if err != nil {
		return nil, fmt.Errorf("error loading root certificate: %w", err)
	}

	// Load client cert & key
	clientCert, err := os.ReadFile(connectionStringSettings["sslcert"])
	if err != nil {
		return nil, fmt.Errorf("failed to read client certificate file: %w", err)
	}
	clientKey, err := os.ReadFile(connectionStringSettings["sslkey"])
	if err != nil {
		return nil, fmt.Errorf("failed to read client key file: %w", err)
	}

	ok := rootCertPool.AppendCertsFromPEM(rootCert)
	if !ok {
		return nil, fmt.Errorf("failed to append root certificate to pool")
	}

	if *debugFlag {
		var block *pem.Block
		log.Println("Loaded Root CA Certificates:")
		rootsPEM := rootCert
		block, rootsPEM = pem.Decode(rootsPEM)
		if block != nil {
			if block.Type == "CERTIFICATE" {
				caCert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, fmt.Errorf("error parsing certificate: %w", err)
				}
				log.Printf("\tSubject: %s\n", caCert.Subject)
			}
		}
	}

	// Append the client cert and CA chain to get a full certificate chain
	clientChain := append(clientCert, []byte("\n")...)
	clientChain = append(clientChain, rootCert...)
	clientCerts, err := tls.X509KeyPair(clientChain, clientKey)
	if err != nil {
		return nil, fmt.Errorf("error loading client key pair: %w", err)
	}

	if *debugFlag {
		log.Println("Client Certificate and Chain:")
		for _, cert := range clientCerts.Certificate {
			parsedCert, err := x509.ParseCertificate(cert)
			if err != nil {
				return nil, fmt.Errorf("error parsing client certificate: %w", err)
			}
			log.Printf("\tSubject: %s\n", parsedCert.Subject)
		}
	}

	if *debugFlag {
		log.Println("Making tls config for", host)
	}
	// Create a custom TLS config with specific versions and cipher suites
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCerts},
		ClientCAs:    rootCertPool,
		RootCAs:      rootCertPool,
		ServerName:   host, // Set the ServerName to the host you are connecting to
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		},
	}

	return tlsConfig, nil
}

func loadConfiguration(path string) (*Configuration, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("opening config file: %w", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("decode config file: %w", err)
	}
	for _, old := range []string{"RottenDBConn", "LogicalID", "PhysicalID"} {
		if _, ok := raw[old]; ok {
			return nil, fmt.Errorf("%s is no longer supported; configure ServerURL, PassKeyFile, ServerCAFile, StateDir, and MaxSnapshotAge", old)
		}
	}
	var c Configuration
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("decode config file: %w", err)
	}
	if len(c.ObservedDBConn) == 0 || c.ObservedDBConn[0] == "" {
		return nil, fmt.Errorf("ObservedDBConn is required")
	}
	required := []struct {
		name  string
		value string
	}{
		{"ServerURL", c.ServerURL},
		{"PassKeyFile", c.PassKeyFile},
		{"ServerCAFile", c.ServerCAFile},
		{"StateDir", c.StateDir},
		{"SanityCheck", c.SanityCheck},
		{"FQDN", c.FQDN},
		{"Project", c.Project},
		{"Environment", c.Environment},
		{"Cluster", c.Cluster},
		{"Role", c.Role},
		{"ContextController", c.ContextController},
		{"ContextAction", c.ContextAction},
		{"ContextJob", c.ContextJob},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return nil, fmt.Errorf("%s is required", r.name)
		}
	}
	if c.ObservationInterval == 0 {
		return nil, fmt.Errorf("ObservationInterval is required")
	}
	if c.StatusInterval == 0 {
		return nil, fmt.Errorf("StatusInterval is required")
	}
	if c.MaxSnapshotAge == 0 {
		return nil, fmt.Errorf("MaxSnapshotAge is required")
	}
	return &c, nil
}

func compileRegexes(controller, action, job string) (c, a, j *regexp.Regexp, err error) {
	if c, err = regexp.Compile(controller); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextController: %w", err)
	}
	if a, err = regexp.Compile(action); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextAction: %w", err)
	}
	if j, err = regexp.Compile(job); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextJob: %w", err)
	}
	return c, a, j, nil
}

type outboxDrainer interface {
	Drain(context.Context) (int, error)
}

type outboxCounter interface {
	OutboxCounts(context.Context) (state.OutboxCounts, error)
}

type closeStore interface {
	Close() error
}

type sourceRegistrar interface {
	Register(context.Context, *rottenv1.RegisterRequest) (*rottenv1.RegisterResponse, error)
}

type sourceRegistrationStore interface {
	LoadSourceRegistration(context.Context) (state.SourceRegistration, bool, error)
	SaveSourceRegistration(context.Context, state.SourceRegistration) error
}

var exitProcess = os.Exit

var ErrSignalShutdown = errors.New("signal shutdown")

func registerSourceWithCache(startupCtx, backgroundCtx context.Context, client sourceRegistrar, store sourceRegistrationStore, serverURL string, req *rottenv1.RegisterRequest, logger *slog.Logger) (state.SourceRegistration, error) {
	if cached, ok, err := store.LoadSourceRegistration(startupCtx); err != nil {
		return state.SourceRegistration{}, fmt.Errorf("load cached source registration: %w", err)
	} else if ok && sourceRegistrationMatches(cached, serverURL, req) {
		if err := store.SaveSourceRegistration(startupCtx, cached); err != nil {
			return state.SourceRegistration{}, fmt.Errorf("reconcile cached source registration: %w", err)
		}
		go retryRegisterAndCache(backgroundCtx, client, store, serverURL, req, logger, cached, exitProcess)
		return cached, nil
	}
	return registerUntilSuccess(startupCtx, client, store, serverURL, req, logger)
}

func retryRegisterAndCache(ctx context.Context, client sourceRegistrar, store sourceRegistrationStore, serverURL string, req *rottenv1.RegisterRequest, logger *slog.Logger, inUse state.SourceRegistration, exit func(int)) {
	reg, err := registerUntilSuccess(ctx, client, store, serverURL, req, logger)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("source registration retry stopped", "err", err)
		return
	}
	if err == nil && (reg.LogicalSourceID != inUse.LogicalSourceID || reg.PhysicalSourceID != inUse.PhysicalSourceID) {
		logger.Error("source registration IDs changed; exiting so supervisor restarts with the fresh cache", "old_logical_source_id", inUse.LogicalSourceID, "old_physical_source_id", inUse.PhysicalSourceID, "new_logical_source_id", reg.LogicalSourceID, "new_physical_source_id", reg.PhysicalSourceID)
		exit(1)
	}
}

func registerUntilSuccess(ctx context.Context, client sourceRegistrar, store sourceRegistrationStore, serverURL string, req *rottenv1.RegisterRequest, logger *slog.Logger) (state.SourceRegistration, error) {
	attempt := 0
	for {
		resp, err := client.Register(ctx, req)
		if err == nil {
			reg := sourceRegistrationFrom(serverURL, req, resp)
			if err := store.SaveSourceRegistration(ctx, reg); err != nil {
				return state.SourceRegistration{}, fmt.Errorf("save source registration: %w", err)
			}
			return reg, nil
		}
		attempt++
		delay := jitteredOutboxBackoff(attempt)
		attrs := []any{"err", err, "attempt", attempt, "retry_in", delay.String()}
		if code := connect.CodeOf(err); code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
			logger.Error("source registration authentication or authorization failure; retrying", attrs...)
		} else {
			logger.Warn("source registration failed; retrying", attrs...)
		}
		if !sleepContext(ctx, delay) {
			return state.SourceRegistration{}, ctx.Err()
		}
	}
}

func sourceRegistrationFrom(serverURL string, req *rottenv1.RegisterRequest, resp *rottenv1.RegisterResponse) state.SourceRegistration {
	return state.SourceRegistration{
		ServerURL:        serverURL,
		Project:          req.GetProject(),
		Environment:      req.GetEnvironment(),
		Cluster:          req.GetCluster(),
		Role:             req.GetRole(),
		FQDN:             req.GetFqdn(),
		LogicalSourceID:  resp.GetLogicalSourceId(),
		PhysicalSourceID: resp.GetPhysicalSourceId(),
	}
}

func sourceRegistrationMatches(reg state.SourceRegistration, serverURL string, req *rottenv1.RegisterRequest) bool {
	return reg.ServerURL == serverURL &&
		reg.Project == req.GetProject() &&
		reg.Environment == req.GetEnvironment() &&
		reg.Cluster == req.GetCluster() &&
		reg.Role == req.GetRole() &&
		reg.FQDN == req.GetFqdn()
}

func runOutboxSender(ctx context.Context, sender outboxDrainer, counts outboxCounter, logger *slog.Logger) {
	consecutiveErrors := 0
	for {
		sent, err := sender.Drain(ctx)
		c, countErr := counts.OutboxCounts(ctx)
		if countErr != nil {
			logger.Error("worker outbox counts failed", "err", countErr)
		} else {
			logger.Info("worker outbox status", "queued", c.Queued, "dropped_cap", c.DroppedCap, "dropped_rejected", c.DroppedRejected, "dropped_stale_source", c.DroppedStaleSource, "sent", sent)
		}
		if err == nil {
			consecutiveErrors = 0
			if !sleepContext(ctx, 5*time.Second) {
				return
			}
			continue
		}
		consecutiveErrors++
		code := connect.CodeOf(err)
		delay := jitteredOutboxBackoff(consecutiveErrors)
		attrs := []any{"err", err, "code", code.String(), "consecutive_errors", consecutiveErrors, "retry_in", delay.String()}
		if code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
			logger.Error("worker outbox authentication or authorization failure; harvests are queued but cannot be delivered", attrs...)
		} else {
			logger.Warn("worker outbox drain failed; retrying", attrs...)
		}
		if !sleepContext(ctx, delay) {
			return
		}
	}
}

func jitteredOutboxBackoff(consecutiveErrors int) time.Duration {
	if consecutiveErrors < 1 {
		consecutiveErrors = 1
	}
	shift := consecutiveErrors - 1
	if shift > 6 {
		shift = 6
	}
	base := time.Second << shift
	if base > time.Minute {
		base = time.Minute
	}
	half := base / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func observedConnector(configuration *Configuration) (worker.ObservedConnector, error) {
	base, err := pgx.ParseConfig(configuration.ObservedDBConn[0])
	if err != nil {
		return nil, fmt.Errorf("couldn't create observedDBConfig: %w", err)
	}
	base.DefaultQueryExecMode = pgx.QueryExecModeExec
	if base.TLSConfig != nil && base.TLSConfig.RootCAs != nil {
		if *debugFlag {
			log.Printf("We seem to have a root CA for observed DB; remaking the chain to be sure to capture any intermediate certs.")
		}
		base.TLSConfig, err = remakeSSLCertConfig(configuration.ObservedDBConn[0], "")
		if err != nil {
			return nil, fmt.Errorf("couldn't remake observed db TLS config: %w", err)
		}
	}
	return func(ctx context.Context) (*pgx.Conn, error) {
		cfg := base.Copy()
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return pgx.ConnectConfig(connectCtx, cfg)
	}, nil
}

func signalContext(ctx context.Context, signals <-chan os.Signal, second func()) (context.Context, context.CancelFunc, <-chan struct{}, func() bool) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var consumed atomic.Bool
	go func() {
		defer close(done)
		select {
		case <-signals:
			consumed.Store(true)
			cancel()
			select {
			case <-signals:
				consumed.Store(true)
				if second != nil {
					second()
				}
			case <-ctx.Done():
			}
		case <-ctx.Done():
			select {
			case <-signals:
				consumed.Store(true)
			default:
			}
		}
	}()
	return ctx, cancel, done, consumed.Load
}

func gracefulWorkerShutdown(ctx context.Context, signals <-chan os.Signal, runDone <-chan error, requestStop func(), cancelRun func(), drainer outboxDrainer, counts outboxCounter, store closeStore, flushTimeout time.Duration, logger *slog.Logger) (bool, error) {
	var runErr error
	signalShutdown := false
	forced := false
	stopped := false
	stop := func() {
		if !stopped {
			requestStop()
			stopped = true
		}
	}
	handleSignal := func(sig os.Signal) {
		signalShutdown = true
		logger.Info("worker shutdown signal received", "signal", sig.String())
		stop()
		waitCtx, cancel := context.WithTimeout(ctx, flushTimeout)
		select {
		case runErr = <-runDone:
		case sig := <-signals:
			logger.Warn("second shutdown signal received; canceling worker immediately", "signal", sig.String())
			cancelRun()
			forced = true
			runErr = waitForRunAfterCancel(runDone, 100*time.Millisecond, context.Canceled)
		case <-waitCtx.Done():
			cancelRun()
			forced = true
			runErr = waitForRunAfterCancel(runDone, 100*time.Millisecond, context.DeadlineExceeded)
			if runErr == nil {
				runErr = waitCtx.Err()
			}
		}
		cancel()
	}
	select {
	case sig := <-signals:
		handleSignal(sig)
	default:
		select {
		case sig := <-signals:
			handleSignal(sig)
		case runErr = <-runDone:
		case <-ctx.Done():
			stop()
			runErr = ctx.Err()
		}
	}
	stop()
	var flushErr error
	if drainer != nil && !forced {
		flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		defer cancel()
		flushForced := make(chan os.Signal, 1)
		go func() {
			select {
			case sig := <-signals:
				logger.Warn("shutdown signal received during outbox flush; aborting flush", "signal", sig.String())
				flushForced <- sig
				cancel()
			case <-flushCtx.Done():
			}
		}()
		attempt := 0
		for flushCtx.Err() == nil {
			var sent int
			sent, flushErr = drainer.Drain(flushCtx)
			var c state.OutboxCounts
			var countErr error
			if counts != nil {
				c, countErr = counts.OutboxCounts(flushCtx)
			}
			if countErr == nil && c.Queued == 0 {
				flushErr = nil
				logger.Info("worker outbox flush complete", "sent", sent)
				break
			}
			attempt++
			delay := jitteredOutboxBackoff(attempt)
			if delay > time.Until(time.Now().Add(flushTimeout)) {
				delay = 100 * time.Millisecond
			}
			if !sleepContext(flushCtx, delay) {
				break
			}
		}
		select {
		case <-flushForced:
			forced = true
			flushErr = context.Canceled
		default:
		}
		if flushCtx.Err() != nil && counts != nil {
			if c, err := counts.OutboxCounts(context.Background()); err == nil {
				logger.Warn("worker outbox flush deadline reached; durable batches remain queued", "queued", c.Queued, "dropped_cap", c.DroppedCap, "dropped_rejected", c.DroppedRejected, "dropped_stale_source", c.DroppedStaleSource)
			}
		}
	}
	var closeErr error
	if store != nil {
		closeErr = store.Close()
	}
	if forced && runErr != nil {
		return signalShutdown, runErr
	}
	if forced && flushErr != nil {
		return signalShutdown, flushErr
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return signalShutdown, runErr
	}
	if flushErr != nil && !signalShutdown {
		return signalShutdown, fmt.Errorf("flush outbox: %w", flushErr)
	}
	if closeErr != nil {
		return signalShutdown, fmt.Errorf("close state store: %w", closeErr)
	}
	if signalShutdown {
		return true, nil
	}
	return false, nil
}

func waitForRunAfterCancel(runDone <-chan error, d time.Duration, fallback error) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-runDone:
		if err == nil {
			return context.Canceled
		}
		return err
	case <-timer.C:
		return fallback
	}
}

func shutdownExitCode(signalShutdown bool, err error) int {
	if err == nil || errors.Is(err, ErrSignalShutdown) {
		return 0
	}
	return 1
}

func main() {
	var cfg worker.Config

	flag.Parse()
	rootCtx := context.Background()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	startupCtx, startupCancel, startupSignalDone, startupSignalConsumed := signalContext(rootCtx, sigs, nil)
	defer startupCancel()

	if *configFileFlag == "" {
		log.Println("I need a config file!")
		os.Exit(1)
	}
	configuration, err := loadConfiguration(*configFileFlag)
	if err != nil {
		log.Println("config file:", err)
		os.Exit(1)
	}

	cfg.ObservedDBConnect, err = observedConnector(configuration)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	stateDir, maxAge := stateSettings(configuration)
	store, err := state.Open(stateDir, state.Options{MaxSnapshotAge: maxAge})
	if err != nil {
		log.Println("couldn't open the state store:", err)
		os.Exit(1)
	}
	if store.MovedAside != "" {
		log.Println("the state store was corrupt; moved it to", store.MovedAside, "and started fresh")
	}
	cfg.State = store
	cfg.ServerOutbox = store

	cfg.ObservationInterval = configuration.ObservationInterval
	cfg.SanityCheck = configuration.SanityCheck
	cfg.MinmaxResetSchema = configuration.MinmaxResetSchema
	if cfg.MinmaxResetSchema == "" {
		cfg.MinmaxResetSchema = pgss.DefaultMinmaxResetSchema
	}
	cfg.Fingerprint, err = fingerprint.NewOptions(configuration.KeepSchemas, configuration.CursorPattern, configuration.TempTablePattern)
	if err != nil {
		log.Println("bad fingerprint pattern in config:", err)
		os.Exit(1)
	}
	cfg.ReController, cfg.ReAction, cfg.ReJobTag, err = compileRegexes(configuration.ContextController, configuration.ContextAction, configuration.ContextJob)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	cfg.WatchdogExit = func(reason string) { os.Exit(1) }
	cfg.Logger = logger

	client, err := serverclient.New(serverclient.Config{
		ServerURL:   configuration.ServerURL,
		PassKeyFile: configuration.PassKeyFile,
		CAFile:      configuration.ServerCAFile,
		Logger:      logger,
	})
	if err != nil {
		log.Println("couldn't build server client:", err)
		os.Exit(1)
	}
	reg, err := registerSourceWithCache(startupCtx, rootCtx, client, store, configuration.ServerURL, &rottenv1.RegisterRequest{
		Project:     configuration.Project,
		Environment: configuration.Environment,
		Cluster:     configuration.Cluster,
		Role:        configuration.Role,
		Fqdn:        configuration.FQDN,
	}, logger)
	if err != nil {
		if startupCtx.Err() != nil {
			AppCleanup()
			os.Exit(0)
		}
		log.Println("couldn't register source with server:", err)
		os.Exit(1)
	}
	if startupCtx.Err() != nil {
		AppCleanup()
		os.Exit(0)
	}
	startupCancel()
	<-startupSignalDone
	if startupSignalConsumed() {
		AppCleanup()
		os.Exit(0)
	}
	cfg.LogicalID = reg.LogicalSourceID
	cfg.PhysicalID = reg.PhysicalSourceID

	w := worker.New(cfg, worker.RealClock{})

	sender := worker.NewOutboxSender(store, client, logger)
	runCtx, runCancel := context.WithCancel(rootCtx)
	defer runCancel()
	outboxCtx, outboxCancel := context.WithCancel(rootCtx)
	outboxDone := make(chan struct{})
	go func() {
		defer close(outboxDone)
		runOutboxSender(outboxCtx, sender, store, logger)
	}()

	progressCtx, progressCancel := context.WithCancel(rootCtx)
	go w.ReportProgress(progressCtx, *noIdleHandsFlag, configuration.StatusInterval)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()
	requestStop := func() {
		progressCancel()
		w.StopAfterCurrent()
		outboxCancel()
		<-outboxDone
	}
	signalShutdown, err := gracefulWorkerShutdown(rootCtx, sigs, runDone, requestStop, runCancel, sender, store, store, 10*time.Second, logger)
	if err != nil {
		log.Println("worker stopped with error:", err)
		AppCleanup()
		os.Exit(shutdownExitCode(signalShutdown, err))
	}
	if signalShutdown {
		AppCleanup()
		os.Exit(shutdownExitCode(true, ErrSignalShutdown))
	}
	AppCleanup()
}

func AppCleanup() {
	log.Println("...and that's all folks!")
	pprof.StopCPUProfile()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
