package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/testdb"
)

func TestServeRequiresTLSFiles(t *testing.T) {
	for _, args := range [][]string{
		{"serve"},
		{"serve", "-tls-cert", "cert.pem"},
		{"serve", "-tls-key", "key.pem"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Setenv("ROTTEN_TLS_CERT", "")
			t.Setenv("ROTTEN_TLS_KEY", "")
			var out, errb bytes.Buffer
			if code := run(args, &out, &errb); code != 2 || !strings.Contains(errb.String(), "both -tls-cert and -tls-key") {
				t.Fatalf("code %d, stderr %q; want missing TLS files error", code, errb.String())
			}

		})
	}
}

func TestServeConfiguration(t *testing.T) {
	ca := newTestCA(t)
	cert, key := ca.pair(t, 1)
	certFile, keyFile := writePair(t, cert, key)
	t.Setenv("ROTTEN_TLS_CERT", certFile)
	t.Setenv("ROTTEN_TLS_KEY", keyFile)
	t.Setenv("ROTTEN_INGEST_DSN", "")
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"serve"}, 2, "no DSN"},
		{[]string{"serve", "extra"}, 2, "unexpected arguments"},
		{[]string{"serve", "-tls-cert", "missing"}, 1, "load TLS certificate"},
		{[]string{"serve", "-tls-cert", ""}, 2, "both -tls-cert and -tls-key"},
		{[]string{"serve", "-dsn", "not a DSN"}, 1, "database configuration"},
		{[]string{"serve", "-h"}, 2, "-tls-key"},
	} {
		var out, errb bytes.Buffer
		if code := run(tc.args, &out, &errb); code != tc.code || !strings.Contains(errb.String(), tc.want) {
			t.Errorf("%v: code %d, stderr %q; want %d, %q", tc.args, code, errb.String(), tc.code, tc.want)
		}
	}
}

func TestServeConfigFileEnvAndFlagPrecedence(t *testing.T) {
	file := filepath.Join(t.TempDir(), "server.json")
	b, err := json.Marshal(ServeFileConfig{
		DSN:                    "file-dsn",
		Listen:                 "file-listen",
		TLSCert:                "file-cert",
		TLSKey:                 "file-key",
		ShutdownTimeout:        20,
		HealthTimeout:          2,
		FailedAuthBurst:        4,
		FailedAuthRefill:       5,
		GlobalFailedAuthBurst:  40,
		GlobalFailedAuthRefill: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ROTTEN_SERVER_DSN", "env-dsn")
	t.Setenv("ROTTEN_SERVER_LISTEN", "env-listen")
	t.Setenv("ROTTEN_SERVER_TLS_CERT", "env-cert")
	t.Setenv("ROTTEN_SERVER_TLS_KEY", "env-key")
	t.Setenv("ROTTEN_SERVER_SHUTDOWN_TIMEOUT", "30")
	t.Setenv("ROTTEN_SERVER_HEALTH_TIMEOUT", "3")
	t.Setenv("ROTTEN_SERVER_FAILED_AUTH_BURST", "6")
	t.Setenv("ROTTEN_SERVER_FAILED_AUTH_REFILL", "7")
	t.Setenv("ROTTEN_SERVER_GLOBAL_FAILED_AUTH_BURST", "60")
	t.Setenv("ROTTEN_SERVER_GLOBAL_FAILED_AUTH_REFILL", "9")

	cfg, err := loadServeConfig([]string{
		"-config", file,
		"-listen", "flag-listen",
		"-shutdown-timeout", "40",
		"-failed-auth-burst", "8",
		"-global-failed-auth-refill", "11",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DSN != "env-dsn" || cfg.Listen != "flag-listen" || cfg.TLSCert != "env-cert" || cfg.TLSKey != "env-key" {
		t.Fatalf("cfg = %+v; want env values except flag listen", cfg)
	}
	if cfg.ShutdownTimeout != 40*time.Second || cfg.HealthTimeout != 3*time.Second {
		t.Fatalf("timeouts = %v, %v; want 40s flag and 3s env", cfg.ShutdownTimeout, cfg.HealthTimeout)
	}
	if cfg.FailedAuthBurst != 8 || cfg.FailedAuthRefill != 7*time.Second {
		t.Fatalf("failed auth limit = %d, %v; want 8 flag and 7s env", cfg.FailedAuthBurst, cfg.FailedAuthRefill)
	}
	if cfg.GlobalFailedAuthBurst != 60 || cfg.GlobalFailedAuthRefill != 11*time.Second {
		t.Fatalf("global failed auth limit = %d, %v; want 60 env and 11s flag", cfg.GlobalFailedAuthBurst, cfg.GlobalFailedAuthRefill)
	}
}

// serveFlagUsages runs `serve -h` and returns each flag's help text.
func serveFlagUsages(t *testing.T) map[string]string {
	t.Helper()
	var out, errb bytes.Buffer
	if code := run([]string{"serve", "-h"}, &out, &errb); code != 2 {
		t.Fatalf("serve -h: code %d, stderr %q", code, errb.String())
	}
	usages := make(map[string]string)
	var name string
	for _, line := range strings.Split(errb.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "  -"):
			name = strings.Fields(strings.TrimPrefix(trimmed, "-"))[0]
			usages[name] = ""
		case name != "" && trimmed != "":
			usages[name] = strings.TrimSpace(usages[name] + " " + trimmed)
		}
	}
	return usages
}

func TestServeHelpStatesPrecedence(t *testing.T) {
	usages := serveFlagUsages(t)
	for _, tc := range []struct{ flag, env, key, def string }{
		{"dsn", "ROTTEN_SERVER_DSN", "DSN", ""},
		{"listen", "ROTTEN_SERVER_LISTEN", "Listen", ":8443"},
		{"tls-cert", "ROTTEN_SERVER_TLS_CERT", "TLSCert", ""},
		{"tls-key", "ROTTEN_SERVER_TLS_KEY", "TLSKey", ""},
		{"shutdown-timeout", "ROTTEN_SERVER_SHUTDOWN_TIMEOUT", "ShutdownTimeout", "10"},
		{"health-timeout", "ROTTEN_SERVER_HEALTH_TIMEOUT", "HealthTimeout", "1"},
		{"failed-auth-burst", "ROTTEN_SERVER_FAILED_AUTH_BURST", "FailedAuthBurst", "5"},
		{"failed-auth-refill", "ROTTEN_SERVER_FAILED_AUTH_REFILL", "FailedAuthRefill", "10"},
		{"global-failed-auth-burst", "ROTTEN_SERVER_GLOBAL_FAILED_AUTH_BURST", "GlobalFailedAuthBurst", "50"},
		{"global-failed-auth-refill", "ROTTEN_SERVER_GLOBAL_FAILED_AUTH_REFILL", "GlobalFailedAuthRefill", "1"},
	} {
		usage, ok := usages[tc.flag]
		if !ok {
			t.Errorf("-%s missing from serve -h output", tc.flag)
			continue
		}
		want := "(default $" + tc.env + ", then config " + tc.key
		if tc.def != "" {
			want += ", else " + tc.def
		}
		want += ")"
		if !strings.HasSuffix(usage, want) {
			t.Errorf("-%s help %q; want it to end with %q (flag, then env, then config file, then default)", tc.flag, usage, want)
		}
		delete(usages, tc.flag)
	}
	delete(usages, "config")
	for flag, usage := range usages {
		t.Errorf("-%s help %q not checked for precedence", flag, usage)
	}
}

// The test binary is also the CLI subprocess, so signals exercise the actual
// entry point without installing or building another binary.
func TestServeProcess(t *testing.T) {
	if os.Getenv("ROTTEN_TEST_SERVE_PROCESS") != "1" {
		return
	}
	os.Exit(run([]string{"serve", "-listen", "127.0.0.1:0"}, os.Stdout, os.Stderr))
}

type testCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(100), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert, key}
}

func (ca testCA) pair(t *testing.T, serial int64) ([]byte, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv})
}

func (ca testCA) config() *tls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	return &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13}
}

func writePair(t *testing.T, cert, key []byte) (string, string) {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeTLSFile(t, certFile, cert)
	writeTLSFile(t, keyFile, key)
	return certFile, keyFile
}

func writeTLSFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

type serveLogs struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *serveLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *serveLogs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func eventually(t *testing.T, check func() bool, detail func() string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", detail())
}

func logsNeverReady() string { return "health never reported unhealthy" }

type serveProcess struct {
	addr   string
	cmd    *exec.Cmd
	logs   *serveLogs
	done   chan error
	mu     sync.Mutex
	exited bool
}

func (p *serveProcess) markExited() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exited = true
}

func (p *serveProcess) hasExited() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exited
}

func (p *serveProcess) wait(timeout time.Duration) (error, bool) {
	select {
	case err := <-p.done:
		p.markExited()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func startServe(t *testing.T, dsn, certFile, keyFile string, extraEnv ...string) *serveProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServeProcess$")
	cmd.Env = append(os.Environ(), "ROTTEN_TEST_SERVE_PROCESS=1",
		"ROTTEN_INGEST_DSN="+dsn, "ROTTEN_TLS_CERT="+certFile, "ROTTEN_TLS_KEY="+keyFile)
	cmd.Env = append(cmd.Env, extraEnv...)
	logs := &serveLogs{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	proc := &serveProcess{cmd: cmd, logs: logs, done: done}
	t.Cleanup(func() {
		if proc.hasExited() {
			return
		}
		select {
		case err := <-done:
			proc.markExited()
			t.Errorf("server exited early: %v\n%s", err, logs.String())
			return
		default:
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("terminate server: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server exit: %v\n%s", err, logs.String())
			}
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("server did not stop")
		}
	})
	var addr string
	eventually(t, func() bool {
		select {
		case err := <-done:
			proc.markExited()
			t.Fatalf("server exited before listening: %v\n%s", err, logs.String())
		default:
		}
		for _, line := range strings.Split(logs.String(), "\n") {
			if _, suffix, ok := strings.Cut(line, "listening on https://"); ok {
				addr = strings.TrimSpace(suffix)
				return true
			}
		}
		return false
	}, logs.String)
	proc.addr = addr
	return proc
}

func serveHarvestRequest(logical, physical uint32, start time.Time, suffix string) *connect.Request[rottenv1.SubmitHarvestRequest] {
	end := start.Add(30 * time.Second)
	msg := &rottenv1.SubmitHarvestRequest{
		BatchId:          fmt.Sprintf("%d:%d:%d", physical, start.UnixMicro(), end.UnixMicro()),
		LogicalSourceId:  logical,
		PhysicalSourceId: physical,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates: []*rottenv1.FingerprintAggregate{{
			Fingerprint: "serve-" + suffix,
			Normalized:  "select $1",
			Contexts:    []*rottenv1.QueryContext{{Controller: "serve", Action: "drain", Count: 1}},
			Metrics:     &rottenv1.Metrics{Calls: 1, TotalTime: 2},
		}},
	}
	return connect.NewRequest(msg)
}

func dialTLS(addr string, config *tls.Config) (*tls.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, config)
}

func assertSerial(t *testing.T, addr string, config *tls.Config, serial int64) {
	t.Helper()
	c, err := dialTLS(addr, config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.ConnectionState().PeerCertificates[0].SerialNumber.Int64(); got != serial {
		t.Fatalf("certificate serial %d, want %d", got, serial)
	}
}

func keepAliveRequest(t *testing.T, conn *tls.Conn, reader *bufio.Reader) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound || resp.Close {
		t.Fatalf("response %s, close=%v", resp.Status, resp.Close)
	}
}

func TestServeHealthChecksDatabase(t *testing.T) {
	db := testdb.StartRotten(t)
	ca := newTestCA(t)
	cert, priv := ca.pair(t, 1)
	certFile, keyFile := writePair(t, cert, priv)
	proc := startServe(t, db.DSNAs(t, testdb.IngestRole), certFile, keyFile)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: ca.config()}, Timeout: 3 * time.Second}

	resp, err := client.Get("https://" + proc.addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("healthy response %s %q; want 200 ok", resp.Status, body)
	}

	db.Stop(t)
	eventually(t, func() bool {
		resp, err := client.Get("https://" + proc.addr + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "postgres") || strings.Contains(string(body), db.DSN) {
			t.Fatalf("unhealthy response leaked database detail: %q", body)
		}
		return resp.StatusCode == http.StatusServiceUnavailable && string(body) == "unhealthy\n"
	}, logsNeverReady)
}

func TestServeSIGTERMDuringHarvestDrains(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	key, err := auth.CreateKey(ctx, owner, "drain-test", "drain.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	cert, priv := ca.pair(t, 1)
	certFile, keyFile := writePair(t, cert, priv)
	proc := startServe(t, db.DSNAs(t, testdb.IngestRole), certFile, keyFile)

	tr := &http.Transport{TLSClientConfig: ca.config(), ForceAttemptHTTP2: true}
	defer tr.CloseIdleConnections()
	api := rottenv1connect.NewIngestServiceClient(&http.Client{Transport: tr}, "https://"+proc.addr)
	regReq := connect.NewRequest(&rottenv1.RegisterRequest{
		Project: "drain", Environment: "test", Cluster: "cluster", Role: "primary", Fqdn: "drain.example",
	})
	regReq.Header().Set("Authorization", "Bearer "+key.Token)
	reg, err := api.Register(ctx, regReq)
	if err != nil {
		t.Fatal(err)
	}

	ingestPool, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	defer ingestPool.Close()
	lockConn, err := ingestPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	defer lockConn.Exec(context.Background(), "rollback")
	if _, err := lockConn.Exec(ctx, "select pg_advisory_xact_lock($1::bigint)", int64(reg.Msg.GetPhysicalSourceId())); err != nil {
		t.Fatal(err)
	}

	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	harvest := serveHarvestRequest(reg.Msg.GetLogicalSourceId(), reg.Msg.GetPhysicalSourceId(), start, "drain")
	harvest.Header().Set("Authorization", "Bearer "+key.Token)
	done := make(chan error, 1)
	go func() {
		_, err := api.SubmitHarvest(context.Background(), harvest)
		done <- err
	}()
	waitForWaitingAdvisoryLock(t, owner)
	if err := proc.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, proc.logs, "shutting down")
	if _, err := lockConn.Exec(context.Background(), "commit"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("in-flight harvest did not drain cleanly: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight harvest did not finish")
	}

	var count int
	if err := owner.QueryRow(ctx, "select count(*) from rotten.ingested_batches where batch_id = $1", harvest.Msg.GetBatchId()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("ingested_batches count = %d, want committed exactly once", count)
	}
	if err, ok := proc.wait(10 * time.Second); !ok || err != nil {
		t.Fatalf("server exit after drained shutdown = %v, ok=%v\n%s", err, ok, proc.logs.String())
	}
}

func TestServeShutdownTimeoutCancelsStuckHarvest(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	key, err := auth.CreateKey(ctx, owner, "timeout-test", "timeout.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	cert, priv := ca.pair(t, 1)
	certFile, keyFile := writePair(t, cert, priv)
	proc := startServe(t, db.DSNAs(t, testdb.IngestRole), certFile, keyFile, "ROTTEN_SERVER_SHUTDOWN_TIMEOUT=1")

	tr := &http.Transport{TLSClientConfig: ca.config(), ForceAttemptHTTP2: true}
	defer tr.CloseIdleConnections()
	api := rottenv1connect.NewIngestServiceClient(&http.Client{Transport: tr}, "https://"+proc.addr)
	regReq := connect.NewRequest(&rottenv1.RegisterRequest{
		Project: "timeout", Environment: "test", Cluster: "cluster", Role: "primary", Fqdn: "timeout.example",
	})
	regReq.Header().Set("Authorization", "Bearer "+key.Token)
	reg, err := api.Register(ctx, regReq)
	if err != nil {
		t.Fatal(err)
	}

	ingestPool, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	defer ingestPool.Close()
	lockConn, err := ingestPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	defer lockConn.Exec(context.Background(), "rollback")
	if _, err := lockConn.Exec(ctx, "select pg_advisory_xact_lock($1::bigint)", int64(reg.Msg.GetPhysicalSourceId())); err != nil {
		t.Fatal(err)
	}

	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	harvest := serveHarvestRequest(reg.Msg.GetLogicalSourceId(), reg.Msg.GetPhysicalSourceId(), start, "timeout")
	harvest.Header().Set("Authorization", "Bearer "+key.Token)
	done := make(chan error, 1)
	go func() {
		_, err := api.SubmitHarvest(context.Background(), harvest)
		done <- err
	}()
	waitForWaitingAdvisoryLock(t, owner)
	if err := proc.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, proc.logs, "shutting down")
	if err, ok := proc.wait(5 * time.Second); !ok {
		t.Fatalf("server did not exit after shutdown timeout\n%s", proc.logs.String())
	} else if err == nil {
		t.Fatalf("server exit was nil; want nonzero timeout exit\n%s", proc.logs.String())
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stuck harvest call did not return after server exit")
	}
	var count int
	if err := owner.QueryRow(ctx, "select count(*) from rotten.ingested_batches where batch_id = $1", harvest.Msg.GetBatchId()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("ingested_batches count = %d, want rollback/no commit", count)
	}
}

func waitForWaitingAdvisoryLock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	eventually(t, func() bool {
		var count int
		if err := pool.QueryRow(context.Background(), "select count(*) from pg_locks where locktype = 'advisory' and granted = false").Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count > 0
	}, func() string { return "no backend waited on advisory lock" })
}

func waitForLog(t *testing.T, logs *serveLogs, substr string) {
	t.Helper()
	eventually(t, func() bool {
		return strings.Contains(logs.String(), substr)
	}, logs.String)
}

func TestServeTLS(t *testing.T) {
	db := testdb.StartRotten(t)
	owner, err := pgxpool.New(context.Background(), db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	key, err := auth.CreateKey(context.Background(), owner, "tls-test", "db.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	cert, priv := ca.pair(t, 1)
	certFile, keyFile := writePair(t, cert, priv)
	proc := startServe(t, db.DSNAs(t, testdb.IngestRole), certFile, keyFile)
	addr := proc.addr
	assertSerial(t, addr, ca.config(), 1)

	t.Run("plaintext refused", func(t *testing.T) {
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("HTTP request to an HTTPS server")) {
			t.Fatalf("plaintext status %s, body %q", resp.Status, body)
		}
	})
	t.Run("wrong CA refused", func(t *testing.T) {
		c, err := dialTLS(addr, newTestCA(t).config())
		if err == nil {
			c.Close()
			t.Fatal("accepted a certificate signed by an untrusted CA")
		}
		var unknownCA x509.UnknownAuthorityError
		if !errors.As(err, &unknownCA) {
			t.Fatalf("want an untrusted CA failure, got %v", err)
		}
	})
	t.Run("TLS12 refused", func(t *testing.T) {
		conf := ca.config()
		conf.MinVersion, conf.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
		c, err := dialTLS(addr, conf)
		if err == nil {
			c.Close()
			t.Fatal("accepted TLS 1.2")
		}
		if !strings.Contains(err.Error(), "protocol version") {
			t.Fatalf("want protocol version rejection, got %v", err)
		}
	})
	for _, grpc := range []bool{false, true} {
		t.Run(fmt.Sprintf("authenticated API grpc=%v", grpc), func(t *testing.T) {
			tr := &http.Transport{TLSClientConfig: ca.config(), ForceAttemptHTTP2: true}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 3 * time.Second}
			var opts []connect.ClientOption
			if grpc {
				opts = append(opts, connect.WithGRPC())
			}
			api := rottenv1connect.NewIngestServiceClient(client, "https://"+addr, opts...)
			_, err := api.Register(context.Background(), connect.NewRequest(&rottenv1.RegisterRequest{}))
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("missing bearer key: %v", err)
			}
			req := connect.NewRequest(&rottenv1.RegisterRequest{
				Project:     "tls",
				Environment: "test",
				Cluster:     "cluster",
				Role:        "primary",
				Fqdn:        "db.example",
			})
			req.Header().Set("Authorization", "Bearer "+key.Token)
			resp, err := api.Register(context.Background(), req)
			if err != nil {
				t.Fatalf("valid key Register: %v", err)
			}
			if resp.Msg.GetLogicalSourceId() == 0 || resp.Msg.GetPhysicalSourceId() == 0 {
				t.Fatalf("Register returned zero IDs: %v", resp.Msg)
			}
			if !grpc {
				start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
				end := start.Add(30 * time.Second)
				harvest := connect.NewRequest(&rottenv1.SubmitHarvestRequest{
					BatchId:          fmt.Sprintf("%d:%d:%d", resp.Msg.GetPhysicalSourceId(), start.UnixMicro(), end.UnixMicro()),
					LogicalSourceId:  resp.Msg.GetLogicalSourceId(),
					PhysicalSourceId: resp.Msg.GetPhysicalSourceId(),
					WindowStart:      timestamppb.New(start),
					WindowEnd:        timestamppb.New(end),
					Aggregates: []*rottenv1.FingerprintAggregate{{
						Fingerprint: "serve-fingerprint",
						Normalized:  "select $1",
						Contexts:    []*rottenv1.QueryContext{{Controller: "serve", Action: "show", Count: 1}},
						Metrics:     &rottenv1.Metrics{Calls: 1, TotalTime: 2},
					}},
				})
				harvest.Header().Set("Authorization", "Bearer "+key.Token)
				got, err := api.SubmitHarvest(context.Background(), harvest)
				if err != nil {
					t.Fatalf("valid key SubmitHarvest: %v", err)
				}
				if got.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
					t.Fatalf("SubmitHarvest status = %v, want ACCEPTED", got.Msg.GetStatus())
				}
			}
		})
	}
}

func TestServeTLSRotation(t *testing.T) {
	db := testdb.StartRotten(t)
	for _, mode := range []string{"file changes", "SIGHUP"} {
		t.Run(mode, func(t *testing.T) {
			ca := newTestCA(t)
			cert1, key1 := ca.pair(t, 1)
			cert2, key2 := ca.pair(t, 2)
			certFile, keyFile := writePair(t, cert1, key1)
			proc := startServe(t, db.DSNAs(t, testdb.IngestRole), certFile, keyFile)
			addr, cmd, logs := proc.addr, proc.cmd, proc.logs
			conf := ca.config()
			conf.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			old, err := dialTLS(addr, conf)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			reader := bufio.NewReader(old)
			keepAliveRequest(t, old, reader)
			trigger := func() {
				if mode == "SIGHUP" {
					if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "SIGHUP" {
				// With no file changes, polling cannot produce this acknowledgement.
				trigger()
				eventually(t, func() bool { return strings.Contains(logs.String(), "reloaded TLS certificate on SIGHUP") }, logs.String)
				assertSerial(t, addr, conf, 1)
			}
			// An incomplete rotation must never replace the last good pair.
			writeTLSFile(t, certFile, cert2)
			trigger()
			eventually(t, func() bool { return strings.Contains(logs.String(), "reload TLS certificate") }, logs.String)
			assertSerial(t, addr, ca.config(), 1)
			keepAliveRequest(t, old, reader)

			// Rename replacement catches common atomic/symlink-style deployment;
			// preserving timestamps ensures detection isn't metadata-only.
			info, err := os.Stat(keyFile)
			if err != nil {
				t.Fatal(err)
			}
			replacement := keyFile + ".new"
			writeTLSFile(t, replacement, key2)
			if err := os.Chtimes(replacement, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(replacement, keyFile); err != nil {
				t.Fatal(err)
			}
			trigger()
			eventually(t, func() bool {
				c, err := dialTLS(addr, conf)
				if err != nil {
					return false
				}
				defer c.Close()
				state := c.ConnectionState()
				return !state.DidResume && state.PeerCertificates[0].SerialNumber.Int64() == 2
			}, logs.String)
			keepAliveRequest(t, old, reader)
			if got := old.ConnectionState().PeerCertificates[0].SerialNumber.Int64(); got != 1 {
				t.Fatalf("existing connection's certificate changed: %d", got)
			}
			if err := os.Remove(certFile); err != nil {
				t.Fatal(err)
			}
			trigger()
			eventually(t, func() bool { return strings.Contains(logs.String(), "no such file") }, logs.String)
			assertSerial(t, addr, ca.config(), 2)
			writeTLSFile(t, certFile, []byte("not a certificate"))
			trigger()
			eventually(t, func() bool { return strings.Contains(logs.String(), "failed to find any PEM data") }, logs.String)
			assertSerial(t, addr, conf, 2)
			writeTLSFile(t, certFile, cert1)
			writeTLSFile(t, keyFile, key1)
			trigger()
			eventually(t, func() bool {
				c, err := dialTLS(addr, ca.config())
				if err != nil {
					return false
				}
				defer c.Close()
				return c.ConnectionState().PeerCertificates[0].SerialNumber.Int64() == 1
			}, logs.String)
		})
	}
}
func TestServeInvalidTLSFailsBeforeDatabase(t *testing.T) {
	t.Setenv("ROTTEN_INGEST_DSN", "not a DSN")
	var out, errb bytes.Buffer
	code := run([]string{"serve", "-tls-cert", "missing.crt", "-tls-key", "missing.key"}, &out, &errb)
	if code != 1 || !strings.Contains(errb.String(), "load TLS certificate") {
		t.Fatalf("code %d, stderr %q; want certificate loading failure", code, errb.String())
	}
}
