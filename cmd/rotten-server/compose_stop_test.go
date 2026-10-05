package main

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testcerts"
	"github.com/benchub/rotten/internal/testdb"
)

// TestComposeStopShutsDownServerCleanly runs dev/docker-compose.yaml's
// server command, with its /certs paths and listen address pointed here,
// and does what `docker compose stop` does: SIGTERM to the process it
// started only. The server must get the signal, log its shutdown, and exit
// 0 within the grace period.
func TestComposeStopShutsDownServerCleanly(t *testing.T) {
	db := testdb.StartRotten(t)
	certDir := t.TempDir()
	if err := testcerts.Generate(certDir, "localhost", "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	proc := testdb.StartDevComposeCommand(t, "server",
		[]string{"ROTTEN_SERVER_DSN=" + db.DSNAs(t, testdb.IngestRole)},
		"/certs/", certDir+"/",
		":8443", "127.0.0.1:0",
	)
	// The first start may compile.
	proc.WaitForLog(t, "listening on https://", 4*time.Minute)
	addr := ""
	for _, line := range strings.Split(proc.Logs(), "\n") {
		if _, a, ok := strings.Cut(line, "listening on https://"); ok {
			addr = strings.TrimSpace(a)
		}
	}
	healthz(t, addr, filepath.Join(certDir, "ca.pem"))

	log := proc.ComposeStop(t)
	if !strings.Contains(log, `"msg":"shutting down"`) {
		t.Fatalf("no shutdown log line after SIGTERM:\n%s", log)
	}
}

func healthz(t *testing.T, addr, caFile string) {
	t.Helper()
	pem, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Get("https://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", resp.StatusCode)
	}
}
