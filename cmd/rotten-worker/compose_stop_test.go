package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testcerts"
	"github.com/benchub/rotten/internal/testdb"
)

// TestComposeStopShutsDownWorkerCleanly runs dev/docker-compose.yaml's
// worker command against a server run from its compose command, with
// dev/worker.json's container paths and hosts pointed here, and does what
// `docker compose stop` does: SIGTERM to the process it started only. The
// worker must get the signal, log its graceful shutdown, and exit 0 within
// the grace period.
func TestComposeStopShutsDownWorkerCleanly(t *testing.T) {
	rotten := testdb.StartRotten(t)
	observed := testdb.StartObserved(t, 18)
	loadObserverSchemaInContainer(t, observed)

	dir := t.TempDir()
	certDir := filepath.Join(dir, "certs")
	if err := testcerts.Generate(certDir, "localhost", "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	server := testdb.StartDevComposeCommand(t, "server",
		[]string{"ROTTEN_SERVER_DSN=" + rotten.DSNAs(t, testdb.IngestRole)},
		"/certs/", certDir+"/",
		":8443", "127.0.0.1:0",
	)
	server.WaitForLog(t, "listening on https://", 4*time.Minute)
	serverAddr := ""
	for _, line := range strings.Split(server.Logs(), "\n") {
		if _, a, ok := strings.Cut(line, "listening on https://"); ok {
			serverAddr = strings.TrimSpace(a)
		}
	}

	raw, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "dev", "worker.json"))
	if err != nil {
		t.Fatal(err)
	}
	var conf map[string]any
	if err := json.Unmarshal(raw, &conf); err != nil {
		t.Fatal(err)
	}
	passFile := filepath.Join(dir, "pass.key")
	token := createE2EKey(t, rotten, conf["FQDN"].(string))
	if err := os.WriteFile(passFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf["ObservedDBConn"] = []string{observed.DSN}
	conf["ServerURL"] = "https://" + serverAddr
	conf["PassKeyFile"] = passFile
	conf["ServerCAFile"] = filepath.Join(certDir, "ca.pem")
	conf["StateDir"] = filepath.Join(dir, "state")
	conf["StatusInterval"] = 1
	conf["ObservationInterval"] = 2
	confFile := filepath.Join(dir, "worker.json")
	out, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(confFile, out, 0o600); err != nil {
		t.Fatal(err)
	}

	worker := testdb.StartDevComposeCommand(t, "worker", nil, "/src/dev/worker.json", confFile)
	// The first start may compile.
	worker.WaitForLog(t, "baseline harvest", 4*time.Minute)

	log := worker.ComposeStop(t)
	for _, want := range []string{`"msg":"worker shutdown signal received"`, "...and that's all folks!"} {
		if !strings.Contains(log, want) {
			t.Fatalf("no %q log line after SIGTERM:\n%s", want, log)
		}
	}
}
