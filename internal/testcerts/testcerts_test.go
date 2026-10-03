package testcerts

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateIncludesRequestedHosts(t *testing.T) {
	dir := t.TempDir()

	if err := Generate(dir, "rotten-server", "localhost", "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	certPEM, err := os.ReadFile(filepath.Join(dir, "server.pem"))
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("server.pem has no PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"rotten-server", "localhost", "127.0.0.1"} {
		if err := cert.VerifyHostname(host); err != nil {
			t.Errorf("certificate missing host %s: %v", host, err)
		}
	}
}
