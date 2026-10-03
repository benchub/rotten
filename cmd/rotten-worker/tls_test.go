package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testCert struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func makeCert(t *testing.T, cn string, serial int64, isCA bool, parent *testCert) *testCert {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	signerCert, signerKey := tmpl, key
	if parent != nil {
		signerCert, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCert{cert: c, key: key, der: der}
}

func certPEM(cs ...*testCert) []byte {
	var b bytes.Buffer
	for _, c := range cs {
		pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: c.der})
	}
	return b.Bytes()
}

type tlsFixture struct {
	root, inter, client *testCert
	dir                 string
}

func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()
	root := makeCert(t, "test root", 1, true, nil)
	inter := makeCert(t, "test intermediate", 2, true, root)
	client := makeCert(t, "rotten client", 3, false, inter)
	dir := t.TempDir()
	keyDER, err := x509.MarshalECPrivateKey(client.key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("client.crt", certPEM(client))
	write("client.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	write("bundle.crt", certPEM(inter, root)) // intermediate + root
	write("root.crt", certPEM(root))          // root only
	write("garbage.crt", []byte("not a cert\n"))
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherDER, err := x509.MarshalECPrivateKey(other)
	if err != nil {
		t.Fatal(err)
	}
	write("other.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: otherDER}))
	return &tlsFixture{root: root, inter: inter, client: client, dir: dir}
}

func (f *tlsFixture) connString(host, rootFile string) string {
	return "host=" + host + " dbname=x sslmode=verify-full" +
		" sslrootcert=" + filepath.Join(f.dir, rootFile) +
		" sslcert=" + filepath.Join(f.dir, "client.crt") +
		" sslkey=" + filepath.Join(f.dir, "client.key")
}

func chainDERs(t *testing.T, cfg *tls.Config) [][]byte {
	t.Helper()
	if len(cfg.Certificates) != 1 {
		t.Fatalf("got %d certificates, want 1", len(cfg.Certificates))
	}
	return cfg.Certificates[0].Certificate
}

func assertRemadeChain(t *testing.T, cfg *tls.Config, f *tlsFixture) {
	t.Helper()
	got := chainDERs(t, cfg)
	want := [][]byte{f.client.der, f.inter.der, f.root.der}
	if len(got) != len(want) {
		t.Fatalf("chain length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("chain[%d] is not the expected cert", i)
		}
	}
}

func TestObservedConfigRemakesTLSForFallbackHosts(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{f.connString("db1.example,db2.example,db3.example", "bundle.crt")},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
	if got, want := cfg.Host, "db1.example"; got != want {
		t.Fatalf("primary Host = %q, want %q", got, want)
	}
	if len(cfg.Fallbacks) != 2 {
		t.Fatalf("fallback count = %d, want 2", len(cfg.Fallbacks))
	}
	for i, fallback := range cfg.Fallbacks {
		if fallback.TLSConfig == nil {
			t.Fatalf("fallback %d has nil TLSConfig", i)
		}
		assertRemadeChain(t, fallback.TLSConfig, f)
		if got, want := fallback.TLSConfig.ServerName, fallback.Host; got != want {
			t.Errorf("fallback %d ServerName = %q, want %q", i, got, want)
		}
	}
}

func TestObservedConfigAllowModeRemakesTLSFallback(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{"host=db1.example dbname=x sslmode=allow" +
			" sslrootcert=" + filepath.Join(f.dir, "bundle.crt") +
			" sslcert=" + filepath.Join(f.dir, "client.crt") +
			" sslkey=" + filepath.Join(f.dir, "client.key")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig != nil {
		t.Fatalf("sslmode=allow primary TLSConfig = %v, want nil", cfg.TLSConfig)
	}
	if len(cfg.Fallbacks) != 1 {
		t.Fatalf("fallback count = %d, want 1", len(cfg.Fallbacks))
	}
	assertRemadeChain(t, cfg.Fallbacks[0].TLSConfig, f)
}

func TestObservedConfigSkipsRootOnlyTLS(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{"host=db1.example dbname=x sslmode=verify-full" +
			" sslrootcert=" + filepath.Join(f.dir, "bundle.crt")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TLSConfig.Certificates) != 0 {
		t.Fatalf("certificate count = %d, want 0", len(cfg.TLSConfig.Certificates))
	}
}

func TestObservedConfigSkipsSystemRootCert(t *testing.T) {
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{"host=db1.example dbname=x sslmode=verify-full sslrootcert=system"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig == nil {
		t.Fatal("TLSConfig is nil")
	}
	if len(cfg.TLSConfig.Certificates) != 0 {
		t.Fatalf("certificate count = %d, want 0", len(cfg.TLSConfig.Certificates))
	}
}

func TestObservedConfigHonorsPGSSLROOTCERT(t *testing.T) {
	f := newTLSFixture(t)
	t.Setenv("PGSSLROOTCERT", filepath.Join(f.dir, "bundle.crt"))
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{"host=db1.example dbname=x sslmode=verify-full" +
			" sslcert=" + filepath.Join(f.dir, "client.crt") +
			" sslkey=" + filepath.Join(f.dir, "client.key")},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
}

func TestObservedConfigURLQuestionMarkInUserInfoDoesNotHideSSLRootCert(t *testing.T) {
	f := newTLSFixture(t)
	env := newTLSFixture(t)
	t.Setenv("PGSSLROOTCERT", filepath.Join(env.dir, "bundle.crt"))
	connString := "postgres://u:p?x@db1.example/observed?sslrootcert=" + filepath.Join(f.dir, "bundle.crt") +
		"&sslmode=verify-full" +
		"&sslcert=" + filepath.Join(f.dir, "client.crt") +
		"&sslkey=" + filepath.Join(f.dir, "client.key")
	cfg, err := observedConfig(&Configuration{ObservedDBConn: []string{connString}})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
}

func TestObservedConfigHonorsServiceSSLRootCert(t *testing.T) {
	f := newTLSFixture(t)
	serviceDir := t.TempDir()
	serviceFile := filepath.Join(serviceDir, "pg_service.conf")
	if err := os.WriteFile(serviceFile, []byte("[observed]\nsslrootcert="+filepath.Join(f.dir, "bundle.crt")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := observedConfig(&Configuration{
		ObservedDBConn: []string{"host=db1.example dbname=x sslmode=verify-full service=observed" +
			" servicefile=" + serviceFile +
			" sslcert=" + filepath.Join(f.dir, "client.crt") +
			" sslkey=" + filepath.Join(f.dir, "client.key")},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
}

func TestObservedConfigURLFormsPGCompatible(t *testing.T) {
	f := newTLSFixture(t)
	plusDir := filepath.Join(t.TempDir(), "cert+dir")
	if err := os.MkdirAll(plusDir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bundle.crt", "client.crt", "client.key"} {
		data, err := os.ReadFile(filepath.Join(f.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(plusDir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	connString := "postgres://[2001:db8::1]:5432,db2/observed?sslmode=verify-full" +
		"&application_name=a;b" +
		"&sslrootcert=" + filepath.Join(plusDir, "bundle.crt") +
		"&sslcert=" + filepath.Join(plusDir, "client.crt") +
		"&sslkey=" + filepath.Join(plusDir, "client.key")
	cfg, err := observedConfig(&Configuration{ObservedDBConn: []string{connString}})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
	if got, want := cfg.TLSConfig.ServerName, "2001:db8::1"; got != want {
		t.Errorf("primary ServerName = %q, want %q", got, want)
	}
	if len(cfg.Fallbacks) != 1 {
		t.Fatalf("fallback count = %d, want 1", len(cfg.Fallbacks))
	}
	assertRemadeChain(t, cfg.Fallbacks[0].TLSConfig, f)
	if got, want := cfg.Fallbacks[0].TLSConfig.ServerName, "db2"; got != want {
		t.Errorf("fallback ServerName = %q, want %q", got, want)
	}
}

func TestObservedConfigQuotedPathsWithSpaces(t *testing.T) {
	f := newTLSFixture(t)
	dir := filepath.Join(t.TempDir(), "cert dir=with spaces")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bundle.crt", "client.crt", "client.key"} {
		data, err := os.ReadFile(filepath.Join(f.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}

	connString := "host=db1.example dbname=observed sslmode=verify-full" +
		" sslrootcert='" + filepath.Join(dir, "bundle.crt") + "'" +
		" sslcert='" + filepath.Join(dir, "client.crt") + "'" +
		" sslkey='" + filepath.Join(dir, "client.key") + "'"
	cfg, err := observedConfig(&Configuration{ObservedDBConn: []string{connString}})
	if err != nil {
		t.Fatal(err)
	}
	assertRemadeChain(t, cfg.TLSConfig, f)
}
