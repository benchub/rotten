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

// With a root file holding intermediate + root, the chain sent is
// client, intermediate, root: the whole root file is appended after the
// client cert. That's the layout the code expects.
func TestRemakeSSLCertConfigBundleRootFile(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := remakeSSLCertConfig(f.connString("db1.example,db2.example,db3.example", "bundle.crt"), "")
	if err != nil {
		t.Fatal(err)
	}
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
	if cfg.ServerName != "db1.example" {
		t.Errorf("ServerName = %q, want first host db1.example", cfg.ServerName)
	}
	// RootCAs and ClientCAs are the same pool, holding both bundle certs.
	if cfg.RootCAs == nil || cfg.RootCAs != cfg.ClientCAs {
		t.Errorf("RootCAs and ClientCAs should be the same non-nil pool")
	}
	for _, c := range []*testCert{f.inter, f.root} {
		if _, err := c.cert.Verify(x509.VerifyOptions{Roots: cfg.RootCAs}); err != nil {
			t.Errorf("%s not trusted by RootCAs: %v", c.cert.Subject.CommonName, err)
		}
	}
	if cfg.MinVersion != tls.VersionTLS12 || cfg.MaxVersion != tls.VersionTLS13 {
		t.Errorf("versions = %x..%x, want TLS1.2..TLS1.3", cfg.MinVersion, cfg.MaxVersion)
	}
	wantSuites := []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	}
	if len(cfg.CipherSuites) != len(wantSuites) {
		t.Fatalf("CipherSuites = %v, want %v", cfg.CipherSuites, wantSuites)
	}
	for i := range wantSuites {
		if cfg.CipherSuites[i] != wantSuites[i] {
			t.Errorf("CipherSuites[%d] = %x, want %x", i, cfg.CipherSuites[i], wantSuites[i])
		}
	}
}

// With a root-only file, the intermediate never makes it into the chain.
// The code has no other source for intermediates.
func TestRemakeSSLCertConfigRootOnlyOmitsIntermediate(t *testing.T) {
	f := newTLSFixture(t)
	cfg, err := remakeSSLCertConfig(f.connString("db1.example", "root.crt"), "")
	if err != nil {
		t.Fatal(err)
	}
	got := chainDERs(t, cfg)
	if len(got) != 2 || !bytes.Equal(got[0], f.client.der) || !bytes.Equal(got[1], f.root.der) {
		t.Fatalf("chain = %d certs, want client then root", len(got))
	}
}

func TestRemakeSSLCertConfigServerName(t *testing.T) {
	f := newTLSFixture(t)
	cases := []struct {
		name, hosts, explicit, want string
	}{
		{"single host", "db1.example", "", "db1.example"},
		{"first of list", "db1.example,db2.example", "", "db1.example"},
		{"explicit fallback host wins", "db1.example,db2.example,db3.example", "db3.example", "db3.example"},
		{"explicit host not in list", "db1.example", "other.example", "other.example"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := remakeSSLCertConfig(f.connString(c.hosts, "bundle.crt"), c.explicit)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ServerName != c.want {
				t.Errorf("ServerName = %q, want %q", cfg.ServerName, c.want)
			}
		})
	}
	// No host= key at all: ServerName comes out empty.
	cfg, err := remakeSSLCertConfig("sslrootcert="+filepath.Join(f.dir, "bundle.crt")+
		" sslcert="+filepath.Join(f.dir, "client.crt")+
		" sslkey="+filepath.Join(f.dir, "client.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "" {
		t.Errorf("ServerName = %q, want empty", cfg.ServerName)
	}
}

func TestRemakeSSLCertConfigErrors(t *testing.T) {
	f := newTLSFixture(t)
	good := map[string]string{
		"sslrootcert": filepath.Join(f.dir, "bundle.crt"),
		"sslcert":     filepath.Join(f.dir, "client.crt"),
		"sslkey":      filepath.Join(f.dir, "client.key"),
	}
	build := func(override, value string) string {
		s := "host=db1.example"
		for _, k := range []string{"sslrootcert", "sslcert", "sslkey"} {
			v := good[k]
			if k == override {
				v = value
			}
			s += " " + k + "=" + v
		}
		return s
	}
	missing := filepath.Join(f.dir, "nope")
	garbage := filepath.Join(f.dir, "garbage.crt")
	cases := []struct{ name, key, value string }{
		{"missing root", "sslrootcert", missing},
		{"missing cert", "sslcert", missing},
		{"missing key", "sslkey", missing},
		{"unparseable root", "sslrootcert", garbage},
		{"unparseable key", "sslkey", garbage},
		{"key does not match", "sslkey", filepath.Join(f.dir, "other.key")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := remakeSSLCertConfig(build(c.key, c.value), "")
			if err == nil {
				t.Fatalf("want error, got config %v", cfg)
			}
			if cfg != nil {
				t.Errorf("want nil config on error")
			}
		})
	}
}
