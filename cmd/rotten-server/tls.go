package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

type serverCertificate struct {
	certFile, keyFile string
	current           atomic.Pointer[tls.Certificate]
	digest            [2][sha256.Size]byte
}

func loadServerCertificate(certFile, keyFile string) (*serverCertificate, error) {
	c := &serverCertificate{certFile: certFile, keyFile: keyFile}
	if err := c.reload(true); err != nil {
		return nil, err
	}
	return c, nil
}

// Only the reload loop writes digest; handshakes read the immutable pair
// through current. A partial file rotation cannot publish a mismatched pair.
func (c *serverCertificate) reload(force bool) error {
	certPEM, err := os.ReadFile(c.certFile)
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(c.keyFile)
	if err != nil {
		return err
	}
	digest := [2][sha256.Size]byte{sha256.Sum256(certPEM), sha256.Sum256(keyPEM)}
	if !force && digest == c.digest {
		return nil
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	c.current.Store(&pair)
	c.digest = digest
	return nil
}

func (c *serverCertificate) config() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// Disable resumption so every new connection gets the current
		// certificate, even when the client cached a session before rotation.
		SessionTicketsDisabled: true,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			pair := c.current.Load()
			if pair == nil {
				return nil, fmt.Errorf("no TLS certificate loaded")
			}
			return pair, nil
		},
	}
}

func (c *serverCertificate) watch(ctx context.Context, hup <-chan os.Signal, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		force := false
		select {
		case <-ctx.Done():
			return
		case <-hup:
			force = true
		case <-ticker.C:
		}
		if err := c.reload(force); err != nil {
			logger.Error("reload TLS certificate: keeping last good pair", "error", err)
		} else if force {
			logger.Info("reloaded TLS certificate on SIGHUP")
		}
	}
}
