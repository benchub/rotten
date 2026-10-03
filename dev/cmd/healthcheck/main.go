package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	url := flag.String("url", "https://localhost:8443/healthz", "health endpoint")
	caPath := flag.String("ca", "/certs/ca.pem", "CA certificate PEM")
	flag.Parse()

	caPEM, err := os.ReadFile(*caPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		fmt.Fprintln(os.Stderr, "healthcheck: no CA certificates found")
		os.Exit(1)
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots},
		},
	}
	resp, err := client.Get(*url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, resp.Status)
		os.Exit(1)
	}
}
