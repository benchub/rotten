package main

import (
	"flag"
	"log"
	"strings"

	"github.com/benchub/rotten/internal/testcerts"
)

func main() {
	out := flag.String("out", "/certs", "directory for generated PEM files")
	hosts := flag.String("host", "rotten-server", "comma-separated server DNS names or IPs")
	flag.Parse()
	if err := testcerts.Generate(*out, splitHosts(*hosts)...); err != nil {
		log.Fatal(err)
	}
}

func splitHosts(hosts string) []string {
	var out []string
	for _, h := range strings.Split(hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}
