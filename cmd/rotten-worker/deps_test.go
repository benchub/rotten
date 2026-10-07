package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestWorkerCommandDoesNotDependOnRottenDBCode(t *testing.T) {
	cmd := exec.Command("go", "list", "-mod=readonly", "-deps", "-f", "{{.ImportPath}}", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/jackc/pgx/v5/pgxpool" {
			t.Fatalf("cmd/rotten-worker must not depend on pgxpool; observed DB pgx is enough; deps:\n%s", out)
		}
		if strings.HasPrefix(dep, "github.com/benchub/rotten/") && !allowedWorkerDep(dep) {
			t.Fatalf("cmd/rotten-worker must not depend on rotten DB package %s; deps:\n%s", dep, out)
		}
	}
}

func allowedWorkerDep(dep string) bool {
	switch dep {
	case "github.com/benchub/rotten/cmd/rotten-worker",
		"github.com/benchub/rotten/gen/rotten/v1",
		"github.com/benchub/rotten/gen/rotten/v1/rottenv1connect",
		"github.com/benchub/rotten/internal/fingerprint",
		"github.com/benchub/rotten/internal/harvestlimits",
		"github.com/benchub/rotten/internal/pgss",
		"github.com/benchub/rotten/internal/pssc",
		"github.com/benchub/rotten/internal/serverclient",
		"github.com/benchub/rotten/internal/state",
		"github.com/benchub/rotten/internal/worker":
		return true
	default:
		return false
	}
}
