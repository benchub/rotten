package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/state"
)

func TestStateSettings(t *testing.T) {
	dir, age := stateSettings(&Configuration{StateDir: "/var/lib/rotten-worker", MaxSnapshotAge: 60})
	if dir != "/var/lib/rotten-worker" || age != time.Minute {
		t.Errorf("settings = %q, %v; want /var/lib/rotten-worker, 1m", dir, age)
	}
}

func TestSampleConfLoads(t *testing.T) {
	c, err := loadConfiguration("../../conf")
	if err != nil {
		t.Fatal(err)
	}
	if c.ServerURL == "" || c.PassKeyFile == "" || c.ServerCAFile == "" {
		t.Fatalf("conf missing server settings: %+v", c)
	}
	if c.StateDir != "/var/lib/rotten-worker" || c.MaxSnapshotAge != 3*c.ObservationInterval {
		t.Errorf("conf StateDir %q, MaxSnapshotAge %d; want /var/lib/rotten-worker and three windows", c.StateDir, c.MaxSnapshotAge)
	}
}

func TestOldRottenDBConnFailsFast(t *testing.T) {
	for _, key := range []string{"RottenDBConn", "LogicalID", "PhysicalID"} {
		t.Run(key, func(t *testing.T) {
			path := writeConfig(t, map[string]string{key: `"old"`})
			_, err := loadConfiguration(path)
			if err == nil || !strings.Contains(err.Error(), key+" is no longer supported") {
				t.Fatalf("loadConfiguration err = %v, want clear %s failure", err, key)
			}
		})
	}
}

func TestLoadConfigurationMissingRequiredKeys(t *testing.T) {
	for _, key := range []string{
		"ObservedDBConn",
		"ServerURL",
		"PassKeyFile",
		"ServerCAFile",
		"StateDir",
		"MaxSnapshotAge",
		"StatusInterval",
		"ObservationInterval",
		"SanityCheck",
		"FQDN",
		"Project",
		"Environment",
		"Cluster",
		"Role",
		"ContextController",
		"ContextAction",
		"ContextJob",
	} {
		t.Run(key, func(t *testing.T) {
			path := writeConfigWithout(t, key)
			_, err := loadConfiguration(path)
			if err == nil || !strings.Contains(err.Error(), key+" is required") {
				t.Fatalf("loadConfiguration err = %v, want %s required", err, key)
			}
		})
	}
}

func writeConfigWithout(t *testing.T, missing string) string {
	t.Helper()
	return writeConfig(t, map[string]string{missing: ""})
}

func writeConfig(t *testing.T, overrides map[string]string) string {
	t.Helper()
	dir := filepath.Join(".test-artifacts", "config-"+strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "conf.json")
	fields := map[string]string{
		"ObservedDBConn":      `["postgres://observed"]`,
		"ServerURL":           `"https://server"`,
		"PassKeyFile":         `"pass"`,
		"ServerCAFile":        `"ca"`,
		"StateDir":            `"state"`,
		"MaxSnapshotAge":      `60`,
		"StatusInterval":      `1`,
		"ObservationInterval": `1`,
		"SanityCheck":         `"select true"`,
		"FQDN":                `"db"`,
		"Project":             `"p"`,
		"Environment":         `"e"`,
		"Cluster":             `"c"`,
		"Role":                `"r"`,
		"ContextController":   `"x"`,
		"ContextAction":       `"x"`,
		"ContextJob":          `"x"`,
	}
	for key, value := range overrides {
		if value == "" {
			delete(fields, key)
		} else {
			fields[key] = value
		}
	}
	var b strings.Builder
	b.WriteString("{")
	first := true
	for key, value := range fields {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(`"`)
		b.WriteString(key)
		b.WriteString(`":`)
		b.WriteString(value)
	}
	b.WriteString("}")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRegisterSourceWithCacheUsesCachedIDsWhenServerIsDown(t *testing.T) {
	store := openConfigStore(t)
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "r", Fqdn: "db"}
	want := state.SourceRegistration{
		ServerURL:        "https://server",
		Project:          "p",
		Environment:      "e",
		Cluster:          "c",
		Role:             "r",
		FQDN:             "db",
		LogicalSourceID:  7,
		PhysicalSourceID: 42,
	}
	if err := store.SaveSourceRegistration(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	client := &fakeRegistrar{err: errors.New("server down")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, err := registerSourceWithCache(ctx, client, store, "https://server", req, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("registration = %+v, want cached %+v", got, want)
	}
}

func TestRegisterSourceWithCacheReconcilesCachedOutboxBeforeStartup(t *testing.T) {
	store := openConfigStore(t)
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "r", Fqdn: "db"}
	cached := state.SourceRegistration{
		ServerURL:        "https://server",
		Project:          "p",
		Environment:      "e",
		Cluster:          "c",
		Role:             "r",
		FQDN:             "db",
		LogicalSourceID:  8,
		PhysicalSourceID: 43,
	}
	if err := store.SaveSourceRegistration(context.Background(), cached); err != nil {
		t.Fatal(err)
	}
	msg := configBatch(7, 42, 1)
	if _, err := store.EnqueueHarvest(context.Background(), msg, msg.GetWindowStart().AsTime()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeRegistrar{err: errors.New("server down")}
	got, err := registerSourceWithCache(ctx, client, store, "https://server", req, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got != cached {
		t.Fatalf("registration = %+v, want cached %+v", got, cached)
	}
	batch, err := store.NextOutboxBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || batch.BatchID != "43:1:2" {
		t.Fatalf("oldest batch = %+v, want re-stamped cached startup batch_id 43:1:2", batch)
	}
}

func TestRegisterSourceWithCacheIgnoresCacheOnIdentityMismatch(t *testing.T) {
	store := openConfigStore(t)
	if err := store.SaveSourceRegistration(context.Background(), state.SourceRegistration{
		ServerURL:        "https://server",
		Project:          "p",
		Environment:      "e",
		Cluster:          "c",
		Role:             "old",
		FQDN:             "db",
		LogicalSourceID:  7,
		PhysicalSourceID: 42,
	}); err != nil {
		t.Fatal(err)
	}
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "new", Fqdn: "db"}
	client := &fakeRegistrar{resp: &rottenv1.RegisterResponse{LogicalSourceId: 8, PhysicalSourceId: 43}}
	got, err := registerSourceWithCache(context.Background(), client, store, "https://server", req, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got.LogicalSourceID != 8 || got.PhysicalSourceID != 43 || client.calls != 1 {
		t.Fatalf("got %+v after %d calls, want blocking register with 8/43", got, client.calls)
	}
}

func TestRetryRegisterAndCacheExitsWhenServerIDsChange(t *testing.T) {
	store := openConfigStore(t)
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "r", Fqdn: "db"}
	inUse := state.SourceRegistration{
		ServerURL:        "https://server",
		Project:          "p",
		Environment:      "e",
		Cluster:          "c",
		Role:             "r",
		FQDN:             "db",
		LogicalSourceID:  7,
		PhysicalSourceID: 42,
	}
	client := &fakeRegistrar{resp: &rottenv1.RegisterResponse{LogicalSourceId: 8, PhysicalSourceId: 43}}
	exitCode := 0
	retryRegisterAndCache(context.Background(), client, store, "https://server", req, discardLogger(), inUse, func(code int) {
		exitCode = code
	})
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestRegisterUntilSuccessRetries(t *testing.T) {
	store := openConfigStore(t)
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "r", Fqdn: "db"}
	client := &fakeRegistrar{
		errs: []error{errors.New("server down"), nil},
		resp: &rottenv1.RegisterResponse{LogicalSourceId: 8, PhysicalSourceId: 43},
	}
	got, err := registerUntilSuccess(context.Background(), client, store, "https://server", req, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got.LogicalSourceID != 8 || got.PhysicalSourceID != 43 || client.calls != 2 {
		t.Fatalf("got %+v after %d calls, want 8/43 after 2 calls", got, client.calls)
	}
	cached, ok, err := store.LoadSourceRegistration(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || cached != got {
		t.Fatalf("cached = %+v ok %v, want %+v true", cached, ok, got)
	}
}

func TestRegisterUntilSuccessLogsAuthFailuresAtError(t *testing.T) {
	store := openConfigStore(t)
	req := &rottenv1.RegisterRequest{Project: "p", Environment: "e", Cluster: "c", Role: "r", Fqdn: "db"}
	client := &fakeRegistrar{err: connect.NewError(connect.CodePermissionDenied, errors.New("denied"))}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := registerUntilSuccess(ctx, client, store, "https://server", req, logger)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("log = %q, want ERROR", logs.String())
	}
}

func openConfigStore(t *testing.T) *state.Store {
	t.Helper()
	dir := filepath.Join(".test-artifacts", "registration-"+strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	store, err := state.Open(dir, state.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

func configBatch(logicalID, physicalID uint32, i int) *rottenv1.SubmitHarvestRequest {
	start := time.UnixMicro(int64(i)).UTC()
	end := time.UnixMicro(int64(i + 1)).UTC()
	return &rottenv1.SubmitHarvestRequest{
		BatchId:          fmt.Sprintf("%d:%d:%d", physicalID, start.UnixMicro(), end.UnixMicro()),
		LogicalSourceId:  logicalID,
		PhysicalSourceId: physicalID,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

type fakeRegistrar struct {
	resp  *rottenv1.RegisterResponse
	err   error
	errs  []error
	calls int
	reqs  []*rottenv1.RegisterRequest
}

func (f *fakeRegistrar) Register(_ context.Context, req *rottenv1.RegisterRequest) (*rottenv1.RegisterResponse, error) {
	f.calls++
	f.reqs = append(f.reqs, req)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return nil, err
		}
	} else if f.err != nil {
		return nil, f.err
	}
	if f.resp != nil {
		return f.resp, nil
	}
	return &rottenv1.RegisterResponse{LogicalSourceId: 1, PhysicalSourceId: 2}, nil
}
