package testdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moby/moby/api/types/network"
)

func TestRetryableConnectError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		// A unique superuser password per container means an auth failure is
		// a different container (or a stale port forward) answering.
		{"password auth failed", &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: `password authentication failed for user "postgres"`}, true},
		{"SASL protocol violation", fmt.Errorf("failed SASL auth: %w", &pgconn.PgError{Severity: "FATAL", Code: "08P01", Message: "SASL authentication failed"}), true},
		{"starting up", &pgconn.PgError{Severity: "FATAL", Code: "57P03", Message: "the database system is starting up"}, true},
		{"admin shutdown", &pgconn.PgError{Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"}, true},
		{"unexpected EOF", fmt.Errorf("failed to receive message: %w", io.ErrUnexpectedEOF), true},
		{"EOF", fmt.Errorf("failed to receive message: %w", io.EOF), true},
		{"flattened EOF", errors.New("failed to connect to `user=postgres database=rotten`: 192.168.65.254:57963 (host.docker.internal): failed to receive message: unexpected EOF"), true},
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true},
		{"reset", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, true},
		{"unreachable", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ENETUNREACH)}, true},
		{"missing database", &pgconn.PgError{Severity: "FATAL", Code: "3D000", Message: `database "rotten" does not exist`}, false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"other", errors.New("cannot parse DSN"), false},
		{"nil", nil, false},
	} {
		if got := retryableConnectError(tc.err); got != tc.want {
			t.Errorf("%s: retryableConnectError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// portSequenceResolver returns ports in order, repeating the last.
type portSequenceResolver struct {
	ports []string
	calls int
}

func (r *portSequenceResolver) Host(context.Context) (string, error) {
	return "host.docker.internal", nil
}

func (r *portSequenceResolver) MappedPort(context.Context, string) (network.Port, error) {
	i := r.calls
	if i >= len(r.ports) {
		i = len(r.ports) - 1
	}
	r.calls++
	return network.MustParsePort(r.ports[i] + "/tcp"), nil
}

func TestConnectVerifiedRetriesWrongContainerAndRereadsPort(t *testing.T) {
	resolver := &portSequenceResolver{ports: []string{"57962", "57963"}}
	var dialed []string
	connect := func(_ context.Context, dsn string) error {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse %q: %v", dsn, err)
		}
		if pw, _ := u.User.Password(); pw != "s3cret" {
			t.Fatalf("DSN password = %q, want the container's password", pw)
		}
		dialed = append(dialed, u.Port())
		if u.Port() == "57962" {
			return fmt.Errorf("failed SASL auth: %w", &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"})
		}
		return nil
	}

	dsn, err := connectVerified(context.Background(), resolver, "rotten", "s3cret", time.Second, 0, connect)
	if err != nil {
		t.Fatalf("connectVerified: %v", err)
	}
	if !strings.Contains(dsn, ":57963/") {
		t.Fatalf("DSN = %q, want the re-read port 57963", dsn)
	}
	if strings.Join(dialed, ",") != "57962,57963" {
		t.Fatalf("dialed ports = %v, want 57962 then 57963", dialed)
	}
}

func TestConnectVerifiedStopsOnNonRetryableError(t *testing.T) {
	resolver := &portSequenceResolver{ports: []string{"57962"}}
	attempts := 0
	missing := &pgconn.PgError{Severity: "FATAL", Code: "3D000", Message: "database does not exist"}
	_, err := connectVerified(context.Background(), resolver, "rotten", "s3cret", time.Second, 0, func(context.Context, string) error {
		attempts++
		return missing
	})
	if !errors.Is(err, missing) {
		t.Fatalf("err = %v, want %v", err, missing)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestConnectVerifiedIsBounded(t *testing.T) {
	resolver := &portSequenceResolver{ports: []string{"57962"}}
	authFailed := &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"}
	start := time.Now()
	_, err := connectVerified(context.Background(), resolver, "rotten", "s3cret", 200*time.Millisecond, 10*time.Millisecond, func(context.Context, string) error {
		return authFailed
	})
	if !errors.Is(err, authFailed) {
		t.Fatalf("err = %v, want the last auth failure", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("connectVerified took %s, want it bounded by its timeout", elapsed)
	}
}

func TestSuperuserPasswordsAreUnique(t *testing.T) {
	a, b := newSuperuserPassword(), newSuperuserPassword()
	if a == b || a == "postgres" || len(a) < 16 {
		t.Fatalf("passwords %q and %q: want distinct, random, not %q", a, b, "postgres")
	}
}

func TestRolePasswordsArePerContainer(t *testing.T) {
	a := &DB{DSN: "postgres://postgres:pa@host.docker.internal:1/rotten?sslmode=disable", password: "pa", secret: "s1"}
	b := &DB{DSN: "postgres://postgres:pb@host.docker.internal:2/rotten?sslmode=disable", password: "pb", secret: "s2"}
	for _, role := range []string{OwnerRole, IngestRole, "rotten_observer"} {
		pa, pb := a.RolePassword(role), b.RolePassword(role)
		if pa == role || pb == role || pa == pb {
			t.Errorf("%s: passwords %q and %q, want distinct per container and not the role name", role, pa, pb)
		}
		u, err := url.Parse(a.DSNAs(t, role))
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := u.User.Password(); got != pa {
			t.Errorf("DSNAs(%s) password = %q, want RolePassword %q", role, got, pa)
		}
	}
	if a.RolePassword("postgres") != "pa" {
		t.Errorf("RolePassword(postgres) = %q, want the superuser password", a.RolePassword("postgres"))
	}
	if a.RolePassword(OwnerRole) == a.RolePassword(IngestRole) {
		t.Errorf("roles share password %q", a.RolePassword(OwnerRole))
	}
	// Topology containers have no host port; their internal DSNs keep
	// role-name passwords.
	internal := &DB{password: "postgres"}
	if got := internal.RolePassword(OwnerRole); got != OwnerRole {
		t.Errorf("no-host-DSN RolePassword = %q, want %q", got, OwnerRole)
	}
}

func TestConnectRerouted(t *testing.T) {
	authFailed := &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: "password authentication failed"}
	missing := &pgconn.PgError{Severity: "FATAL", Code: "3D000", Message: "database does not exist"}

	t.Run("reverifies route then retries once", func(t *testing.T) {
		dsn := "old"
		var tried []string
		reverified := 0
		got, err := connectRerouted(context.Background(), func() string { return dsn },
			func(_ context.Context, d string) (string, error) {
				tried = append(tried, d)
				if d == "old" {
					return "", authFailed
				}
				return "conn:" + d, nil
			},
			func() error { reverified++; dsn = "new"; return nil })
		if err != nil || got != "conn:new" {
			t.Fatalf("got %q, %v; want conn:new", got, err)
		}
		if reverified != 1 || strings.Join(tried, ",") != "old,new" {
			t.Fatalf("reverified %d, tried %v; want 1 and old,new", reverified, tried)
		}
	})

	t.Run("non-retryable fails at once", func(t *testing.T) {
		attempts, reverified := 0, 0
		_, err := connectRerouted(context.Background(), func() string { return "d" },
			func(context.Context, string) (string, error) { attempts++; return "", missing },
			func() error { reverified++; return nil })
		if !errors.Is(err, missing) || attempts != 1 || reverified != 0 {
			t.Fatalf("err %v, attempts %d, reverified %d; want missing, 1, 0", err, attempts, reverified)
		}
	})

	t.Run("bounded to one retry", func(t *testing.T) {
		attempts, reverified := 0, 0
		_, err := connectRerouted(context.Background(), func() string { return "d" },
			func(context.Context, string) (string, error) { attempts++; return "", authFailed },
			func() error { reverified++; return nil })
		if !errors.Is(err, authFailed) || attempts != 2 || reverified != 1 {
			t.Fatalf("err %v, attempts %d, reverified %d; want auth failure, 2, 1", err, attempts, reverified)
		}
	})

	t.Run("reverify failure is reported", func(t *testing.T) {
		gone := errors.New("route never verified")
		attempts := 0
		_, err := connectRerouted(context.Background(), func() string { return "d" },
			func(context.Context, string) (string, error) { attempts++; return "", authFailed },
			func() error { return gone })
		if !errors.Is(err, gone) || attempts != 1 {
			t.Fatalf("err %v, attempts %d; want reverify error, 1", err, attempts)
		}
	})
}

// Rotten roles must reject role-name passwords, so a connection that reaches
// another test's rotten container fails auth instead of using it.
func TestRottenRolesRejectSharedPasswords(t *testing.T) {
	db := StartRottenEmpty(t)
	for _, role := range RottenRoles {
		u, err := url.Parse(db.DSNAs(t, role))
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(role, role)
		conn, err := pgx.Connect(context.Background(), u.String())
		if err == nil {
			conn.Close(context.Background())
			t.Errorf("%s connected with its role-name password", role)
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
			t.Errorf("%s: err = %v, want 28P01", role, err)
		}
		db.ConnectAs(t, role)
	}
}

// A container started by testdb must reject the old shared password, so a
// connection that reaches the wrong container fails auth instead of silently
// using it.
func TestStartedContainerRejectsSharedPassword(t *testing.T) {
	db := StartObserved(t, 18)
	u, err := url.Parse(db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	if pw, _ := u.User.Password(); pw == "postgres" {
		t.Fatalf("DSN uses the shared password %q", pw)
	}
	u.User = url.UserPassword("postgres", "postgres")
	conn, err := pgx.Connect(context.Background(), u.String())
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("connected with the shared password; want auth failure")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
		t.Fatalf("err = %v, want 28P01", err)
	}
	if !retryableConnectError(err) {
		t.Fatalf("wrong-container auth failure %v is not retryable", err)
	}
	db.Connect(t)
}
