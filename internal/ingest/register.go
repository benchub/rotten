package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
)

// Source identifies the observed database a worker watches.
type Source struct {
	Project     string
	Environment string
	Cluster     string
	Role        string
	FQDN        string
}

// Registration is the pair of IDs workers attach to harvest batches.
type Registration struct {
	LogicalSourceID  uint32
	PhysicalSourceID uint32
}

// Beginner is the database surface RegisterSource needs.
type Beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// Handler implements the ingest Connect service.
type Handler struct {
	db     Beginner
	logger *slog.Logger
}

// Options configures a Handler. Zero values take the defaults.
type Options struct {
	Logger *slog.Logger
}

// NewHandler returns a Connect handler backed by db.
func NewHandler(db Beginner, opts ...Options) *Handler {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Handler{db: db, logger: opt.Logger}
}

// Register creates or reuses the source rows for this authenticated worker.
func (h *Handler) Register(ctx context.Context, req *connect.Request[rottenv1.RegisterRequest]) (*connect.Response[rottenv1.RegisterResponse], error) {
	key, ok := auth.FromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing pass key"))
	}
	src := Source{
		Project:     req.Msg.GetProject(),
		Environment: req.Msg.GetEnvironment(),
		Cluster:     req.Msg.GetCluster(),
		Role:        req.Msg.GetRole(),
		FQDN:        req.Msg.GetFqdn(),
	}
	if err := validateSource(src); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if !key.AllowsFQDN(src.FQDN) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("pass key is not pinned to this fqdn"))
	}
	reg, err := RegisterSource(ctx, h.db, src)
	if err != nil {
		h.logger.Error("register source failed", "key_id", key.ID, "err", err)
		if isUnavailable(err) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("source registration unavailable"))
		}
		return nil, connect.NewError(connect.CodeInternal, errors.New("source registration failed"))
	}
	return connect.NewResponse(&rottenv1.RegisterResponse{
		LogicalSourceId:  reg.LogicalSourceID,
		PhysicalSourceId: reg.PhysicalSourceID,
	}), nil
}

// SubmitHarvest is wired in a later task.
func (h *Handler) SubmitHarvest(context.Context, *connect.Request[rottenv1.SubmitHarvestRequest]) (*connect.Response[rottenv1.SubmitHarvestResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("rotten.v1.IngestService.SubmitHarvest is not implemented"))
}

// RegisterSource atomically creates or reuses logical_sources and
// physical_sources rows. It is shared by the server and, until the worker
// switches to RPC, the direct-DB worker startup path.
func RegisterSource(ctx context.Context, db Beginner, src Source) (Registration, error) {
	if err := validateSource(src); err != nil {
		return Registration{}, err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return Registration{}, registerError{op: "begin", err: err}
	}
	defer tx.Rollback(ctx)

	logical, err := upsertID(ctx, tx, `insert into rotten.logical_sources(project, environment, cluster, role)
		values ($1, $2, $3, $4)
		on conflict (cluster, role, project, environment) do update
			set project = rotten.logical_sources.project
		returning id`, src.Project, src.Environment, src.Cluster, src.Role)
	if err != nil {
		return Registration{}, registerError{op: "upsert logical source", err: err}
	}
	physical, err := upsertID(ctx, tx, `insert into rotten.physical_sources(fqdn)
		values ($1)
		on conflict (fqdn) do update
			set fqdn = rotten.physical_sources.fqdn
		returning id`, src.FQDN)
	if err != nil {
		return Registration{}, registerError{op: "upsert physical source", err: err}
	}
	if err := tx.Commit(ctx); err != nil {
		return Registration{}, registerError{op: "commit", err: err}
	}
	return Registration{LogicalSourceID: logical, PhysicalSourceID: physical}, nil
}

type registerError struct {
	op  string
	err error
}

func (e registerError) Error() string { return e.op + " register source: " + e.err.Error() }
func (e registerError) Unwrap() error { return e.err }

func isUnavailable(err error) bool {
	var e registerError
	if errors.As(err, &e) && (e.op == "begin" || e.op == "commit") {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "08000" || strings.HasPrefix(pgErr.Code, "08")
	}
	return false
}

func validateSource(src Source) error {
	if src.Project == "all" && src.Environment == "all" && src.Cluster == "all" && src.Role == "all" {
		return errors.New("reserved logical source all/all/all/all is not valid for registration")
	}
	for name, value := range map[string]string{
		"project":     src.Project,
		"environment": src.Environment,
		"cluster":     src.Cluster,
		"role":        src.Role,
		"fqdn":        src.FQDN,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is empty", name)
		}
	}
	return nil
}

func upsertID(ctx context.Context, tx pgx.Tx, sql string, args ...any) (uint32, error) {
	var id int64
	if err := tx.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		return 0, err
	}
	if id <= 0 || id > math.MaxUint32 {
		return 0, fmt.Errorf("source id %d out of uint32 range", id)
	}
	return uint32(id), nil
}
