package worker

import (
	"context"
	"strings"

	"github.com/benchub/rotten/internal/pssc"
)

// pssc is optional, and a misconfigured pssc quietly ships calls as
// untagged, so the worker says what it found. On each connection to the
// observed database it logs whether pssc is in use. When it is, the worker
// warns if pssc's settings can't produce the contexts it maps, and if
// utility_missing_queryid keeps rising (the usual sign that pssc is loaded
// before pg_stat_statements; the observer can't read
// shared_preload_libraries to check the order directly). Each warning is
// logged once per process: the settings change with a config reload, not on
// their own, so repeating them would only add noise.

const (
	psscInUseMessage          = "pg_stat_statement_context is in use, so contexts come from it"
	psscNotInUseMessage       = "pg_stat_statement_context isn't in use on the observed database, so every call ships untagged"
	psscExtractorsWarning     = "pg_stat_statement_context.extractors reads only appended comments, so prepended marginalia (as Rails writes it) is untagged; add position=any (or position=prepend) to its marginalia and sqlcommenter extractors"
	psscTagsWarning           = "pg_stat_statement_context.tags leaves out keys the worker maps to contexts, so those calls lose them; add the missing keys"
	psscUtilityMissingWarning = "pg_stat_statement_context's utility_missing_queryid keeps rising; that's usually load order, but it also rises when utility statements are re-run from a plan cache (e.g. a named prepared SET); check shared_preload_libraries lists pg_stat_statements first"
)

// The worker warns when utility_missing_queryid rose in psscUtilityRises of
// the last psscUtilityWindow harvests. One rise can be a prepared utility
// statement re-run from a plan cache, which counts even in the right order,
// and a misordered server's rises needn't be back to back either.
const (
	psscUtilityRises  = 3
	psscUtilityWindow = 6
)

type psscWatch struct {
	// Per connection.
	schema      string
	lastMissing int64
	haveLast    bool
	// recent holds, oldest first, whether each of the last
	// psscUtilityWindow harvests saw a rise.
	recent []bool
	// Per process.
	warnedExtractors, warnedTags, warnedUtility bool
}

// psscConnected starts the per-connection part of the watch afresh.
func (w *Worker) psscConnected() {
	pw := &w.psscWatch
	pw.schema, pw.lastMissing, pw.haveLast, pw.recent = "", 0, false, nil
}

// checkPSSCAtConnect logs whether pssc is in use on a new connection and
// checks its settings. Failures are logged, not returned: the harvest's own
// reads decide whether the connection works.
func (w *Worker) checkPSSCAtConnect(ctx context.Context, r *pssc.Reader) {
	w.psscConnected()
	log := w.cfg.Logger
	d, err := r.Detect(ctx)
	if err != nil {
		log.Warn("couldn't check for pg_stat_statement_context", "err", err)
		return
	}
	if !d.Available() {
		reason := "the library isn't in shared_preload_libraries"
		if d.Preloaded {
			reason = "the extension isn't created in this database"
		}
		log.Info(psscNotInUseMessage, "reason", reason)
		return
	}
	w.psscWatch.schema = d.Schema
	log.Info(psscInUseMessage, "schema", d.Schema)
	s, err := r.Settings(ctx)
	if err != nil {
		log.Warn("couldn't read pg_stat_statement_context's settings", "err", err)
		return
	}
	w.checkPSSCSettings(s)
}

// checkPSSCSettings warns, once per process each, about extractors that
// can't see prepended comments and a tags allowlist missing mapped keys.
func (w *Worker) checkPSSCSettings(s pssc.Settings) {
	pw := &w.psscWatch
	if !pw.warnedExtractors && !pssc.SeesPrepended(s.Extractors) {
		pw.warnedExtractors = true
		w.cfg.Logger.Warn(psscExtractorsWarning, "extractors", s.Extractors)
	}
	if missing := pssc.MissingMappedTags(s.Tags); !pw.warnedTags && len(missing) > 0 {
		pw.warnedTags = true
		w.cfg.Logger.Warn(psscTagsWarning, "tags", s.Tags, "missing", strings.Join(missing, ", "))
	}
}

// checkPSSCUtility reads utility_missing_queryid for the rising check. harvest
// calls it when pssc was read. A pssc created after the connection opened is
// picked up here.
func (w *Worker) checkPSSCUtility(ctx context.Context, r *pssc.Reader) error {
	pw := &w.psscWatch
	if pw.schema == "" {
		d, err := r.Detect(ctx)
		if err != nil || !d.Available() {
			return err
		}
		pw.schema = d.Schema
	}
	n, err := r.UtilityMissingQueryID(ctx, pw.schema)
	if err != nil {
		// The schema may have moved; detect again next time.
		pw.schema = ""
		w.cfg.Logger.Debug("couldn't read pg_stat_statement_context_info", "err", err)
		return err
	}
	w.notePSSCUtilityMissing(n)
	return nil
}

// notePSSCUtilityMissing records one harvest's utility_missing_queryid.
func (w *Worker) notePSSCUtilityMissing(n int64) {
	pw := &w.psscWatch
	if pw.haveLast {
		pw.recent = append(pw.recent, n > pw.lastMissing)
		if len(pw.recent) > psscUtilityWindow {
			pw.recent = pw.recent[1:]
		}
	}
	pw.lastMissing, pw.haveLast = n, true
	rises := 0
	for _, r := range pw.recent {
		if r {
			rises++
		}
	}
	if rises >= psscUtilityRises && !pw.warnedUtility {
		pw.warnedUtility = true
		w.cfg.Logger.Warn(psscUtilityMissingWarning, "utility_missing_queryid", n, "rises", rises, "harvests", len(pw.recent))
	}
}
