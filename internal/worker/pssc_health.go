package worker

import (
	"context"
	"strings"

	"github.com/benchub/rotten/internal/pssc"
)

// pssc is optional, and a misconfigured pssc quietly ships calls as
// untagged, so the worker says what it found. On each connection to the
// observed database it logs whether pssc is in use, and each harvest logs
// again when that changes (pssc created or dropped while connected). While
// pssc is in use, each harvest rereads its settings (one pg_settings query)
// and warns if they can't produce the contexts the worker maps. A settings
// warning is logged once per distinct bad value, so a reload that breaks a
// setting warns at the next harvest, and an unchanged bad value stays quiet
// across harvests and reconnects. The worker also warns if
// utility_missing_queryid keeps rising (the usual sign that pssc is loaded
// before pg_stat_statements; the observer can't read
// shared_preload_libraries to check the order directly).
//
// The tags check compares the allowlist with the mapped keys as written, and
// that's right even when keys are renamed. In pssc, rename is a parameter of
// each extractor in the extractors setting (rename=old:new|...), and the tags
// allowlist applies after it, so tags already names final keys. A key renamed
// to controller survives only if tags lists controller.

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
	inUse, haveInUse               bool
	checkedExtractors, checkedTags string
	haveSettings                   bool
	warnedUtility                  bool
	// settingsErr is the last settings read error warned about, so each
	// distinct error warns once.
	settingsErr string
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
	d, err := r.Detect(ctx)
	if err != nil {
		w.cfg.Logger.Warn("couldn't check for pg_stat_statement_context", "err", err)
		return
	}
	w.notePSSCDetection(d, true)
	if !d.Available() {
		return
	}
	s, err := r.Settings(ctx)
	if err != nil {
		w.cfg.Logger.Warn("couldn't read pg_stat_statement_context's settings", "err", err)
		return
	}
	w.checkPSSCSettings(s)
}

// notePSSCDetection records a detection and logs whether pssc is in use:
// always on connect, otherwise only when that changed since the last log.
func (w *Worker) notePSSCDetection(d pssc.Detection, connect bool) {
	pw := &w.psscWatch
	inUse := d.Available()
	pw.schema = ""
	if inUse {
		pw.schema = d.Schema
	}
	if !connect && pw.haveInUse && pw.inUse == inUse {
		return
	}
	pw.inUse, pw.haveInUse = inUse, true
	if inUse {
		w.cfg.Logger.Info(psscInUseMessage, "schema", d.Schema)
		return
	}
	reason := "the library isn't in shared_preload_libraries"
	if d.Preloaded {
		reason = "the extension isn't created in this database"
	}
	w.cfg.Logger.Info(psscNotInUseMessage, "reason", reason)
}

// checkPSSCSettings warns about extractors that can't see prepended comments
// and a tags allowlist missing mapped keys, once for each value it sees.
func (w *Worker) checkPSSCSettings(s pssc.Settings) {
	pw := &w.psscWatch
	first := !pw.haveSettings
	pw.haveSettings = true
	if first || s.Extractors != pw.checkedExtractors {
		pw.checkedExtractors = s.Extractors
		if !pssc.SeesPrepended(s.Extractors) {
			w.cfg.Logger.Warn(psscExtractorsWarning, "extractors", s.Extractors)
		}
	}
	if first || s.Tags != pw.checkedTags {
		pw.checkedTags = s.Tags
		if missing := pssc.MissingMappedTags(s.Tags); len(missing) > 0 {
			w.cfg.Logger.Warn(psscTagsWarning, "tags", s.Tags, "missing", strings.Join(missing, ", "))
		}
	}
}

// checkPSSCHarvest runs the per-harvest checks. harvest calls it after
// reading pssc, with whether that read found pssc available. It logs pssc
// appearing or vanishing, rechecks the settings, and feeds the
// utility_missing_queryid rule. Errors are returned so harvest can reconnect
// on a broken connection; anything else is only logged.
func (w *Worker) checkPSSCHarvest(ctx context.Context, r *pssc.Reader, available bool) error {
	pw := &w.psscWatch
	if !available {
		if !pw.haveInUse || !pw.inUse {
			return nil
		}
		d, err := r.Detect(ctx)
		if err != nil {
			return err
		}
		w.notePSSCDetection(d, false)
		return nil
	}
	if pw.schema == "" {
		d, err := r.Detect(ctx)
		if err != nil {
			return err
		}
		w.notePSSCDetection(d, false)
		if !d.Available() {
			return nil
		}
	}
	s, settingsErr := r.Settings(ctx)
	if settingsErr != nil {
		if msg := settingsErr.Error(); msg != pw.settingsErr {
			pw.settingsErr = msg
			w.cfg.Logger.Warn("couldn't read pg_stat_statement_context's settings", "err", settingsErr)
		}
		// A broken connection fails the utility read too; return that
		// error, so harvest reconnects.
		if err := w.checkPSSCUtility(ctx, r); err != nil {
			return err
		}
		return settingsErr
	}
	pw.settingsErr = ""
	w.checkPSSCSettings(s)
	return w.checkPSSCUtility(ctx, r)
}

// checkPSSCUtility reads utility_missing_queryid for the rising check, from
// the schema the last detection found.
func (w *Worker) checkPSSCUtility(ctx context.Context, r *pssc.Reader) error {
	pw := &w.psscWatch
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
