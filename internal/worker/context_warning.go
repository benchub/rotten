package worker

import (
	"context"
	"regexp"
	"regexp/syntax"

	"github.com/jackc/pgx/v5"
)

// Postgres 18's pg_stat_statements keeps no leading comment in the query
// text, so leading marginalia never reach the context regexes. The worker
// can't fix that, but it can say so. On an 18+ server with context regexes
// that can match, once one connection has seen contextWarnMinWindows
// non-baseline windows and contextWarnMinCalls calls without a single
// context match, it warns once per process.
//
// The observer role's own statements (the worker's sanity checks and reads)
// don't count, so an idle server can't trigger it. The window minimum is a
// grace period, not a statistical test: it keeps one burst of untagged tool
// traffic from warning before a tagged job runs. A job quieter than that, on
// a server with no other tagged traffic, can still be warned about.
// One match on the connection is proof the app's comments reach
// pg_stat_statements in a form the regexes read, so it ends the watch. A
// restart re-checks; repeating it daily would only add noise for a setting
// that changes when the app is redeployed, not on its own.
const (
	contextWarnMinCalls      = 1000
	contextWarnMinWindows    = 3
	contextWarnMinVersionNum = 180000
)

const noContextMatchesWarning = "No marginalia contexts found in sampled calls on PostgreSQL 18+. If your application emits leading comments, PostgreSQL 18 removes them from pg_stat_statements; configure it to append them (e.g. prepend_comment = false)"

// contextWatch tracks context matches for the no-contexts warning. Only Run's
// goroutine touches it. Everything but warned resets on each connection, so a
// cluster upgraded to 18 under a running worker is checked afresh.
type contextWatch struct {
	versionNum     int
	observerUserID uint32
	calls          uint64
	windows        int
	matched        bool
	warned         bool
}

// observedServerInfo returns the observed server's server_version_num and
// the oid of the role the worker connected as. A version of 0 means it
// couldn't read them, which turns the no-contexts warning off.
func (w *Worker) observedServerInfo(ctx context.Context, conn *pgx.Conn) (int, uint32) {
	var v int
	var oid uint32
	if err := conn.QueryRow(ctx, `select current_setting('server_version_num')::int, current_user::regrole::oid`).Scan(&v, &oid); err != nil {
		w.cfg.Logger.Warn("couldn't read the observed server version; skipping the Postgres 18 marginalia check on this connection", "err", err)
		return 0, 0
	}
	return v, oid
}

// observedConnected starts a fresh watch for a new observed connection.
func (w *Worker) observedConnected(versionNum int, observerUserID uint32) {
	w.ctxWatch = contextWatch{versionNum: versionNum, observerUserID: observerUserID, warned: w.ctxWatch.warned}
}

// noteContext records one statement's calls and whether it had a context.
// Only top-level statements from roles other than the observer count: with
// pg_stat_statements.track = all, statements nested in a SECURITY DEFINER
// function the observer runs are recorded under the function's owner, and
// marginalia live on the application's top-level statements anyway.
func (w *Worker) noteContext(userID uint32, topLevel bool, calls uint64, matched bool) {
	if !topLevel || userID == w.ctxWatch.observerUserID {
		return
	}
	w.ctxWatch.calls += calls
	if matched {
		w.ctxWatch.matched = true
	}
}

// maybeWarnNoContexts logs the Postgres 18 marginalia warning if the watch
// says so. harvest calls it after each non-baseline window.
func (w *Worker) maybeWarnNoContexts() {
	cw := &w.ctxWatch
	cw.windows++
	if cw.warned || cw.matched || cw.versionNum < contextWarnMinVersionNum || cw.calls < contextWarnMinCalls || cw.windows < contextWarnMinWindows {
		return
	}
	if neverMatches(w.cfg.ReController) && neverMatches(w.cfg.ReAction) && neverMatches(w.cfg.ReJobTag) {
		return
	}
	cw.warned = true
	w.cfg.Logger.Warn(noContextMatchesWarning,
		"server_version_num", cw.versionNum,
		"sampled_calls", cw.calls,
		"windows", cw.windows)
}

// neverMatches reports whether re can't match any text, like the documented
// `a^` that turns contexts off. It's conservative: a false means re might
// match. It walks re's program and rules out paths that need the start of
// text after a character, or a character after the end of text.
func neverMatches(re *regexp.Regexp) bool {
	if re == nil {
		return true
	}
	parsed, err := syntax.Parse(re.String(), syntax.Perl)
	if err != nil {
		return false
	}
	prog, err := syntax.Compile(parsed.Simplify())
	if err != nil {
		return false
	}
	type state struct {
		pc            uint32
		consumed, end bool
	}
	seen := map[state]bool{}
	stack := []state{{pc: uint32(prog.Start)}}
	push := func(s state) {
		if !seen[s] {
			seen[s] = true
			stack = append(stack, s)
		}
	}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		inst := prog.Inst[s.pc]
		switch inst.Op {
		case syntax.InstMatch:
			return false
		case syntax.InstFail:
		case syntax.InstAlt, syntax.InstAltMatch:
			push(state{inst.Out, s.consumed, s.end})
			push(state{inst.Arg, s.consumed, s.end})
		case syntax.InstCapture, syntax.InstNop:
			push(state{inst.Out, s.consumed, s.end})
		case syntax.InstEmptyWidth:
			op := syntax.EmptyOp(inst.Arg)
			if op&syntax.EmptyBeginText != 0 && s.consumed {
				continue
			}
			push(state{inst.Out, s.consumed, s.end || op&syntax.EmptyEndText != 0})
		case syntax.InstRune:
			if s.end || len(inst.Rune) == 0 {
				continue
			}
			push(state{inst.Out, true, false})
		default: // InstRune1, InstRuneAny, InstRuneAnyNotNL
			if s.end {
				continue
			}
			push(state{inst.Out, true, false})
		}
	}
	return true
}
