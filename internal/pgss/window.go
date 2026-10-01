package pgss

import "math"

// minM2Ratio is the smallest M2_win / M2_cur for which the window stddev is
// trusted. pg_stat_statements' stddev is a float64 built by a long Welford
// sum, so assume each side's M2 carries a relative error ε of roughly 1e-12
// to 1e-9 after many calls. The error in M2_win is then about
// ε × M2_cur / M2_win relative. At a ratio of 1e-6 that's at worst about 1e-3
// (0.1%) on the variance, and half that on the stddev, which is fine for
// reporting. Below it, the result can be mostly noise.
const minM2Ratio = 1e-6

// flatRatio: when M2_cur is this small relative to n × mean², the entry's
// times are effectively constant. The window stddev is then 0, and that's
// reliable even though the ratio test can't be applied.
const flatRatio = 1e-20

// WindowStats returns the exec-time mean and stddev of d's window, and
// whether the stddev can be trusted.
//
// Both are exec only, to match MeanTime and StddevTime from the reader: the
// mean uses TotalExecTime, not TotalTime (plan + exec), because
// pg_stat_statements builds its mean and stddev from exec times alone, and
// mixing plan time into the mean would make it disagree with the stddev.
//
// pg_stat_statements keeps sum_var_time with Welford's method and reports
// stddev_exec_time = sqrt(sum_var_time / calls) (0 when calls <= 1), a
// population stddev (contrib/pg_stat_statements/pg_stat_statements.c). So
// each side's sum of squared deviations is M2 = stddev² × calls, and the
// parallel-variance formula (Chan et al.) for combining prev and window,
//
//	M2_cur = M2_prev + M2_win + δ² × n_prev × n_win / n_cur,  δ = mean_win − mean_prev,
//
// solved for M2_win gives the window's variance M2_win / n_win.
//
// Accuracy: the mean is a difference of exact-ish totals and is always
// usable. The stddev is not. M2_win comes from subtracting two large, nearly
// equal numbers, so a relative error ε in pgss's stddev becomes an error of
// about 2ε × M2_cur / M2_win in M2_win. With a huge history (say 1e9 calls)
// and a small window, that's noise. stddevOK is false when
// M2_win / M2_cur < minM2Ratio (1e-6, see its comment), including when
// rounding pushed M2_win negative and it was clamped to 0. The clamp keeps
// the result from being NaN, but it can hide real variance, which is why a
// clamp on a non-trivial M2_cur also reports stddevOK = false. Callers
// should drop or flag the stddev when stddevOK is false.
//
// For a New delta the counters are the entry's full values, so the current
// MeanTime and StddevTime are returned as-is, with stddevOK true. A delta
// with zero calls (Diff never returns one) gives 0, 0, true. A one-call
// window has stddev 0, true, as pg_stat_statements would report.
func WindowStats(d Delta) (mean, stddev float64, stddevOK bool) {
	if d.New || d.Prev == nil {
		return d.MeanTime, d.StddevTime, true
	}
	n := float64(d.Calls) // already the window's Δcalls
	if n <= 0 {
		return 0, 0, true
	}
	mean = d.TotalExecTime / n
	if d.Calls == 1 {
		return mean, 0, true
	}
	p := d.Prev
	np := float64(p.Calls)
	nc := np + n
	m2c := d.StddevTime * d.StddevTime * nc // current side, cumulative
	m2p := p.StddevTime * p.StddevTime * np
	var cross float64
	if np > 0 {
		delta := mean - p.TotalExecTime/np
		cross = delta * delta * np * n / nc
	}
	meanc := (p.TotalExecTime + d.TotalExecTime) / nc
	if m2c <= flatRatio*nc*meanc*meanc {
		return mean, 0, true
	}
	m2w := m2c - m2p - cross
	stddevOK = m2w >= minM2Ratio*m2c
	if m2w < 0 {
		m2w = 0
	}
	return mean, math.Sqrt(m2w / n), stddevOK
}
