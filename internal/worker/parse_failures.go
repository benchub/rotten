package worker

import "strings"

const (
	parseFailureSampleLimit = 5
	parseFailureSampleBytes = 1024
)

func (w *Worker) resetParseFailures() {
	w.parseFailuresMu.Lock()
	defer w.parseFailuresMu.Unlock()
	w.parseFailures = 0
	w.parseFailureSamples = nil
}

func (w *Worker) recordParseFailure(query string) {
	w.parseFailuresMu.Lock()
	defer w.parseFailuresMu.Unlock()
	w.parseFailures++
	if len(w.parseFailureSamples) >= parseFailureSampleLimit {
		return
	}
	// Clone the prefix so a short sample doesn't retain a large query.
	sample := strings.Clone(query[:min(len(query), parseFailureSampleBytes)])
	if len(query) > parseFailureSampleBytes {
		sample += "..."
	}
	w.parseFailureSamples = append(w.parseFailureSamples, sample)
}

func (w *Worker) parseFailureSnapshot() (uint32, []string) {
	w.parseFailuresMu.Lock()
	defer w.parseFailuresMu.Unlock()
	return w.parseFailures, append([]string(nil), w.parseFailureSamples...)
}
