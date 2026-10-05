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
	w.parseFailureCalls = 0
	w.parseFailureSamples = nil
}

// recordParseFailure counts an entry the parser rejected, which is sent
// under a fallback fingerprint, and its calls.
func (w *Worker) recordParseFailure(query string, calls uint64) {
	w.parseFailuresMu.Lock()
	defer w.parseFailuresMu.Unlock()
	w.parseFailures++
	w.parseFailureCalls += calls
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

func (w *Worker) parseFailureSnapshot() (uint32, uint64, []string) {
	w.parseFailuresMu.Lock()
	defer w.parseFailuresMu.Unlock()
	return w.parseFailures, w.parseFailureCalls, append([]string(nil), w.parseFailureSamples...)
}
