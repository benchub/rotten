package harvestlimits

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
)

const (
	// MaxIngestMessageBytes caps the decoded Connect request body. It must fit
	// a full semantic-limit harvest with headroom for protobuf overhead.
	MaxIngestMessageBytes = 32 << 20

	// MaxHarvestAggregates and MaxHarvestContexts are coupled to the worker's
	// topDeltasPerMetric × len(deltaMetrics): one picked delta can become one
	// aggregate and contributes exactly one context before fingerprint merges.
	MaxHarvestAggregates  = 2000
	MaxHarvestContexts    = 2000
	MaxFingerprintBytes   = 128
	MaxNormalizedBytes    = 8 << 10
	MaxContextStringBytes = 512
	MaxSourceStringBytes  = 255
	MaxWorkerVersionBytes = 128
	MaxFloatMetricValue   = 1e15
	// MaxContextCount caps values that are stored in event_context.c and may be
	// summed by reports. 2^53 is exactly representable in float64, matches the
	// events.calls precision, and leaves over 1000 rows of summing headroom before
	// PostgreSQL bigint overflow.
	MaxContextCount = uint64(1 << 53)

	MaxHarvestWindowDuration = 24 * time.Hour
	MaxHarvestFutureSkew     = 5 * time.Minute
)

type FutureSkewPolicy bool

const (
	SkipFutureSkew  FutureSkewPolicy = false
	CheckFutureSkew FutureSkewPolicy = true
)

func ValidateHarvest(msg *rottenv1.SubmitHarvestRequest, now time.Time, futureSkew FutureSkewPolicy) (time.Time, time.Time, error) {
	if msg == nil {
		return time.Time{}, time.Time{}, errors.New("harvest request is required")
	}
	if msg.GetLogicalSourceId() == 0 {
		return time.Time{}, time.Time{}, errors.New("logical_source_id is empty")
	}
	if msg.GetPhysicalSourceId() == 0 {
		return time.Time{}, time.Time{}, errors.New("physical_source_id is empty")
	}
	if err := ValidateTextField("batch_id", msg.GetBatchId(), 128, false); err != nil {
		return time.Time{}, time.Time{}, err
	}
	if msg.GetWindowStart() == nil || msg.GetWindowEnd() == nil {
		return time.Time{}, time.Time{}, errors.New("window timestamps are required")
	}
	if err := msg.GetWindowStart().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window_start is invalid: %w", err)
	}
	if err := msg.GetWindowEnd().CheckValid(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window_end is invalid: %w", err)
	}
	start := msg.GetWindowStart().AsTime()
	end := msg.GetWindowEnd().AsTime()
	if !end.After(start) {
		return time.Time{}, time.Time{}, errors.New("window_end must be after window_start")
	}
	if end.Sub(start) > MaxHarvestWindowDuration {
		return time.Time{}, time.Time{}, fmt.Errorf("window is longer than %s", MaxHarvestWindowDuration)
	}
	if futureSkew == CheckFutureSkew {
		futureLimit := now.UTC().Add(MaxHarvestFutureSkew)
		if start.After(futureLimit) || end.After(futureLimit) {
			return time.Time{}, time.Time{}, fmt.Errorf("window is more than %s in the future", MaxHarvestFutureSkew)
		}
	}
	wantBatch := fmt.Sprintf("%d:%d:%d", msg.GetPhysicalSourceId(), start.UnixMicro(), end.UnixMicro())
	if msg.GetBatchId() != wantBatch {
		return time.Time{}, time.Time{}, errors.New("batch_id does not match source and window")
	}
	if len(msg.GetAggregates()) > MaxHarvestAggregates {
		return time.Time{}, time.Time{}, fmt.Errorf("aggregates exceed %d", MaxHarvestAggregates)
	}
	seenFingerprints := map[string]struct{}{}
	contexts := 0
	for _, aggregate := range msg.GetAggregates() {
		if aggregate == nil || aggregate.GetMetrics() == nil {
			return time.Time{}, time.Time{}, errors.New("aggregate is missing required fields")
		}
		if err := ValidateTextField("fingerprint", aggregate.GetFingerprint(), MaxFingerprintBytes, false); err != nil {
			return time.Time{}, time.Time{}, err
		}
		if err := ValidateTextField("normalized", aggregate.GetNormalized(), MaxNormalizedBytes, false); err != nil {
			return time.Time{}, time.Time{}, err
		}
		if _, ok := seenFingerprints[aggregate.GetFingerprint()]; ok {
			return time.Time{}, time.Time{}, errors.New("aggregate fingerprints must be unique within a batch")
		}
		seenFingerprints[aggregate.GetFingerprint()] = struct{}{}
		if err := validateMetrics(aggregate.GetMetrics()); err != nil {
			return time.Time{}, time.Time{}, err
		}
		if aggregate.GetMetrics().GetCalls() > MaxContextCount {
			return time.Time{}, time.Time{}, errors.New("aggregate calls exceed event storage range")
		}
		seenContexts := map[string]struct{}{}
		var contextCountSum uint64
		for _, qc := range aggregate.GetContexts() {
			contexts++
			if contexts > MaxHarvestContexts {
				return time.Time{}, time.Time{}, fmt.Errorf("contexts exceed %d", MaxHarvestContexts)
			}
			if qc == nil {
				return time.Time{}, time.Time{}, errors.New("context is required")
			}
			if err := ValidateTextField("context controller", qc.GetController(), MaxContextStringBytes, true); err != nil {
				return time.Time{}, time.Time{}, err
			}
			if err := ValidateTextField("context action", qc.GetAction(), MaxContextStringBytes, true); err != nil {
				return time.Time{}, time.Time{}, err
			}
			if err := ValidateTextField("context job_tag", qc.GetJobTag(), MaxContextStringBytes, true); err != nil {
				return time.Time{}, time.Time{}, err
			}
			if qc.GetCount() == 0 || qc.GetCount() > MaxContextCount {
				return time.Time{}, time.Time{}, errors.New("context count is out of range")
			}
			key := qc.GetController() + "\x00" + qc.GetAction() + "\x00" + qc.GetJobTag()
			if _, ok := seenContexts[key]; ok {
				return time.Time{}, time.Time{}, errors.New("aggregate contexts must be unique")
			}
			seenContexts[key] = struct{}{}
			contextCountSum += qc.GetCount()
		}
		if contextCountSum > aggregate.GetMetrics().GetCalls() {
			return time.Time{}, time.Time{}, errors.New("context counts exceed aggregate calls")
		}
	}
	return start, end, nil
}

func ValidateTextField(name, value string, maxBytes int, allowEmpty bool) error {
	if !allowEmpty && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", name, maxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s contains a NUL byte", name)
	}
	return nil
}

func validateMetrics(metrics *rottenv1.Metrics) error {
	for name, value := range map[string]float64{
		"total_time":     metrics.GetTotalTime(),
		"min_time":       metrics.GetMinTime(),
		"max_time":       metrics.GetMaxTime(),
		"mean_time":      metrics.GetMeanTime(),
		"blk_read_time":  metrics.GetBlkReadTime(),
		"blk_write_time": metrics.GetBlkWriteTime(),
	} {
		if err := validateFloatCounter(name, value); err != nil {
			return err
		}
	}
	if metrics.StddevTime != nil {
		if err := validateFloatCounter("stddev_time", metrics.GetStddevTime()); err != nil {
			return err
		}
	}
	return nil
}

func validateFloatCounter(name string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > MaxFloatMetricValue {
		return fmt.Errorf("%s is outside the finite non-negative metric range", name)
	}
	return nil
}
