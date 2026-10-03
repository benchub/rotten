package ingest

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
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

	MaxHarvestWindowDuration = 24 * time.Hour
	MaxHarvestFutureSkew     = 5 * time.Minute
)

func validateTextField(name, value string, maxBytes int, allowEmpty bool) error {
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
