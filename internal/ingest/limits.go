package ingest

import (
	"time"

	"github.com/benchub/rotten/internal/harvestlimits"
)

const (
	// MaxIngestMessageBytes caps the decoded Connect request body. It must fit
	// a full semantic-limit harvest with headroom for protobuf overhead.
	MaxIngestMessageBytes = harvestlimits.MaxIngestMessageBytes

	// MaxHarvestAggregates and MaxHarvestContexts are coupled to the worker's
	// topDeltasPerMetric × len(deltaMetrics): one picked delta can become one
	// aggregate and contributes exactly one context before fingerprint merges.
	MaxHarvestAggregates  = harvestlimits.MaxHarvestAggregates
	MaxHarvestContexts    = harvestlimits.MaxHarvestContexts
	MaxFingerprintBytes   = harvestlimits.MaxFingerprintBytes
	MaxNormalizedBytes    = harvestlimits.MaxNormalizedBytes
	MaxContextStringBytes = harvestlimits.MaxContextStringBytes
	MaxSourceStringBytes  = harvestlimits.MaxSourceStringBytes
	MaxWorkerVersionBytes = harvestlimits.MaxWorkerVersionBytes
	MaxFloatMetricValue   = harvestlimits.MaxFloatMetricValue
	MaxContextCount       = harvestlimits.MaxContextCount

	MaxHarvestWindowDuration time.Duration = harvestlimits.MaxHarvestWindowDuration
	MaxHarvestFutureSkew     time.Duration = harvestlimits.MaxHarvestFutureSkew
)

func validateTextField(name, value string, maxBytes int, allowEmpty bool) error {
	return harvestlimits.ValidateTextField(name, value, maxBytes, allowEmpty)
}
