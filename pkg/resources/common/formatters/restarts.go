package formatters

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"
)

// FormatRestarts converts [count, timestamp_ms] back to "4 (3h38m ago)"
func FormatRestarts(arrayValue []interface{}) string {
	if len(arrayValue) < 2 {
		return fmt.Sprintf("%v", arrayValue[0])
	}

	count := arrayValue[0]

	// Recalculate fresh "ago" time from timestamp
	if arrayValue[1] != nil {
		if timestamp, ok := arrayValue[1].(float64); ok {
			dur := time.Since(time.UnixMilli(int64(timestamp)))
			humanDur := duration.HumanDuration(dur)
			return fmt.Sprintf("%v (%s ago)", count, humanDur)
		}
	}

	// No timestamp, just show count
	return fmt.Sprintf("%v", count)
}
