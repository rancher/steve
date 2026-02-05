package composite

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"
)

// CompositeInt represents a field that splits into two integer values in the database.
// This is a wrapper type for type-safe processing. The underlying object stores []interface{}.
// Used for fields like Pod restarts where we need both a count and a timestamp.
type CompositeInt struct {
	Primary   int64 // e.g., restart count
	Secondary int64 // e.g., timestamp in milliseconds (or 0 if not applicable)
}

// From creates a CompositeInt from a slice of values.
// This is the main constructor used by SQL layer and formatter.
//
// Expected input: [count, timestamp_ms] where:
//   - count: restart count (int or float64)
//   - timestamp_ms: Unix timestamp in milliseconds of last restart (int64, float64, or nil)
//
// Handles partial/malformed input gracefully:
//   - Empty slice: returns {Primary: 0, Secondary: 0}
//   - Single element [count]: returns {Primary: count, Secondary: 0}
//   - nil values: treated as 0
func (CompositeInt) From(values []interface{}) CompositeInt {
	ci := CompositeInt{}
	if len(values) >= 1 {
		ci.Primary = ToInt64(values[0])
	}
	if len(values) >= 2 {
		ci.Secondary = ToInt64(values[1])
	}
	return ci
}

// ToSlice converts CompositeInt back to a slice.
// The secondary value is set to nil if it's zero, matching the original format.
// Used if we need to store back to the object (rarely needed).
func (ci CompositeInt) ToSlice() []interface{} {
	var secondary interface{}
	if ci.Secondary != 0 {
		secondary = ci.Secondary
	}
	return []interface{}{ci.Primary, secondary}
}

// IsZero returns true if both values are zero.
func (ci CompositeInt) IsZero() bool {
	return ci.Primary == 0 && ci.Secondary == 0
}

// FormatAsRestarts formats the CompositeInt as a Pod restart count display string.
// Examples: "0", "4 (3h38m ago)"
// The "ago" time is calculated fresh on every call, never cached.
func (ci CompositeInt) FormatAsRestarts() string {
	if ci.Secondary == 0 {
		return fmt.Sprintf("%d", ci.Primary)
	}

	// Calculate fresh duration from timestamp
	timestamp := time.UnixMilli(ci.Secondary)
	dur := time.Since(timestamp)
	humanDur := duration.HumanDuration(dur)

	return fmt.Sprintf("%d (%s ago)", ci.Primary, humanDur)
}

// ToInt64 converts various numeric types to int64, returning 0 for nil or unsupported types.
func ToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case nil:
		return 0
	}
	return 0
}
