package informer

import (
	rescommon "github.com/rancher/steve/pkg/resources/common"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"regexp"
	"strconv"
	"time"
)

// IndexedField represents a field that can be indexed in the SQL cache.
// It provides both the column name(s) for storage and value extraction logic.
// All methods return slices in consistent order for deterministic SQL generation.
type IndexedField interface {
	// ColumnNames returns the column names in order.
	ColumnNames() []string

	// ColumnTypes returns the SQL types in the same order as ColumnNames.
	ColumnTypes() []string

	// GetValues extracts the value(s) from an unstructured object.
	// Returns values in the same order as ColumnNames.
	// Returns nil values for missing/invalid data.
	GetValues(obj *unstructured.Unstructured) ([]any, error)
}

// JSONPathField represents a standard field accessed via JSON path
type JSONPathField struct {
	Path []string
	Type string // Optional: TEXT (default), INTEGER, REAL, etc.
}

func (f *JSONPathField) ColumnNames() []string {
	return []string{toColumnName(f.Path)}
}

func (f *JSONPathField) ColumnTypes() []string {
	sqlType := f.Type
	if sqlType == "" {
		sqlType = "TEXT"
	}
	return []string{sqlType}
}

func (f *JSONPathField) GetValues(obj *unstructured.Unstructured) ([]any, error) {
	col := toColumnName(f.Path)
	value, err := getField(obj, col)
	if err != nil {
		return []any{nil}, err
	}
	return []any{value}, nil
}

// ComputedField represents a field with custom column names and value extraction
type ComputedField struct {
	Names         []string
	Types         []string
	GetValuesFunc func(obj *unstructured.Unstructured) ([]any, error)
}

func (f *ComputedField) ColumnNames() []string {
	return f.Names
}

func (f *ComputedField) ColumnTypes() []string {
	return f.Types
}

func (f *ComputedField) GetValues(obj *unstructured.Unstructured) ([]any, error) {
	return f.GetValuesFunc(obj)
}

// restartsPattern matches "4 (3h38m ago)" or just "0"
var restartsPattern = regexp.MustCompile(`^(\d+)(?:\s+\((.+?)\s+ago\))?$`)

// timeNow is mockable for testing
var timeNow = time.Now

// ExtractPodRestarts extracts restart count and timestamp from Pod metadata.fields[3].
// Returns [count, timestamp_ms] where timestamp is the Unix time in milliseconds of the last restart.
func ExtractPodRestarts(obj *unstructured.Unstructured) ([]any, error) {
	value, found, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "fields")
	if err != nil || !found {
		return []any{int64(0), int64(0)}, nil
	}

	fields, ok := value.([]interface{})
	if !ok || len(fields) <= 3 {
		return []any{int64(0), int64(0)}, nil
	}

	field3 := fields[3]

	// If it's already a slice (from transform), extract values directly
	if arr, ok := field3.([]interface{}); ok {
		return []any{toInt64(arr, 0), toInt64(arr, 1)}, nil
	}

	// If it's a string (from raw K8s), parse it
	if str, ok := field3.(string); ok {
		count, timestamp := parseRestarts(str)
		return []any{count, timestamp}, nil
	}

	return []any{int64(0), int64(0)}, nil
}

// toInt64 safely extracts an int64 from a slice at the given index
func toInt64(arr []interface{}, idx int) int64 {
	if idx >= len(arr) || arr[idx] == nil {
		return 0
	}
	switch v := arr[idx].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// parseRestarts parses pod restart values like "4 (3h38m ago)" into (count, timestamp_ms)
func parseRestarts(value string) (int64, int64) {
	matches := restartsPattern.FindStringSubmatch(value)
	if matches == nil {
		return 0, 0
	}

	count, _ := strconv.ParseInt(matches[1], 10, 64)

	var timestamp int64
	if matches[2] != "" {
		dur, err := rescommon.ParseHumanReadableDuration(matches[2])
		if err == nil {
			timestamp = timeNow().Add(-dur).UnixMilli()
		}
	}

	return count, timestamp
}
