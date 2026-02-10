package informer

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
