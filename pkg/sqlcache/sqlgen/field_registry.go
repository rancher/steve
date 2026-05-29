package sqlgen

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
)

// FieldRegistry resolves user-facing field paths to validated SQL column expressions.
// It wraps the indexedFields map from the informer package.
type FieldRegistry interface {
	// Resolve returns a column expression for the given field path.
	// If inSort is true, timestamp fields are wrapped in adjustTimestampForSorting().
	Resolve(prefix string, fields []string, inSort bool) (sqlexpr.Expr, error)

	// IsInteger returns whether the field identified by fieldID is stored as INTEGER.
	IsInteger(fieldID string) bool
}

// IndexedField is a subset of informer.IndexedField needed by the compiler.
type IndexedField interface {
	ColumnName() string
	ColumnType() string
}

// TimestampField is an optional interface for computed fields that are timestamps.
type TimestampField interface {
	IndexedField
	IsTimestampField() bool
}

var containsNonNumeric = regexp.MustCompile(`\D`)

// NewFieldRegistry creates a FieldRegistry from the given indexed fields map.
func NewFieldRegistry(fields map[string]IndexedField) FieldRegistry {
	return &fieldRegistry{fields: fields}
}

type fieldRegistry struct {
	fields map[string]IndexedField
}

func (r *fieldRegistry) Resolve(prefix string, fields []string, inSort bool) (sqlexpr.Expr, error) {
	fieldID := smartJoin(fields)

	// Direct lookup
	if field, ok := r.fields[fieldID]; ok {
		if inSort {
			if ts, ok := field.(TimestampField); ok && ts.IsTimestampField() {
				return sqlexpr.FuncCall{
					Name: "adjustTimestampForSorting",
					Args: []sqlexpr.Expr{sqlexpr.Col{Table: prefix, Name: field.ColumnName()}},
				}, nil
			}
		}
		return sqlexpr.Col{Table: prefix, Name: field.ColumnName()}, nil
	}

	// Fallback: handle numeric indices in the path (e.g., spec.containers.3.image)
	if len(fields) <= 2 {
		return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
	}

	// Find the last numeric index in the field parts
	idx := -1
	for i := len(fields) - 1; i > 0; i-- {
		if !containsNonNumeric.MatchString(fields[i]) {
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
	}

	// Build the base field without the numeric index
	indexField := fields[idx]
	otherFields := append(fields[0:idx], fields[idx+1:]...)
	baseFieldID := smartJoin(otherFields)

	if field, ok := r.fields[baseFieldID]; ok {
		return sqlexpr.FuncCall{
			Name: "extractBarredValue",
			Args: []sqlexpr.Expr{
				sqlexpr.Col{Table: prefix, Name: field.ColumnName()},
				sqlexpr.Raw{SQL: fmt.Sprintf("%q", indexField)},
			},
		}, nil
	}

	return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
}

func (r *fieldRegistry) IsInteger(fieldID string) bool {
	if f, ok := r.fields[fieldID]; ok {
		return f.ColumnType() == "INTEGER"
	}
	return false
}

// DisplayColumn returns the column expression used for summary display.
// Handles the prefix="" case (for simple summary queries without table aliasing).
func DisplayColumn(fields map[string]IndexedField, fieldParts []string, prefix string) (sqlexpr.Expr, error) {
	fieldID := smartJoin(fieldParts)

	if field, ok := fields[fieldID]; ok {
		if prefix == "" {
			return sqlexpr.Raw{SQL: fmt.Sprintf("%q", field.ColumnName())}, nil
		}
		return sqlexpr.Col{Table: prefix, Name: field.ColumnName()}, nil
	}

	// Fallback for numeric-indexed field expressions
	if len(fieldParts) == 1 || containsNonNumeric.MatchString(fieldParts[len(fieldParts)-1]) {
		return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
	}

	baseFieldID := smartJoin(fieldParts[:len(fieldParts)-1])
	if field, ok := fields[baseFieldID]; ok {
		index, err := strconv.Atoi(fieldParts[len(fieldParts)-1])
		if err != nil {
			return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
		}
		if prefix == "" {
			return sqlexpr.Raw{SQL: fmt.Sprintf(`extractBarredValue(%q, %d)`, field.ColumnName(), index)}, nil
		}
		return sqlexpr.FuncCall{
			Name: "extractBarredValue",
			Args: []sqlexpr.Expr{
				sqlexpr.Col{Table: prefix, Name: field.ColumnName()},
				sqlexpr.Raw{SQL: fmt.Sprintf("%d", index)},
			},
		}, nil
	}

	return nil, fmt.Errorf("column is invalid [%s]: %w", fieldID, ErrInvalidColumn)
}

// smartJoin joins field path parts with dots, using bracket notation for complex last segments.
func smartJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	if len(s) == 1 {
		return s[0]
	}
	lastBit := s[len(s)-1]
	simpleName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if simpleName.MatchString(lastBit) {
		return strings.Join(s, ".")
	}
	return fmt.Sprintf("%s[%s]", strings.Join(s[0:len(s)-1], "."), lastBit)
}
