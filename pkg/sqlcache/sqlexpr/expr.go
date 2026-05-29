// Package sqlexpr provides a recursive, composable SQL expression tree.
//
// Every node implements the Expr interface, which resolves itself into
// a SQL fragment and bound parameters via a single recursive Resolve() call.
// This replaces imperative string-concatenation SQL generation with
// immutable, testable, composable expression trees.
package sqlexpr

// Expr is anything that can resolve itself to a SQL fragment and parameters.
// Composition is recursive: an Expr can contain other Exprs.
type Expr interface {
	Resolve() (sql string, params []any)
}

// Raw is a pre-built SQL fragment with optional bound parameters.
// Use for literal SQL that doesn't need further composition.
type Raw struct {
	SQL    string
	Params []any
}

func (r Raw) Resolve() (string, []any) { return r.SQL, r.Params }

// Col references a qualified column: prefix."name" or prefix.name.
// Names containing dots or special characters are quoted; simple identifiers are not.
// If Table is empty, always quotes the name.
type Col struct {
	Table string
	Name  string
}

func (c Col) Resolve() (string, []any) {
	quoted := needsQuoting(c.Name)
	if c.Table == "" {
		if quoted {
			return `"` + c.Name + `"`, nil
		}
		return c.Name, nil
	}
	if quoted {
		return c.Table + `."` + c.Name + `"`, nil
	}
	return c.Table + "." + c.Name, nil
}

// needsQuoting returns true if the identifier needs double-quote wrapping.
// Simple identifiers (alphanumeric + underscore, starting with letter/underscore) don't need quoting.
func needsQuoting(name string) bool {
	if name == "" {
		return true
	}
	for i, c := range name {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return true
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				return true
			}
		}
	}
	return false
}

// Param is a single bound parameter placeholder (?).
type Param struct {
	Value any
}

func (p Param) Resolve() (string, []any) { return "?", []any{p.Value} }

// Params is a convenience for building []Expr from a slice of values.
func Params(values ...any) []Expr {
	exprs := make([]Expr, len(values))
	for i, v := range values {
		exprs[i] = Param{Value: v}
	}
	return exprs
}

// False is a constant expression resolving to FALSE (no rows match).
var False Expr = Raw{SQL: "FALSE"}
