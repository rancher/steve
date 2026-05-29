package sqlexpr

import "strings"

// joinExprs resolves each expression, wraps in parens if multiple,
// and joins them with the given separator.
func joinExprs(exprs []Expr, sep string) (string, []any) {
	parts := make([]string, 0, len(exprs))
	var params []any
	for _, e := range exprs {
		if e == nil {
			continue
		}
		s, p := e.Resolve()
		if s == "" {
			continue
		}
		parts = append(parts, s)
		params = append(params, p...)
	}
	switch len(parts) {
	case 0:
		return "", nil
	case 1:
		return parts[0], params
	default:
		// Wrap each part in parentheses for correct precedence
		wrapped := make([]string, len(parts))
		for i, p := range parts {
			wrapped[i] = "(" + p + ")"
		}
		return strings.Join(wrapped, sep), params
	}
}

// joinExprsFlat resolves each expression and joins them with the given separator.
// Unlike joinExprs, does NOT wrap individual parts in parentheses.
func joinExprsFlat(exprs []Expr, sep string) (string, []any) {
	parts := make([]string, 0, len(exprs))
	var params []any
	for _, e := range exprs {
		if e == nil {
			continue
		}
		s, p := e.Resolve()
		if s == "" {
			continue
		}
		parts = append(parts, s)
		params = append(params, p...)
	}
	switch len(parts) {
	case 0:
		return "", nil
	case 1:
		return parts[0], params
	default:
		return strings.Join(parts, sep), params
	}
}
