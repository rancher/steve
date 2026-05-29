package sqlexpr

// And resolves each child expression and joins them with AND.
// Empty produces no output. Single child resolves without wrapping.
type And []Expr

func (a And) Resolve() (string, []any) {
	return joinExprs([]Expr(a), " AND ")
}

// Or resolves each child expression and joins them with OR.
// Empty produces no output. Single child resolves without wrapping.
type Or []Expr

func (o Or) Resolve() (string, []any) {
	return joinExprs([]Expr(o), " OR ")
}

// Not wraps a child expression in NOT (...).
type Not struct {
	Inner Expr
}

func (n Not) Resolve() (string, []any) {
	sql, params := n.Inner.Resolve()
	if sql == "" {
		return "", nil
	}
	return "NOT (" + sql + ")", params
}
