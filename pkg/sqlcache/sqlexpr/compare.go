package sqlexpr

import "strings"

// Compare resolves to: left op right.
// Both sides resolve recursively.
type Compare struct {
	Left  Expr
	Op    string // "=", "!=", "<", ">", "<=", ">="
	Right Expr
}

func (c Compare) Resolve() (string, []any) {
	ls, lp := c.Left.Resolve()
	rs, rp := c.Right.Resolve()
	return ls + " " + c.Op + " " + rs, append(lp, rp...)
}

// Like resolves to: col [NOT] LIKE pattern ESCAPE '\'.
type Like struct {
	Col     Expr
	Pattern Expr
	Negate  bool
}

func (l Like) Resolve() (string, []any) {
	cs, cp := l.Col.Resolve()
	ps, pp := l.Pattern.Resolve()
	op := "LIKE"
	if l.Negate {
		op = "NOT LIKE"
	}
	return cs + " " + op + " " + ps + ` ESCAPE '\'`, append(cp, pp...)
}

// In resolves to: expr [NOT] IN (val1, val2, ...).
type In struct {
	Expr   Expr
	Values []Expr
	Negate bool
}

func (i In) Resolve() (string, []any) {
	es, ep := i.Expr.Resolve()
	var params []any
	params = append(params, ep...)
	placeholders := make([]string, len(i.Values))
	for idx, v := range i.Values {
		s, p := v.Resolve()
		placeholders[idx] = s
		params = append(params, p...)
	}
	op := "IN"
	if i.Negate {
		op = "NOT IN"
	}
	return es + " " + op + " (" + strings.Join(placeholders, ", ") + ")", params
}

// FuncCall resolves to: name(arg1, arg2, ...).
// Supports functions like hasBarredValue, inet_aton, extractBarredValue,
// adjustTimestampForSorting.
type FuncCall struct {
	Name string
	Args []Expr
}

func (f FuncCall) Resolve() (string, []any) {
	argParts := make([]string, len(f.Args))
	var params []any
	for i, arg := range f.Args {
		s, p := arg.Resolve()
		argParts[i] = s
		params = append(params, p...)
	}
	return f.Name + "(" + strings.Join(argParts, ", ") + ")", params
}

// Func1 is a convenience constructor for single-argument FuncCall.
func Func1(name string, arg Expr) FuncCall {
	return FuncCall{Name: name, Args: []Expr{arg}}
}

// Func2 is a convenience constructor for two-argument FuncCall.
func Func2(name string, arg1, arg2 Expr) FuncCall {
	return FuncCall{Name: name, Args: []Expr{arg1, arg2}}
}
