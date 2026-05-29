package sqlexpr

import (
	"fmt"
	"strings"
)

// TableRef is a table reference with optional alias.
type TableRef struct {
	Name  string
	Alias string
}

func (t TableRef) Resolve() (string, []any) {
	if t.Name == "" {
		// View/CTE reference — just the alias
		return t.Alias, nil
	}
	if t.Alias == "" {
		return `"` + t.Name + `"`, nil
	}
	return `"` + t.Name + `" ` + t.Alias, nil
}

// JoinKind represents the type of JOIN.
type JoinKind string

const (
	InnerJoin     JoinKind = "JOIN"
	LeftOuterJoin JoinKind = "LEFT OUTER JOIN"
)

// Join represents a single JOIN clause.
type Join struct {
	Kind  JoinKind
	Table TableRef
	On    Expr
}

func (j Join) Resolve() (string, []any) {
	ts, _ := j.Table.Resolve()
	os, op := j.On.Resolve()
	return string(j.Kind) + " " + ts + " ON " + os, op
}

// NullsPosition controls NULLS FIRST/LAST in ORDER BY.
type NullsPosition int

const (
	NullsDefault NullsPosition = iota
	NullsFirst
	NullsLast
)

// OrderBy represents a sort directive.
type OrderBy struct {
	Expr  Expr
	Desc  bool
	Nulls NullsPosition
}

func (o OrderBy) Resolve() (string, []any) {
	s, p := o.Expr.Resolve()
	dir := " ASC"
	if o.Desc {
		dir = " DESC"
	}
	nulls := ""
	switch o.Nulls {
	case NullsFirst:
		nulls = " NULLS FIRST"
	case NullsLast:
		nulls = " NULLS LAST"
	}
	return s + dir + nulls, p
}

// WithClause represents a Common Table Expression (CTE).
type WithClause struct {
	Name    string
	Columns []string
	Body    Expr
}

func (w WithClause) Resolve() (string, []any) {
	bs, bp := w.Body.Resolve()
	cols := ""
	if len(w.Columns) > 0 {
		cols = "(" + strings.Join(w.Columns, ", ") + ")"
	}
	return w.Name + cols + " AS (\n" + bs + "\n)", bp
}

// GroupBy represents a GROUP BY clause with one or more expressions.
type GroupBy []Expr

func (g GroupBy) Resolve() (string, []any) {
	parts := make([]string, len(g))
	var params []any
	for i, e := range g {
		s, p := e.Resolve()
		parts[i] = s
		params = append(params, p...)
	}
	return strings.Join(parts, ", "), params
}

// Select represents a full SELECT statement. It resolves recursively
// by resolving each component (CTEs, columns, FROM, JOINs, WHERE,
// ORDER BY, GROUP BY, LIMIT, OFFSET).
type Select struct {
	CTEs     []WithClause
	Distinct bool
	Columns  []Expr
	From     TableRef
	Joins    []Join
	Where    Expr // nil means no WHERE clause
	GroupBy  GroupBy
	OrderBy  []OrderBy
	Limit    *int
	Offset   *int
}

func (s Select) Resolve() (string, []any) {
	var b strings.Builder
	var params []any

	// WITH clauses
	if len(s.CTEs) > 0 {
		b.WriteString("WITH ")
		for i, cte := range s.CTEs {
			if i > 0 {
				b.WriteString(",\n")
			}
			cs, cp := cte.Resolve()
			b.WriteString(cs)
			params = append(params, cp...)
		}
		b.WriteString("\n")
	}

	// SELECT
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	for i, col := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		cs, cp := col.Resolve()
		b.WriteString(cs)
		params = append(params, cp...)
	}

	// FROM (on same line as SELECT)
	fs, _ := s.From.Resolve()
	b.WriteString(" FROM ")
	b.WriteString(fs)

	// JOINs
	for _, j := range s.Joins {
		js, jp := j.Resolve()
		b.WriteString("\n  ")
		b.WriteString(js)
		params = append(params, jp...)
	}

	// WHERE
	if s.Where != nil {
		ws, wp := s.Where.Resolve()
		if ws != "" {
			b.WriteString("\n  WHERE\n    ")
			b.WriteString(ws)
			params = append(params, wp...)
		}
	}

	// GROUP BY
	if len(s.GroupBy) > 0 {
		gs, gp := s.GroupBy.Resolve()
		b.WriteString("\nGROUP BY ")
		b.WriteString(gs)
		params = append(params, gp...)
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		b.WriteString("\n  ORDER BY ")
		for i, ob := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			os, op := ob.Resolve()
			b.WriteString(os)
			params = append(params, op...)
		}
	}

	// LIMIT
	if s.Limit != nil {
		b.WriteString(fmt.Sprintf("\n  LIMIT %d", *s.Limit))
	}

	// OFFSET
	if s.Offset != nil && *s.Offset > 0 {
		b.WriteString(fmt.Sprintf("\n  OFFSET %d", *s.Offset))
	}

	return b.String(), params
}

// Subquery wraps a Select as a subexpression, e.g. for use in IN or EXISTS.
type Subquery struct {
	Query Select
}

func (sq Subquery) Resolve() (string, []any) {
	s, p := sq.Query.Resolve()
	return "(" + s + ")", p
}

// CountWrap wraps an expression in SELECT COUNT(*) FROM (...).
func CountWrap(inner Expr) Expr {
	s, p := inner.Resolve()
	return Raw{SQL: "SELECT COUNT(*) FROM (" + s + ")", Params: p}
}

// IntPtr is a helper to create a pointer to an int (for Limit/Offset).
func IntPtr(n int) *int { return &n }
