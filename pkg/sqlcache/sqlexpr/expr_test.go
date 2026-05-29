package sqlexpr

import (
	"strings"
	"testing"
)

func TestRaw(t *testing.T) {
	r := Raw{SQL: "SELECT 1", Params: []any{}}
	sql, params := r.Resolve()
	assertEqual(t, "SELECT 1", sql)
	assertLen(t, 0, params)
}

func TestRawWithParams(t *testing.T) {
	r := Raw{SQL: "x = ?", Params: []any{"hello"}}
	sql, params := r.Resolve()
	assertEqual(t, "x = ?", sql)
	assertLen(t, 1, params)
	assertEqual(t, "hello", params[0].(string))
}

func TestCol_WithTable(t *testing.T) {
	c := Col{Table: "f", Name: "metadata.name"}
	sql, params := c.Resolve()
	assertEqual(t, `f."metadata.name"`, sql)
	assertLen(t, 0, params)
}

func TestCol_WithoutTable_Simple(t *testing.T) {
	// Simple identifiers (no dots, no special chars) are unquoted
	c := Col{Name: "key"}
	sql, params := c.Resolve()
	assertEqual(t, "key", sql)
	assertLen(t, 0, params)
}

func TestCol_WithoutTable_Complex(t *testing.T) {
	c := Col{Name: "metadata.name"}
	sql, params := c.Resolve()
	assertEqual(t, `"metadata.name"`, sql)
	assertLen(t, 0, params)
}

func TestCol_WithTable_Simple(t *testing.T) {
	// Simple column names like "key", "value", "label" are unquoted
	c := Col{Table: "lt1", Name: "value"}
	sql, params := c.Resolve()
	assertEqual(t, "lt1.value", sql)
	assertLen(t, 0, params)
}

func TestParam(t *testing.T) {
	p := Param{Value: 42}
	sql, params := p.Resolve()
	assertEqual(t, "?", sql)
	assertLen(t, 1, params)
	if params[0].(int) != 42 {
		t.Errorf("expected 42, got %v", params[0])
	}
}

func TestParams(t *testing.T) {
	exprs := Params("a", "b", "c")
	if len(exprs) != 3 {
		t.Fatalf("expected 3 exprs, got %d", len(exprs))
	}
	for _, e := range exprs {
		sql, _ := e.Resolve()
		assertEqual(t, "?", sql)
	}
}

func TestAnd_Single(t *testing.T) {
	a := And{Raw{SQL: "x = 1"}}
	sql, _ := a.Resolve()
	assertEqual(t, "x = 1", sql)
}

func TestAnd_Multiple(t *testing.T) {
	a := And{
		Compare{Col{Table: "f", Name: "a"}, "=", Param{Value: 1}},
		Compare{Col{Table: "f", Name: "b"}, ">", Param{Value: 2}},
	}
	sql, params := a.Resolve()
	// And wraps in parens and separates with newline-indented AND
	assertContains(t, sql, `(f.a = ?) AND`)
	assertContains(t, sql, `(f.b > ?)`)
	assertLen(t, 2, params)
}

func TestOr_Multiple(t *testing.T) {
	o := Or{
		Compare{Col{Table: "f", Name: "x"}, "=", Param{Value: 1}},
		Compare{Col{Table: "f", Name: "x"}, "=", Param{Value: 2}},
	}
	sql, params := o.Resolve()
	assertEqual(t, "(f.x = ?) OR (f.x = ?)", sql)
	assertLen(t, 2, params)
}

func TestFlatAnd(t *testing.T) {
	fa := FlatAnd{
		Compare{Col{Table: "lt1", Name: "label"}, "=", Param{Value: "app"}},
		Compare{Col{Table: "lt1", Name: "value"}, "=", Param{Value: "v1"}},
	}
	sql, params := fa.Resolve()
	assertEqual(t, "lt1.label = ? AND lt1.value = ?", sql)
	assertLen(t, 2, params)
}

func TestFlatOr(t *testing.T) {
	fo := FlatOr{
		Raw{SQL: "NOT EXISTS (subquery)"},
		FlatAnd{
			Compare{Col{Table: "lt1", Name: "label"}, "=", Param{Value: "app"}},
			Compare{Col{Table: "lt1", Name: "value"}, "!=", Param{Value: "v1"}},
		},
	}
	sql, params := fo.Resolve()
	assertEqual(t, "(NOT EXISTS (subquery)) OR (lt1.label = ? AND lt1.value != ?)", sql)
	assertLen(t, 2, params)
}

func TestInlineAnd(t *testing.T) {
	ia := InlineAnd{
		In{Expr: Col{Table: "nsf", Name: "metadata.name"}, Values: Params("ns1"), Negate: true},
		Raw{SQL: "lt1.label = ? AND lt1.value != ?", Params: []any{"proj", "v1"}},
	}
	sql, params := ia.Resolve()
	assertContains(t, sql, `(nsf."metadata.name" NOT IN (?)) AND (lt1.label = ? AND lt1.value != ?)`)
	assertLen(t, 3, params)
}

func TestNot(t *testing.T) {
	n := Not{Inner: Raw{SQL: "exists(x)"}}
	sql, _ := n.Resolve()
	assertEqual(t, "NOT exists(x)", sql)
}

func TestCompare(t *testing.T) {
	c := Compare{Left: Col{Table: "o", Name: "key"}, Op: "=", Right: Col{Table: "f", Name: "key"}}
	sql, params := c.Resolve()
	assertEqual(t, "o.key = f.key", sql)
	assertLen(t, 0, params)
}

func TestLike(t *testing.T) {
	l := Like{Col: Col{Table: "f", Name: "metadata.name"}, Pattern: Param{Value: "%test%"}}
	sql, params := l.Resolve()
	assertEqual(t, `f."metadata.name" LIKE ? ESCAPE '\'`, sql)
	assertLen(t, 1, params)
}

func TestLike_Negated(t *testing.T) {
	l := Like{Col: Col{Table: "f", Name: "metadata.name"}, Pattern: Param{Value: "%test%"}, Negate: true}
	sql, params := l.Resolve()
	assertEqual(t, `f."metadata.name" NOT LIKE ? ESCAPE '\'`, sql)
	assertLen(t, 1, params)
}

func TestIn(t *testing.T) {
	in := In{Expr: Col{Table: "f", Name: "metadata.namespace"}, Values: Params("ns1", "ns2", "ns3")}
	sql, params := in.Resolve()
	assertEqual(t, `f."metadata.namespace" IN (?, ?, ?)`, sql)
	assertLen(t, 3, params)
}

func TestIn_Negated(t *testing.T) {
	in := In{Expr: Col{Table: "f", Name: "metadata.namespace"}, Values: Params("ns1"), Negate: true}
	sql, params := in.Resolve()
	assertEqual(t, `f."metadata.namespace" NOT IN (?)`, sql)
	assertLen(t, 1, params)
}

func TestFuncCall_Single(t *testing.T) {
	f := FuncCall{Name: "inet_aton", Args: []Expr{Col{Table: "f", Name: "status.podIP"}}}
	sql, params := f.Resolve()
	assertEqual(t, `inet_aton(f."status.podIP")`, sql)
	assertLen(t, 0, params)
}

func TestFuncCall_Multi(t *testing.T) {
	f := FuncCall{Name: "hasBarredValue", Args: []Expr{Col{Table: "f", Name: "spec.containers"}, Param{Value: "nginx"}}}
	sql, params := f.Resolve()
	assertEqual(t, `hasBarredValue(f."spec.containers", ?)`, sql)
	assertLen(t, 1, params)
}

func TestTableRef(t *testing.T) {
	tr := TableRef{Name: "_v1_Pod", Alias: "o"}
	sql, _ := tr.Resolve()
	assertEqual(t, `"_v1_Pod" o`, sql)
}

func TestTableRef_NoAlias(t *testing.T) {
	tr := TableRef{Name: "_v1_Pod"}
	sql, _ := tr.Resolve()
	assertEqual(t, `"_v1_Pod"`, sql)
}

func TestTableRef_AliasOnly(t *testing.T) {
	tr := TableRef{Alias: "lt1"}
	sql, _ := tr.Resolve()
	assertEqual(t, "lt1", sql)
}

func TestJoin(t *testing.T) {
	j := Join{
		Kind:  InnerJoin,
		Table: TableRef{Name: "_v1_Pod_fields", Alias: "f"},
		On:    Compare{Left: Col{Table: "o", Name: "key"}, Op: "=", Right: Col{Table: "f", Name: "key"}},
	}
	sql, _ := j.Resolve()
	assertEqual(t, `JOIN "_v1_Pod_fields" f ON o.key = f.key`, sql)
}

func TestOrderBy_ASC(t *testing.T) {
	o := OrderBy{Expr: Col{Table: "f", Name: "metadata.name"}, Desc: false}
	sql, _ := o.Resolve()
	assertEqual(t, `f."metadata.name" ASC`, sql)
}

func TestOrderBy_DESC_NullsFirst(t *testing.T) {
	o := OrderBy{Expr: Col{Table: "lt1", Name: "value"}, Desc: true, Nulls: NullsFirst}
	sql, _ := o.Resolve()
	assertEqual(t, "lt1.value DESC NULLS FIRST", sql)
}

func TestOrderBy_WithFunc(t *testing.T) {
	o := OrderBy{Expr: FuncCall{Name: "inet_aton", Args: []Expr{Col{Table: "f", Name: "status.podIP"}}}}
	sql, _ := o.Resolve()
	assertEqual(t, `inet_aton(f."status.podIP") ASC`, sql)
}

func TestWithClause(t *testing.T) {
	wc := WithClause{
		Name:    "lt1",
		Columns: []string{"key", "value"},
		Body:    Raw{SQL: `SELECT key, value FROM "t_labels" WHERE label = ?`, Params: []any{"app"}},
	}
	sql, params := wc.Resolve()
	assertContains(t, sql, "lt1(key, value) AS")
	assertContains(t, sql, `SELECT key, value FROM "t_labels" WHERE label = ?`)
	assertLen(t, 1, params)
}

func TestGroupBy(t *testing.T) {
	g := GroupBy{Col{Name: "status"}, Col{Name: "phase"}}
	sql, _ := g.Resolve()
	assertEqual(t, "status, phase", sql)
}

func TestSubquery(t *testing.T) {
	s := Subquery{
		Query: Select{
			Columns: []Expr{Col{Table: "f1", Name: "key"}},
			From:    TableRef{Name: "test_fields", Alias: "f1"},
		},
	}
	sql, _ := s.Resolve()
	assertContains(t, sql, `SELECT f1.key FROM "test_fields" f1`)
}

func TestCountWrap(t *testing.T) {
	inner := Select{
		Columns: []Expr{Raw{SQL: "*"}},
		From:    TableRef{Name: "test", Alias: "t"},
	}
	cw := CountWrap(inner)
	sql, _ := cw.Resolve()
	assertContains(t, sql, "SELECT COUNT(*) FROM")
	assertContains(t, sql, `SELECT * FROM "test" t`)
}

func TestSelect_Simple(t *testing.T) {
	s := Select{
		Columns: []Expr{Raw{SQL: "*"}},
		From:    TableRef{Name: "t"},
	}
	sql, _ := s.Resolve()
	assertEqual(t, `SELECT * FROM "t"`, sql)
}

func TestSelect_Full(t *testing.T) {
	limit := 25
	offset := 50
	s := Select{
		Distinct: true,
		Columns:  []Expr{Col{Table: "o", Name: "object"}, Col{Table: "o", Name: "dekid"}},
		From:     TableRef{Name: "_v1_Pod", Alias: "o"},
		Joins: []Join{
			{Kind: InnerJoin, Table: TableRef{Name: "_v1_Pod_fields", Alias: "f"}, On: Compare{Left: Col{Table: "o", Name: "key"}, Op: "=", Right: Col{Table: "f", Name: "key"}}},
			{Kind: LeftOuterJoin, Table: TableRef{Name: "_v1_Pod_labels", Alias: "lt1"}, On: Compare{Left: Col{Table: "f", Name: "key"}, Op: "=", Right: Col{Table: "lt1", Name: "key"}}},
		},
		Where: And{
			Compare{Left: Col{Table: "f", Name: "status.phase"}, Op: "=", Right: Param{Value: "Running"}},
			In{Expr: Col{Table: "f", Name: "metadata.namespace"}, Values: Params("ns1", "ns2")},
		},
		OrderBy: []OrderBy{{Expr: Col{Table: "f", Name: "metadata.name"}}},
		Limit:    &limit,
		Offset:   &offset,
	}
	sql, params := s.Resolve()
	assertContains(t, sql, "SELECT DISTINCT")
	assertContains(t, sql, `o.object, o.dekid FROM "_v1_Pod" o`)
	assertContains(t, sql, `JOIN "_v1_Pod_fields" f ON o.key = f.key`)
	assertContains(t, sql, `LEFT OUTER JOIN "_v1_Pod_labels" lt1 ON f.key = lt1.key`)
	assertContains(t, sql, `f."status.phase" = ?`)
	assertContains(t, sql, `f."metadata.namespace" IN (?, ?)`)
	assertContains(t, sql, `ORDER BY f."metadata.name" ASC`)
	assertContains(t, sql, "LIMIT 25")
	assertContains(t, sql, "OFFSET 50")
	assertLen(t, 3, params)
}

// Helper functions
func assertEqual(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("\nexpected: %s\n  actual: %s", expected, actual)
	}
}

func assertLen(t *testing.T, expected int, params []any) {
	t.Helper()
	if len(params) != expected {
		t.Errorf("expected %d params, got %d", expected, len(params))
	}
}

func assertContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Errorf("expected string to contain %q, got:\n%s", sub, s)
	}
}
