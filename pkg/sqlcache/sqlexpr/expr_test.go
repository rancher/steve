package sqlexpr

import (
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

func TestCol_WithoutTable(t *testing.T) {
	c := Col{Name: "key"}
	sql, params := c.Resolve()
	assertEqual(t, `"key"`, sql)
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
	sql, p := exprs[1].Resolve()
	assertEqual(t, "?", sql)
	assertEqual(t, "b", p[0].(string))
}

func TestFalse(t *testing.T) {
	sql, params := False.Resolve()
	assertEqual(t, "FALSE", sql)
	assertLen(t, 0, params)
}

// --- Logic ---

func TestAnd_Empty(t *testing.T) {
	sql, params := And{}.Resolve()
	assertEqual(t, "", sql)
	assertLen(t, 0, params)
}

func TestAnd_Single(t *testing.T) {
	sql, params := And{Raw{SQL: "x = 1"}}.Resolve()
	assertEqual(t, "x = 1", sql)
	assertLen(t, 0, params)
}

func TestAnd_Multiple(t *testing.T) {
	a := And{
		Compare{Col{Table: "f", Name: "a"}, "=", Param{1}},
		Compare{Col{Table: "f", Name: "b"}, ">", Param{2}},
	}
	sql, params := a.Resolve()
	assertEqual(t, `(f."a" = ?) AND (f."b" > ?)`, sql)
	assertLen(t, 2, params)
}

func TestOr_Multiple(t *testing.T) {
	o := Or{
		Compare{Col{Table: "f", Name: "x"}, "=", Param{"a"}},
		Compare{Col{Table: "f", Name: "x"}, "=", Param{"b"}},
	}
	sql, params := o.Resolve()
	assertEqual(t, `(f."x" = ?) OR (f."x" = ?)`, sql)
	assertLen(t, 2, params)
}

func TestNot(t *testing.T) {
	n := Not{Inner: Raw{SQL: "exists(x)"}}
	sql, params := n.Resolve()
	assertEqual(t, "NOT (exists(x))", sql)
	assertLen(t, 0, params)
}

func TestNot_Empty(t *testing.T) {
	n := Not{Inner: Raw{SQL: ""}}
	sql, _ := n.Resolve()
	assertEqual(t, "", sql)
}

// --- Compare ---

func TestCompare(t *testing.T) {
	c := Compare{Col{Table: "o", Name: "key"}, "=", Col{Table: "f", Name: "key"}}
	sql, params := c.Resolve()
	assertEqual(t, `o."key" = f."key"`, sql)
	assertLen(t, 0, params)
}

func TestLike(t *testing.T) {
	l := Like{
		Col:     Col{Table: "f", Name: "name"},
		Pattern: Param{"%test%"},
		Negate:  false,
	}
	sql, params := l.Resolve()
	assertEqual(t, `f."name" LIKE ? ESCAPE '\'`, sql)
	assertLen(t, 1, params)
}

func TestLike_Negated(t *testing.T) {
	l := Like{
		Col:     Col{Table: "f", Name: "name"},
		Pattern: Param{"%test%"},
		Negate:  true,
	}
	sql, _ := l.Resolve()
	assertEqual(t, `f."name" NOT LIKE ? ESCAPE '\'`, sql)
}

func TestIn(t *testing.T) {
	i := In{
		Expr:   Col{Table: "f", Name: "ns"},
		Values: Params("a", "b", "c"),
		Negate: false,
	}
	sql, params := i.Resolve()
	assertEqual(t, `f."ns" IN (?, ?, ?)`, sql)
	assertLen(t, 3, params)
}

func TestIn_Negated(t *testing.T) {
	i := In{
		Expr:   Col{Table: "f", Name: "ns"},
		Values: Params("x"),
		Negate: true,
	}
	sql, _ := i.Resolve()
	assertEqual(t, `f."ns" NOT IN (?)`, sql)
}

func TestFuncCall_Single(t *testing.T) {
	f := Func1("inet_aton", Col{Table: "f", Name: "ip"})
	sql, params := f.Resolve()
	assertEqual(t, `inet_aton(f."ip")`, sql)
	assertLen(t, 0, params)
}

func TestFuncCall_Multi(t *testing.T) {
	f := Func2("hasBarredValue", Col{Table: "f", Name: "containers"}, Param{"nginx"})
	sql, params := f.Resolve()
	assertEqual(t, `hasBarredValue(f."containers", ?)`, sql)
	assertLen(t, 1, params)
}

// --- Query ---

func TestTableRef_WithAlias(t *testing.T) {
	tr := TableRef{Name: "_v1_Pod", Alias: "o"}
	sql, _ := tr.Resolve()
	assertEqual(t, `"_v1_Pod" o`, sql)
}

func TestTableRef_WithoutAlias(t *testing.T) {
	tr := TableRef{Name: "my_table"}
	sql, _ := tr.Resolve()
	assertEqual(t, `"my_table"`, sql)
}

func TestJoin(t *testing.T) {
	j := Join{
		Kind:  InnerJoin,
		Table: TableRef{Name: "_v1_Pod_fields", Alias: "f"},
		On:    Compare{Col{Table: "o", Name: "key"}, "=", Col{Table: "f", Name: "key"}},
	}
	sql, params := j.Resolve()
	assertEqual(t, `JOIN "_v1_Pod_fields" f ON o."key" = f."key"`, sql)
	assertLen(t, 0, params)
}

func TestOrderBy_ASC(t *testing.T) {
	o := OrderBy{Expr: Col{Table: "f", Name: "name"}, Desc: false}
	sql, _ := o.Resolve()
	assertEqual(t, `f."name" ASC`, sql)
}

func TestOrderBy_DESC_NullsFirst(t *testing.T) {
	o := OrderBy{Expr: Col{Table: "lt1", Name: "value"}, Desc: true, Nulls: NullsFirst}
	sql, _ := o.Resolve()
	assertEqual(t, `lt1."value" DESC NULLS FIRST`, sql)
}

func TestOrderBy_WithFunc(t *testing.T) {
	o := OrderBy{Expr: Func1("inet_aton", Col{Table: "f", Name: "ip"}), Desc: false}
	sql, _ := o.Resolve()
	assertEqual(t, `inet_aton(f."ip") ASC`, sql)
}

func TestWithClause(t *testing.T) {
	w := WithClause{
		Name:    "lt1",
		Columns: []string{"key", "value"},
		Body:    Raw{SQL: `SELECT key, value FROM "_v1_Pod_labels" WHERE label = ?`, Params: []any{"app"}},
	}
	sql, params := w.Resolve()
	expected := "lt1(key, value) AS (\nSELECT key, value FROM \"_v1_Pod_labels\" WHERE label = ?\n)"
	assertEqual(t, expected, sql)
	assertLen(t, 1, params)
}

func TestGroupBy(t *testing.T) {
	g := GroupBy{Col{Name: "status"}, Col{Name: "phase"}}
	sql, _ := g.Resolve()
	assertEqual(t, `"status", "phase"`, sql)
}

func TestSubquery(t *testing.T) {
	sq := Subquery{
		Query: Select{
			Columns: []Expr{Col{Table: "f1", Name: "key"}},
			From:    TableRef{Name: "test_fields", Alias: "f1"},
		},
	}
	sql, _ := sq.Resolve()
	assertEqual(t, `(SELECT f1."key"
FROM "test_fields" f1)`, sql)
}

func TestCountWrap(t *testing.T) {
	inner := Raw{SQL: "SELECT x FROM t WHERE y = ?", Params: []any{1}}
	sql, params := CountWrap(inner).Resolve()
	assertEqual(t, "SELECT COUNT(*) FROM (SELECT x FROM t WHERE y = ?)", sql)
	assertLen(t, 1, params)
}

func TestSelect_Full(t *testing.T) {
	limit := 25
	offset := 50
	s := Select{
		Distinct: true,
		Columns: []Expr{
			Col{Table: "o", Name: "object"},
			Col{Table: "o", Name: "dekid"},
		},
		From: TableRef{Name: "_v1_Pod", Alias: "o"},
		Joins: []Join{
			{
				Kind:  InnerJoin,
				Table: TableRef{Name: "_v1_Pod_fields", Alias: "f"},
				On:    Compare{Col{Table: "o", Name: "key"}, "=", Col{Table: "f", Name: "key"}},
			},
			{
				Kind:  LeftOuterJoin,
				Table: TableRef{Name: "_v1_Pod_labels", Alias: "lt1"},
				On:    Compare{Col{Table: "f", Name: "key"}, "=", Col{Table: "lt1", Name: "key"}},
			},
		},
		Where: And{
			Compare{Col{Table: "f", Name: "status.phase"}, "=", Param{"Running"}},
			In{Expr: Col{Table: "f", Name: "metadata.namespace"}, Values: Params("ns1", "ns2")},
		},
		OrderBy: []OrderBy{
			{Expr: Col{Table: "f", Name: "metadata.name"}, Desc: false},
		},
		Limit:  &limit,
		Offset: &offset,
	}

	sql, params := s.Resolve()
	expected := `SELECT DISTINCT o."object", o."dekid"
FROM "_v1_Pod" o
  JOIN "_v1_Pod_fields" f ON o."key" = f."key"
  LEFT OUTER JOIN "_v1_Pod_labels" lt1 ON f."key" = lt1."key"
WHERE (f."status.phase" = ?) AND (f."metadata.namespace" IN (?, ?))
ORDER BY f."metadata.name" ASC
LIMIT 25
OFFSET 50`
	assertEqual(t, expected, sql)
	assertLen(t, 3, params)
	assertEqual(t, "Running", params[0].(string))
	assertEqual(t, "ns1", params[1].(string))
	assertEqual(t, "ns2", params[2].(string))
}

func TestSelect_WithCTEs(t *testing.T) {
	s := Select{
		CTEs: []WithClause{
			{
				Name:    "lt1",
				Columns: []string{"key", "value"},
				Body:    Raw{SQL: `SELECT key, value FROM "t_labels" WHERE label = ?`, Params: []any{"app"}},
			},
		},
		Columns: []Expr{Col{Table: "o", Name: "object"}},
		From:    TableRef{Name: "t", Alias: "o"},
		Joins: []Join{
			{
				Kind:  LeftOuterJoin,
				Table: TableRef{Name: "", Alias: "lt1"},
				On:    Compare{Col{Table: "f", Name: "key"}, "=", Col{Table: "lt1", Name: "key"}},
			},
		},
		OrderBy: []OrderBy{
			{Expr: Col{Table: "lt1", Name: "value"}, Desc: true, Nulls: NullsFirst},
		},
	}
	sql, params := s.Resolve()
	// Verify CTE is present
	assertContains(t, sql, "WITH lt1(key, value) AS (")
	assertContains(t, sql, "ORDER BY lt1.\"value\" DESC NULLS FIRST")
	assertLen(t, 1, params)
}

func TestSelect_NoWhere(t *testing.T) {
	s := Select{
		Columns: []Expr{Raw{SQL: "*"}},
		From:    TableRef{Name: "t"},
	}
	sql, _ := s.Resolve()
	assertEqual(t, "SELECT *\nFROM \"t\"", sql)
}

func TestSelect_NilWhereExpr(t *testing.T) {
	s := Select{
		Columns: []Expr{Raw{SQL: "1"}},
		From:    TableRef{Name: "t"},
		Where:   nil,
	}
	sql, _ := s.Resolve()
	// Should not contain WHERE
	assertNotContains(t, sql, "WHERE")
}

// --- Helpers ---

func assertEqual(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("\nexpected: %s\n  actual: %s", expected, actual)
	}
}

func assertLen(t *testing.T, expected int, slice []any) {
	t.Helper()
	if len(slice) != expected {
		t.Errorf("expected length %d, got %d: %v", expected, len(slice), slice)
	}
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !contains(s, substr) {
		t.Errorf("expected string to contain %q, got:\n%s", substr, s)
	}
}

func assertNotContains(t *testing.T, s, substr string) {
	t.Helper()
	if contains(s, substr) {
		t.Errorf("expected string NOT to contain %q, got:\n%s", substr, s)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
