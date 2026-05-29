# Plan: Replace SQL Generation in rancher/steve with Recursive Expr Tree

## Problem

`pkg/sqlcache/informer/sqlgenerator.go` (1,564 lines, 38 functions) builds SQL
through imperative string concatenation: `fmt.Sprintf`, mutable accumulators
(`filterComponentsT`), manual `.copy()`, scattered JOINs, and duplicated
switch-cases for field vs label filters. Hard to test, extend, or reason about.

## Goal

Replace with a **functional, recursive Expr tree** where each node resolves
itself to `(sql string, params []any)`. Composition replaces mutation.

---

## Architecture

```
sqltypes.ListOptions + partitions + namespace
        │
        ▼
   ┌─────────────┐
   │  Compiler   │  (ListOptions → Expr tree)
   └─────────────┘
        │
        ▼
   ┌─────────────┐
   │  Expr Tree  │  (immutable, composable, testable)
   └─────────────┘
        │
        ▼
   ┌─────────────┐
   │  Resolve()  │  (recursive walk → SQL + params)
   └─────────────┘
```

---

## Detailed Case-by-Case Mapping

### A. Field Filters (`getFieldFilter` — lines 897-976)

Each operator in the current switch becomes a single expression constructor:

| Current Op | Current code | New Expr |
|---|---|---|
| `Eq` (exact) | `fmt.Sprintf("%s = ?", fieldEntry)` | `Compare{Col{prefix, col}, "=", Param{val}}` |
| `Eq` (partial) | `fmt.Sprintf("%s LIKE ?", fieldEntry) + ESCAPE` | `Like{Col{prefix, col}, Param{fmtMatch(val)}, false}` |
| `NotEq` (exact) | `fmt.Sprintf("%s != ?", fieldEntry)` | `Compare{Col{prefix, col}, "!=", Param{val}}` |
| `NotEq` (partial) | `fmt.Sprintf("%s NOT LIKE ?", fieldEntry) + ESCAPE` | `Like{Col{prefix, col}, Param{fmtMatch(val)}, true}` |
| `Lt` | `fmt.Sprintf("%s < ?", fieldEntry)` | `Compare{Col{prefix, col}, "<", Param{numVal}}` |
| `Gt` | `fmt.Sprintf("%s > ?", fieldEntry)` | `Compare{Col{prefix, col}, ">", Param{numVal}}` |
| `In` | `fmt.Sprintf("%s IN (?,...)", fieldEntry)` | `In{Col{prefix, col}, params(matches), false}` |
| `NotIn` | `fmt.Sprintf("%s NOT IN (?,...)", fieldEntry)` | `In{Col{prefix, col}, params(matches), true}` |
| `Contains` | `fmt.Sprintf("hasBarredValue(%s, ?)", fieldEntry)` | `FuncCall{"hasBarredValue", Col{prefix, col}, Param{val}}` |
| `NotContains` | `fmt.Sprintf("NOT hasBarredValue(%s, ?)", fieldEntry)` | `Not{FuncCall{"hasBarredValue", Col{prefix, col}, Param{val}}}` |
| `Exists/NotExists` | returns error (not supported for fields) | same — return error at compile time |

**Compiler function:**
```go
func compileFieldFilter(filter sqltypes.Filter, prefix string, registry FieldRegistry) (Expr, error)
```

### B. Label Filters (`getLabelFilter` — lines 978-1097)

Labels differ from fields: they reference a JOIN alias (`lt1`, `lt2`...) and
always include a `label = ?` condition paired with the value condition.

| Current Op | Current pattern | New Expr |
|---|---|---|
| `Eq` (exact) | `lt%d.label = ? AND lt%d.value = ?` | `And{Compare{ltCol("label"), "=", Param{name}}, Compare{ltCol("value"), "=", Param{val}}}` |
| `Eq` (partial) | `lt%d.label = ? AND lt%d.value LIKE ?` | `And{Compare{ltCol("label"), "=", Param{name}}, Like{ltCol("value"), Param{pattern}, false}}` |
| `NotEq` (exact) | `(NOT EXISTS ...) OR (lt.label=? AND lt.value != ?)` | `Or{labelNotExists(name), And{labelIs(name), Compare{ltCol("value"), "!=", Param{val}}}}` |
| `NotEq` (partial) | same shape, with NOT LIKE | `Or{labelNotExists(name), And{labelIs(name), Like{ltCol("value"), Param{pat}, true}}}` |
| `Lt` | `lt%d.label = ? AND lt%d.value < ?` | `And{labelIs(name), Compare{ltCol("value"), "<", Param{num}}}` |
| `Gt` | `lt%d.label = ? AND lt%d.value > ?` | `And{labelIs(name), Compare{ltCol("value"), ">", Param{num}}}` |
| `Exists` | `lt%d.label = ?` | `Compare{ltCol("label"), "=", Param{name}}` |
| `NotExists` | subquery: `o.key NOT IN (SELECT f1.key ... WHERE lt.label = ?)` | `NotInSubquery{keyCol, subSelect}` |
| `In` | `lt%d.label = ? AND lt%d.value IN (?,...)` | `And{labelIs(name), In{ltCol("value"), params, false}}` |
| `NotIn` | `(NOT EXISTS ...) OR (lt.label=? AND lt.value NOT IN (...))` | `Or{labelNotExists(name), And{labelIs(name), In{ltCol("value"), params, true}}}` |
| `Contains` | delegates to Eq (labels can't have `\|`) | same as Eq |
| `NotContains` | delegates to NotEq | same as NotEq |

**Compiler function:**
```go
func compileLabelFilter(filter sqltypes.Filter, ltAlias string, dbName string, isSummary bool) (Expr, error)
```

**Helper constructors:**
```go
func labelIs(ltAlias, name string) Expr          // ltN.label = ?
func labelNotExists(ltAlias, dbName string) Expr  // NOT IN subquery
func ltCol(ltAlias, field string) Col             // Col{ltAlias, field}
```

### C. Projects/Namespaces Filters (`getProjectsOrNamespacesFieldFilter` + `LabelFilter`)

| Current case | New Expr |
|---|---|
| Field `In` | `In{Col{"nsf", col}, params, false}` |
| Field `NotIn` | `In{Col{"nsf", col}, params, true}` |
| Label `In` | `And{labelIs(name), In{ltCol("value"), params, false}}` |
| Label `NotIn` | `Or{And{labelIs(name), In{ltCol("value"), params, true}}, NotInSubquery{...}}` |

These also produce **extra JOINs** (namespace fields table + labels table).

**Compiler function:**
```go
func compileProjectsOrNamespaces(filter sqltypes.OrFilter, dbName string, joinCtx *JoinContext) (Expr, error)
```

### D. OR Filter Groups (`buildORClauseFromFilters` — lines 125-156)

Current: loop, accumulate clause strings, join with ") OR (".

New:
```go
func compileOrFilter(orFilter sqltypes.OrFilter, prefix, dbName string, registry FieldRegistry, joinCtx *JoinContext) (Expr, error) {
    exprs := make([]Expr, 0, len(orFilter.Filters))
    for _, f := range orFilter.Filters {
        if isLabelFilter(&f) {
            expr, _ := compileLabelFilter(f, joinCtx.AliasFor(f), dbName, false)
            exprs = append(exprs, expr)
        } else {
            expr, _ := compileFieldFilter(f, prefix, registry)
            exprs = append(exprs, expr)
        }
    }
    return Or(exprs), nil
}
```

Multiple OR groups are AND'd together at the top level.

### E. Partition Clauses (`generatePartitionClauses` — lines 431-527)

Current: complex grouping by signature hash, emitting `IN` clauses.

New: same logic, but returns `Expr` instead of `[]string`:
```go
func compilePartitions(ns string, partitions []partition.Partition, prefix string) Expr {
    // passthrough → nil (no constraint)
    // empty → False{}
    // groups → Or of And(nsIn(...), nameIn(...))
}
```

| Partition case | Expr produced |
|---|---|
| Passthrough / unrestricted | `nil` (no WHERE contribution) |
| No access (empty) | `Raw{"FALSE", nil}` |
| Full namespace access (sig=0) | `In{nsCol, namespaces, false}` |
| Restricted names in specific namespaces | `And{In{nsCol, namespaces, false}, In{nameCol, names, false}}` |
| Restricted names, all namespaces | `In{nameCol, names, false}` |
| Multiple groups | `Or{group1, group2, ...}` |

### F. Namespace Filter (simple case)

```go
func compileNamespace(ns, prefix string) Expr {
    if ns == "" || ns == "*" {
        return nil
    }
    return Compare{Col{prefix, "metadata.namespace"}, "=", Param{ns}}
}
```

### G. Sort Directives (ORDER BY)

| Current case | New Expr |
|---|---|
| Field sort ASC | `OrderBy{Col{prefix, col}, false}` |
| Field sort DESC | `OrderBy{Col{prefix, col}, true}` |
| Field sort as IP | `OrderBy{FuncCall{"inet_aton", Col{prefix, col}}, desc}` |
| Timestamp field in sort | `OrderBy{FuncCall{"adjustTimestampForSorting", Col{prefix, col}}, desc}` |
| Label sort ASC | `OrderBy{Col{ltAlias, "value"}, false}` with `NullsLast` |
| Label sort DESC | `OrderBy{Col{ltAlias, "value"}, true}` with `NullsFirst` |
| Label sort as IP | `OrderBy{FuncCall{"inet_aton", Col{ltAlias, "value"}}, desc}` |
| Default (namespaced) | `OrderBy{Col{prefix, "id"}, false}` |
| Default (cluster-scoped) | `OrderBy{Col{prefix, "metadata.name"}, false}` |

**New type needed:** `OrderBy` gains a `Nulls` field:
```go
type OrderBy struct {
    Expr      Expr
    Desc      bool
    NullsLast *bool  // nil=default, true=NULLS LAST, false=NULLS FIRST
}
```

**Unbound sort labels** (labels sorted on but not filtered on) require CTEs:
```go
func compileSortLabelCTE(labelName, dbName string, ltAlias string) WithClause {
    return WithClause{
        Name:    ltAlias,
        Columns: []string{"key", "value"},
        Body:    Raw{`SELECT key, value FROM "` + dbName + `_labels" WHERE label = ?`, []any{labelName}},
    }
}
```

### H. Pagination (LIMIT/OFFSET)

Trivial — just populated on the `Select` struct:
```go
func compilePagination(p sqltypes.Pagination) (limit *int, offset *int) {
    if p.PageSize <= 0 {
        return nil, nil
    }
    l := p.PageSize
    o := 0
    if p.Page >= 1 {
        o = p.PageSize * (p.Page - 1)
    }
    return &l, &o  // zero offset → Select ignores it
}
```

### I. JoinContext (replaces `joinTableIndexByLabelName` map)

The current code threads a mutable `map[string]int` to track which label JOINs
have been created. Replace with a structured collector:

```go
type JoinContext struct {
    dbName     string
    prefix     string  // main field prefix ("f")
    joins      []Join
    labelIndex map[string]string  // label name → alias ("lt1", "lt2"...)
    counter    int
    usesLabels bool
}

func (jc *JoinContext) EnsureLabelJoin(labelName string) string {
    if alias, ok := jc.labelIndex[labelName]; ok {
        return alias
    }
    jc.counter++
    alias := fmt.Sprintf("lt%d", jc.counter)
    jc.labelIndex[labelName] = alias
    jc.joins = append(jc.joins, Join{
        Kind:  "LEFT OUTER JOIN",
        Table: TableRef{Name: jc.dbName + "_labels", Alias: alias},
        On:    Compare{Col{jc.prefix, "key"}, "=", Col{alias, "key"}},
    })
    jc.usesLabels = true
    return alias
}
```

### J. Summary Queries

Current has 3 paths:
1. `constructSimpleSummaryQueryForStandardField` — no filters active
2. `constructSimpleSummaryQueryForLabelField` — no filters, label field
3. `constructComplexSummaryQueryForField` — filters active, needs CTE

New approach — all 3 become composition of the same building blocks:

```go
func compileSummaryQuery(field []string, dbName string, whereTree Expr, joins []Join, orderBy []OrderBy, limit, offset *int) Select {
    if whereTree == nil && len(joins) == 0 {
        // Simple case: just GROUP BY on the raw table
        return simpleGroupBy(field, dbName)
    }
    // Complex case: CTE wrapping filtered data, then GROUP BY
    return Select{
        CTEs: []WithClause{filteredCTE(whereTree, joins, orderBy, limit, offset)},
        Columns: []Expr{groupByColumns...},
        From: cteRef,
        Where: Raw{"k != ''", nil},  // exclude empty values
        GroupBy: ...
    }
}
```

No `.copy()` of filterComponents — just pass the same `whereTree` (it's immutable).

### K. Count Query

```go
// Strip pagination from the same tree, wrap in COUNT(*)
countSelect := mainSelect
countSelect.Limit = nil
countSelect.Offset = nil
countSelect.OrderBy = nil
countExpr := CountWrap(countSelect)
```

### L. Revision Check (`checkRevision`)

Stays as-is — it's a validation step before query construction, not part of SQL gen.

---

## Top-Level Compiler Flow

```go
func CompileListQuery(lo *sqltypes.ListOptions, partitions []partition.Partition,
    ns, dbName string, registry FieldRegistry, namespaced bool) (*CompiledQuery, error) {

    prefix := "f"
    objPrefix := "o"
    jc := NewJoinContext(dbName, prefix)

    // 1. Compile WHERE subclauses
    var whereParts []Expr

    for _, orFilter := range lo.Filters {
        expr, err := compileOrFilter(orFilter, prefix, dbName, registry, jc)
        // ... append to whereParts
    }

    if projExpr := compileProjectsOrNamespaces(lo.ProjectsOrNamespaces, dbName, jc); projExpr != nil {
        whereParts = append(whereParts, projExpr)
    }

    if nsExpr := compileNamespace(ns, prefix); nsExpr != nil {
        whereParts = append(whereParts, nsExpr)
    }

    if partExpr := compilePartitions(ns, partitions, prefix); partExpr != nil {
        whereParts = append(whereParts, partExpr)
    }

    // 2. Compose WHERE
    var where Expr
    if len(whereParts) > 0 {
        where = And(whereParts)
    }

    // 3. Compile ORDER BY (also may add CTEs for unbound label sorts)
    orderBy, ctes := compileSort(lo.SortList, registry, prefix, namespaced, jc)

    // 4. Compile pagination
    limit, offset := compilePagination(lo.Pagination)

    // 5. Assemble
    baseJoin := Join{
        Kind:  "JOIN",
        Table: TableRef{Name: dbName + "_fields", Alias: prefix},
        On:    Compare{Col{objPrefix, "key"}, "=", Col{prefix, "key"}},
    }

    query := Select{
        CTEs:     ctes,
        Distinct: jc.usesLabels,
        Columns:  []Expr{Col{objPrefix, "object"}, Col{objPrefix, "objectnonce"}, Col{objPrefix, "dekid"}},
        From:     TableRef{Name: dbName, Alias: objPrefix},
        Joins:    append([]Join{baseJoin}, jc.joins...),
        Where:    where,
        OrderBy:  orderBy,
        Limit:    limit,
        Offset:   offset,
    }

    return &CompiledQuery{Query: query, NeedsCount: limit != nil || offset != nil}, nil
}
```

---

## Functions: Current → New Mapping

| # | Current function | Disposition | New equivalent |
|---|---|---|---|
| 1 | `compileQuery` | **Replace** | `CompileListQuery()` |
| 2 | `generateSQL` | **Delete** | `Select.Resolve()` does this |
| 3 | `buildORClauseFromFilters` | **Replace** | `compileOrFilter()` |
| 4 | `buildClauseFromProjectsOrNamespaces` | **Replace** | `compileProjectsOrNamespaces()` |
| 5 | `getFieldFilter` | **Replace** | `compileFieldFilter()` |
| 6 | `getLabelFilter` | **Replace** | `compileLabelFilter()` |
| 7 | `getProjectsOrNamespacesFieldFilter` | **Replace** | folded into `compileProjectsOrNamespaces()` |
| 8 | `getProjectsOrNamespacesLabelFilter` | **Replace** | folded into `compileProjectsOrNamespaces()` |
| 9 | `generatePartitionClauses` | **Replace** | `compilePartitions()` |
| 10 | `namesSignatures` | **Keep** (used by partition grouping logic) | same |
| 11 | `constructQuery` | **Replace** | `CompileListQuery().Query.Resolve()` |
| 12 | `constructComplexSummaryQueryForField` | **Replace** | `compileSummaryQuery()` |
| 13 | `constructSimpleSummaryQueryForField` | **Replace** | `compileSummaryQuery()` (simple path) |
| 14 | `constructSimpleSummaryQueryForLabelField` | **Replace** | `compileSummaryQuery()` (label path) |
| 15 | `constructSimpleSummaryQueryForStandardField` | **Replace** | `compileSummaryQuery()` (standard path) |
| 16 | `constructSummaryQueryForField` | **Replace** | `compileSummaryQuery()` (dispatch) |
| 17 | `ListSummaryFields` | **Simplify** | calls `compileSummaryQuery` per field |
| 18 | `ListSummaryForField` | **Simplify** | compile + resolve + execute |
| 19 | `executeSummaryQueryForField` | **Keep** (execution, not generation) | unchanged |
| 20 | `executeSummaryQuery` | **Keep** | unchanged |
| 21 | `executeQuery` | **Keep** | unchanged |
| 22 | `checkRevision` | **Keep** | unchanged (validation, not SQL gen) |
| 23 | `getValidFieldEntry` | **Move** | becomes `FieldRegistry.Resolve(prefix, fields)` |
| 24 | `getStandardColumnNameToDisplay` | **Move** | `FieldRegistry.DisplayColumn(fields)` |
| 25 | `isIntegerField` | **Move** | `FieldRegistry.IsInteger(fields)` |
| 26 | `filterComponentsT` | **Delete** | replaced by Expr tree |
| 27 | `filterComponentsT.copy()` | **Delete** | trees are immutable |
| 28 | `joinPart` struct | **Delete** | replaced by `Join` Expr node |
| 29 | `withPart` struct | **Delete** | replaced by `WithClause` Expr node |
| 30 | `buildSortLabelsClause` | **Replace** | `compileSortLabel()` returns `OrderBy` |
| 31 | `getWithParts` | **Delete** | `compileSortLabelCTE()` |
| 32 | `getWithPartsForCompiling` | **Delete** | `compileSortLabelCTE()` |
| 33 | `getUnboundSortLabels` | **Keep** (analysis, not SQL gen) | same |
| 34 | `internLabel` | **Replace** | `JoinContext.EnsureLabelJoin()` |
| 35 | `hasLabelFilter` | **Delete** | `JoinContext.usesLabels` tracks this |
| 36 | `formatMatchTarget` | **Keep** (string escaping utility) | same, used by compiler |
| 37 | `formatMatchTargetWithFormatter` | **Keep** | same |
| 38 | `prepareComparisonParameters` | **Keep** (validation) | same, used by compiler |
| 39 | `getField` | **Keep** (runtime value extraction, not SQL gen) | same |
| 40 | `extractSubFields` | **Keep** | same |
| 41 | `getLabelColumnNameToDisplay` | **Move** | to FieldRegistry or summary compiler |
| 42 | `convertMapToAPISummary` | **Keep** (post-processing, not SQL gen) | same |
| 43 | `smartJoin` | **Keep** (utility) | same |
| 44 | `toColumnName` | **Keep** (utility) | same |
| 45 | `isLabelFilter` | **Keep** (classification utility) | same |
| 46 | `isLabelsFieldList` | **Keep** (classification utility) | same |
| 47 | `logLongQuery` | **Keep** (observability) | same |

**Summary: 18 functions replaced/deleted, 18 kept as-is, 5 moved to FieldRegistry.**

---

## New Expr Node Types Needed

| Node | Resolves to | Used by |
|---|---|---|
| `Raw{SQL, Params}` | literal SQL passthrough | CountWrap, FALSE, subqueries |
| `Col{Table, Name}` | `table."name"` | everywhere |
| `Param{Value}` | `?` with bound value | everywhere |
| `And([]Expr)` | `(a) AND (b) AND ...` | filter composition |
| `Or([]Expr)` | `(a) OR (b) OR ...` | OR filter groups, partitions |
| `Not{Expr}` | `NOT (expr)` | NotContains |
| `Compare{L, Op, R}` | `L op R` | =, !=, <, > |
| `Like{Col, Pattern, Neg}` | `col [NOT] LIKE ? ESCAPE '\'` | partial matching |
| `In{Expr, Values, Neg}` | `expr [NOT] IN (?, ?, ...)` | In, NotIn, partitions |
| `FuncCall{Name, Args}` | `name(args...)` | hasBarredValue, inet_aton, extractBarredValue, adjustTimestampForSorting |
| `Subquery{Select}` | `(SELECT ...)` | NOT EXISTS label checks |
| `Select{...}` | full SELECT statement | top-level, CTEs |
| `Join{Kind, Table, On}` | `KIND "table" alias ON cond` | all JOINs |
| `TableRef{Name, Alias}` | `"name" alias` | FROM, JOIN targets |
| `OrderBy{Expr, Desc, Nulls}` | `expr ASC/DESC [NULLS ...]` | sorting |
| `WithClause{Name, Cols, Body}` | `name(cols) AS (body)` | unbound label sorts |
| `GroupBy{Expr}` | `GROUP BY expr` | summary queries |
| `CountWrap(Expr)` | `SELECT COUNT(*) FROM (expr)` | pagination count |

---

## Phases — ALL COMPLETED ✅

### Phase 1: ✅ Expression Tree (`pkg/sqlcache/sqlexpr/`)
- `expr.go`, `logic.go`, `compare.go`, `query.go`, `resolve.go`
- 28 unit tests passing
- Commit: `feat(sqlexpr): add recursive SQL expression tree package`

### Phase 2: ✅ Initial Compiler (`pkg/sqlcache/sqlgen/`)
- Compiler translating ListOptions → Expr tree
- 11 unit tests passing
- Commit: `feat(sqlgen): add compiler package for ListOptions → Expr tree`

### Phase 3: ✅ Wire into Informer
- `constructQuery` + `ListSummaryFields` use new compiler
- All 42 informer tests pass, integration tests pass
- Commits: `feat(informer): wire recursive SQL compiler into constructQuery` + `feat(informer): wire summary queries through new compiler`

### Phase 4: ✅ Dead Code Removal
- Deleted `generateSQL`, `executeSummaryQuery`, `getWithParts`, `ListSummaryForField`
- Old internal functions retained only for test compatibility (tests call them directly)

### Phase 5: ✅ Functional Pipeline Rewrite
- Replaced imperative switch-based compiler with truly functional architecture:
  - **`QueryState`**: immutable accumulator (copy-on-write, clones slices/maps)
  - **`Transform`**: `func(QueryState) → (QueryState, error)` — pure function
  - **`Pipeline`**: left-to-right transform composition
  - **`FieldOps`/`LabelOps`/`ProjectOps`**: `map[Op]Handler` dispatch (no switch statements)
- Entry point: `Pipeline(WithUnboundSortLabels, WithFilters, WithProjects, WithNamespace, WithPartitions, WithSort, WithPagination)`
- Adding a new op = one handler function + one map entry
- Integration tests pass
- Commit: `refactor(sqlgen): rewrite compiler as functional pipeline with op registries`

---

## Final File Structure

```
pkg/sqlcache/sqlgen/
├── pipeline.go          — QueryState + Transform + Pipeline (core abstractions)
├── ops_field.go         — FieldOps registry (8 handlers, no switch)
├── ops_label.go         — LabelOps registry (10 handlers, no switch)
├── ops_project.go       — ProjectOps registry (4 handlers)
├── transforms.go        — WithFilters, WithSort, WithProjects, WithPagination...
├── compile.go           — CompileListQuery/CompileSummaryListQuery (pipeline orchestration)
├── compile_summary.go   — Summary field query builder
├── compile_partition.go — Partition RBAC compilation
├── field_registry.go    — Field path → column resolution
├── helpers.go           — Shared utilities (formatMatchTarget, etc.)
└── errors.go

pkg/sqlcache/sqlexpr/
├── expr.go              — Expr interface + Raw, Col, Param, Compare, Like, In, FuncCall
├── logic.go             — And, Or, Not, FlatAnd, FlatOr, InlineAnd
├── query.go             — Select, Join, OrderBy, WithClause, GroupBy, CountWrap, TableRef
├── resolve.go           — joinExprs, joinExprsFlat helpers
└── expr_test.go         — 28 tests
```

---

## Remaining (Optional)

| Item | Notes |
|------|-------|
| Remove old test-only code in `sqlgenerator.go` | Requires rewriting ~800 lines of tests to call new compiler |
| Add more sqlgen pipeline tests | Edge cases for transform composition |
| Package-level documentation | README.md in `sqlgen/` and `sqlexpr/` |
