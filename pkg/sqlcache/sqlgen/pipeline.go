package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
)

// QueryState is the immutable accumulator threaded through transforms.
// Each method returns a new QueryState without modifying the receiver.
type QueryState struct {
	Columns  []sqlexpr.Expr
	From     sqlexpr.TableRef
	Joins    []sqlexpr.Join
	Where    []sqlexpr.Expr
	CTEs     []sqlexpr.WithClause
	OrderBy  []sqlexpr.OrderBy
	Limit    *int
	Offset   *int
	Distinct bool

	// Label join tracking (part of state, threaded functionally)
	LabelAliases []labelAlias // ordered for deterministic output
	labelLookup  map[string]string
	nextAliasIdx int
	usesLabels   bool

	// Context (immutable after creation)
	dbName string
	prefix string
}

type labelAlias struct {
	Name  string
	Alias string
}

// NewQueryState creates the initial state for compilation.
func NewQueryState(dbName, prefix string) QueryState {
	return QueryState{
		dbName:      dbName,
		prefix:      prefix,
		labelLookup: make(map[string]string),
	}
}

// --- Immutable "setters" (copy-on-write) ---

func (q QueryState) AddJoin(j sqlexpr.Join) QueryState {
	joins := make([]sqlexpr.Join, len(q.Joins), len(q.Joins)+1)
	copy(joins, q.Joins)
	q.Joins = append(joins, j)
	return q
}

func (q QueryState) AddJoins(js []sqlexpr.Join) QueryState {
	joins := make([]sqlexpr.Join, len(q.Joins), len(q.Joins)+len(js))
	copy(joins, q.Joins)
	q.Joins = append(joins, js...)
	return q
}

func (q QueryState) AddWhere(w sqlexpr.Expr) QueryState {
	where := make([]sqlexpr.Expr, len(q.Where), len(q.Where)+1)
	copy(where, q.Where)
	q.Where = append(where, w)
	return q
}

func (q QueryState) AddCTE(c sqlexpr.WithClause) QueryState {
	ctes := make([]sqlexpr.WithClause, len(q.CTEs), len(q.CTEs)+1)
	copy(ctes, q.CTEs)
	q.CTEs = append(ctes, c)
	return q
}

func (q QueryState) AddOrderBy(ob sqlexpr.OrderBy) QueryState {
	orderBy := make([]sqlexpr.OrderBy, len(q.OrderBy), len(q.OrderBy)+1)
	copy(orderBy, q.OrderBy)
	q.OrderBy = append(orderBy, ob)
	return q
}

func (q QueryState) SetDistinct() QueryState {
	q.Distinct = true
	return q
}

func (q QueryState) SetLimit(l int) QueryState {
	q.Limit = &l
	return q
}

func (q QueryState) SetOffset(o int) QueryState {
	q.Offset = &o
	return q
}

// EnsureLabelJoin ensures a label join exists, returning the updated state and alias.
// If the label already has a join, returns the existing alias without modification.
func (q QueryState) EnsureLabelJoin(labelName string) (QueryState, string) {
	if alias, ok := q.labelLookup[labelName]; ok {
		return q, alias
	}

	newIdx := q.nextAliasIdx + 1
	alias := labelAliasName(newIdx)

	// Clone the lookup map
	newLookup := make(map[string]string, len(q.labelLookup)+1)
	for k, v := range q.labelLookup {
		newLookup[k] = v
	}
	newLookup[labelName] = alias

	// Clone and append the ordered list
	newAliases := make([]labelAlias, len(q.LabelAliases), len(q.LabelAliases)+1)
	copy(newAliases, q.LabelAliases)
	newAliases = append(newAliases, labelAlias{Name: labelName, Alias: alias})

	q.LabelAliases = newAliases
	q.labelLookup = newLookup
	q.nextAliasIdx = newIdx
	q.usesLabels = true

	// Add the physical JOIN
	q = q.AddJoin(sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Name: q.dbName + "_labels", Alias: alias},
		On:    sqlexpr.Raw{SQL: q.prefix + ".key = " + alias + ".key"},
	})

	return q, alias
}

// EnsureLabelJoinForView registers a label alias for a CTE view (no physical table join).
func (q QueryState) EnsureLabelJoinForView(labelName, viewAlias string) QueryState {
	// Clone the lookup map
	newLookup := make(map[string]string, len(q.labelLookup)+1)
	for k, v := range q.labelLookup {
		newLookup[k] = v
	}
	newLookup[labelName] = viewAlias

	newAliases := make([]labelAlias, len(q.LabelAliases), len(q.LabelAliases)+1)
	copy(newAliases, q.LabelAliases)
	newAliases = append(newAliases, labelAlias{Name: labelName, Alias: viewAlias})

	q.LabelAliases = newAliases
	q.labelLookup = newLookup
	q.usesLabels = true

	// Add a view JOIN (no table name)
	q = q.AddJoin(sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Alias: viewAlias},
		On:    sqlexpr.Raw{SQL: q.prefix + ".key = " + viewAlias + ".key"},
	})

	return q
}

// RegisterAlias registers a label alias without creating a JOIN (for externally-managed joins).
func (q QueryState) RegisterAlias(labelName, alias string) QueryState {
	newLookup := make(map[string]string, len(q.labelLookup)+1)
	for k, v := range q.labelLookup {
		newLookup[k] = v
	}
	newLookup[labelName] = alias

	newAliases := make([]labelAlias, len(q.LabelAliases), len(q.LabelAliases)+1)
	copy(newAliases, q.LabelAliases)
	newAliases = append(newAliases, labelAlias{Name: labelName, Alias: alias})

	q.LabelAliases = newAliases
	q.labelLookup = newLookup
	q.usesLabels = true
	return q
}

// NextAlias allocates the next alias index and returns both updated state and alias name.
func (q QueryState) NextAlias() (QueryState, string) {
	newIdx := q.nextAliasIdx + 1
	q.nextAliasIdx = newIdx
	return q, labelAliasName(newIdx)
}

// AliasFor returns the alias for a given label, if it exists.
func (q QueryState) AliasFor(labelName string) (string, bool) {
	alias, ok := q.labelLookup[labelName]
	return alias, ok
}

// UsesLabels returns whether any label joins are active.
func (q QueryState) UsesLabels() bool {
	return q.usesLabels
}

// DBName returns the database name.
func (q QueryState) DBName() string {
	return q.dbName
}

// Prefix returns the field table prefix.
func (q QueryState) Prefix() string {
	return q.prefix
}

// AliasIdx returns the current alias index (for external use).
func (q QueryState) AliasIdx() int {
	return q.nextAliasIdx
}

// --- Transform and Pipeline ---

// Transform is a pure function that takes a QueryState and returns a new one.
type Transform func(QueryState) (QueryState, error)

// Pipeline composes transforms left-to-right. Each transform receives the
// output of the previous one. Short-circuits on error.
func Pipeline(transforms ...Transform) Transform {
	return func(q QueryState) (QueryState, error) {
		var err error
		for _, t := range transforms {
			q, err = t(q)
			if err != nil {
				return q, err
			}
		}
		return q, nil
	}
}

func labelAliasName(idx int) string {
	return fmt.Sprintf("lt%d", idx)
}
