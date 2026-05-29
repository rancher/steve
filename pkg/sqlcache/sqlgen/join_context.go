package sqlgen

import (
	"fmt"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
)

// JoinContext tracks label JOINs needed during query compilation.
// It replaces the mutable map[string]int + manual JOIN assembly in the old code.
type JoinContext struct {
	dbName     string
	prefix     string // main field table prefix (e.g., "f" or "f1")
	joins      []sqlexpr.Join
	labelIndex map[string]string // label name → alias ("lt1", "lt2"...)
	counter    int
	UsesLabels bool
}

// NewJoinContext creates a new JoinContext for tracking label JOINs.
func NewJoinContext(dbName, prefix string) *JoinContext {
	return &JoinContext{
		dbName:     dbName,
		prefix:     prefix,
		labelIndex: make(map[string]string),
	}
}

// EnsureLabelJoin ensures a LEFT OUTER JOIN exists for the given label name.
// Returns the alias (e.g., "lt1") for use in expressions.
func (jc *JoinContext) EnsureLabelJoin(labelName string) string {
	if alias, ok := jc.labelIndex[labelName]; ok {
		return alias
	}
	jc.counter++
	alias := fmt.Sprintf("lt%d", jc.counter)
	jc.labelIndex[labelName] = alias
	jc.joins = append(jc.joins, sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Name: fmt.Sprintf("%s_labels", jc.dbName), Alias: alias},
		On: sqlexpr.Raw{
			SQL: fmt.Sprintf("%s.key = %s.key", jc.prefix, alias),
		},
	})
	jc.UsesLabels = true
	return alias
}

// EnsureLabelJoinForView creates a LEFT OUTER JOIN on a CTE view (for unbound sort labels).
// The view is already defined in a WITH clause, so we don't reference a physical table.
func (jc *JoinContext) EnsureLabelJoinForView(labelName string, viewAlias string) {
	jc.labelIndex[labelName] = viewAlias
	jc.joins = append(jc.joins, sqlexpr.Join{
		Kind:  sqlexpr.LeftOuterJoin,
		Table: sqlexpr.TableRef{Alias: viewAlias}, // No Name means it references a CTE
		On: sqlexpr.Raw{
			SQL: fmt.Sprintf("%s.key = %s.key", jc.prefix, viewAlias),
		},
	})
	jc.UsesLabels = true
}

// AliasFor returns the alias for a given label name, if it exists.
func (jc *JoinContext) AliasFor(labelName string) (string, bool) {
	alias, ok := jc.labelIndex[labelName]
	return alias, ok
}

// RegisterAlias registers a label name with a given alias without creating a JOIN.
// Used when the JOIN is managed externally (e.g., projects/namespaces joins).
func (jc *JoinContext) RegisterAlias(labelName, alias string) {
	jc.labelIndex[labelName] = alias
	jc.UsesLabels = true
}

// NextAlias allocates the next alias number and returns it (e.g., "lt3").
func (jc *JoinContext) NextAlias() string {
	jc.counter++
	return fmt.Sprintf("lt%d", jc.counter)
}

// Index returns the numeric index for a given label (1-based), as used by the old code.
func (jc *JoinContext) Index(labelName string) int {
	alias, ok := jc.labelIndex[labelName]
	if !ok {
		return -1
	}
	var idx int
	fmt.Sscanf(alias, "lt%d", &idx)
	return idx
}

// Joins returns all accumulated JOINs.
func (jc *JoinContext) Joins() []sqlexpr.Join {
	return jc.joins
}

// Counter returns the current counter (number of label aliases allocated).
func (jc *JoinContext) Counter() int {
	return jc.counter
}
