package sqlgen

import (
	"fmt"
	"strings"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

// mockFieldRegistry implements FieldRegistry for testing.
type mockFieldRegistry struct {
	fields map[string]fieldEntry
}

type fieldEntry struct {
	col       string
	isNumeric bool
	isIP      bool
}

func (m *mockFieldRegistry) Resolve(prefix string, fieldPath []string, inSort bool) (sqlexpr.Expr, error) {
	key := strings.Join(fieldPath, ".")
	entry, ok := m.fields[key]
	if !ok {
		return nil, fmt.Errorf("unknown field: %s", key)
	}
	col := sqlexpr.Col{Table: prefix, Name: entry.col}
	if inSort && entry.isIP {
		return sqlexpr.FuncCall{Name: "inet_aton", Args: []sqlexpr.Expr{col}}, nil
	}
	return col, nil
}

func (m *mockFieldRegistry) IsInteger(fieldID string) bool {
	if entry, ok := m.fields[fieldID]; ok {
		return entry.isNumeric
	}
	return false
}

func newTestRegistry() *mockFieldRegistry {
	return &mockFieldRegistry{
		fields: map[string]fieldEntry{
			"metadata.name":              {col: "metadata.name"},
			"metadata.namespace":         {col: "metadata.namespace"},
			"metadata.creationTimestamp": {col: "metadata.creationTimestamp"},
			"spec.containers.image":      {col: "spec.containers.image"},
			"status.phase":               {col: "status.phase"},
			"status.podIP":               {col: "status.podIP", isIP: true},
			"id":                         {col: "id"},
		},
	}
}

func TestCompileFieldFilter_Eq(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"metadata", "name"},
		Op:      sqltypes.Eq,
		Matches: []string{"test-pod"},
		Partial: false,
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."metadata.name" = ?`, sql)
	assertLen(t, 1, params)
	assertEqual(t, "test-pod", params[0].(string))
}

func TestCompileFieldFilter_EqPartial(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"metadata", "name"},
		Op:      sqltypes.Eq,
		Matches: []string{"test"},
		Partial: true,
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertContains(t, sql, "LIKE ?")
	assertContains(t, sql, `ESCAPE '\'`)
	assertLen(t, 1, params)
}

func TestCompileFieldFilter_In(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"metadata", "namespace"},
		Op:      sqltypes.In,
		Matches: []string{"ns1", "ns2"},
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."metadata.namespace" IN (?, ?)`, sql)
	assertLen(t, 2, params)
}

func TestCompileFieldFilter_NotIn(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"metadata", "namespace"},
		Op:      sqltypes.NotIn,
		Matches: []string{"kube-system"},
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."metadata.namespace" NOT IN (?)`, sql)
	assertLen(t, 1, params)
}

func TestCompileFieldFilter_Contains(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"spec", "containers", "image"},
		Op:      sqltypes.Contains,
		Matches: []string{"nginx"},
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `hasBarredValue(f."spec.containers.image", ?)`, sql)
	assertLen(t, 1, params)
}

func TestCompileFieldFilter_NotContains(t *testing.T) {
	reg := newTestRegistry()
	f := sqltypes.Filter{
		Field:   []string{"spec", "containers", "image"},
		Op:      sqltypes.NotContains,
		Matches: []string{"nginx"},
	}
	expr, err := CompileFieldFilter(f, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `NOT hasBarredValue(f."spec.containers.image", ?)`, sql)
	assertLen(t, 1, params)
}

func TestCompileLabelFilter_Eq(t *testing.T) {
	f := sqltypes.Filter{
		Field:   []string{"metadata", "labels", "app"},
		Op:      sqltypes.Eq,
		Matches: []string{"myapp"},
		Partial: false,
	}
	expr, err := CompileLabelFilter(f, "lt1", "f", false, "something")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, "lt1.label = ? AND lt1.value = ?", sql)
	assertLen(t, 2, params)
	assertEqual(t, "app", params[0].(string))
	assertEqual(t, "myapp", params[1].(string))
}

func TestCompileLabelFilter_Exists(t *testing.T) {
	f := sqltypes.Filter{
		Field: []string{"metadata", "labels", "app"},
		Op:    sqltypes.Exists,
	}
	expr, err := CompileLabelFilter(f, "lt1", "f", false, "something")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, "lt1.label = ?", sql)
	assertLen(t, 1, params)
}

func TestCompileLabelFilter_In(t *testing.T) {
	f := sqltypes.Filter{
		Field:   []string{"metadata", "labels", "env"},
		Op:      sqltypes.In,
		Matches: []string{"dev", "staging"},
	}
	expr, err := CompileLabelFilter(f, "lt2", "f", false, "something")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, "lt2.label = ? AND lt2.value IN (?, ?)", sql)
	assertLen(t, 3, params)
}

func TestCompilePartitions_Passthrough(t *testing.T) {
	partitions := []partition.Partition{{All: true}}
	expr := CompilePartitions("", partitions, "f")
	if expr != nil {
		t.Fatal("expected nil for passthrough partition")
	}
}

func TestCompilePartitions_Empty(t *testing.T) {
	partitions := []partition.Partition{}
	expr := CompilePartitions("", partitions, "f")
	sql, _ := expr.Resolve()
	assertEqual(t, "FALSE", sql)
}

// Helpers
func assertEqual(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("\nexpected:\n%s\n\ngot:\n%s", expected, actual)
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
