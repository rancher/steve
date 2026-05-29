package sqlgen

import (
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"k8s.io/apimachinery/pkg/util/sets"
)

// mockField implements IndexedField for testing.
type mockField struct {
	name     string
	colType  string
	isTimestamp bool
}

func (f *mockField) ColumnName() string { return f.name }
func (f *mockField) ColumnType() string { return f.colType }
func (f *mockField) IsTimestampField() bool { return f.isTimestamp }

func newMockFields(names ...string) map[string]IndexedField {
	fields := make(map[string]IndexedField)
	for _, name := range names {
		fields[name] = &mockField{name: name, colType: "TEXT"}
	}
	return fields
}

func assertEqual(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("\nexpected:\n%s\n\ngot:\n%s", expected, actual)
	}
}

// --- FieldRegistry Tests ---

func TestFieldRegistry_Resolve_Direct(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name", "status.phase"))
	expr, err := reg.Resolve("f", []string{"metadata", "name"}, false)
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := expr.Resolve()
	assertEqual(t, `f."metadata.name"`, sql)
}

func TestFieldRegistry_Resolve_Invalid(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name"))
	_, err := reg.Resolve("f", []string{"nonexistent", "field"}, false)
	if err == nil {
		t.Fatal("expected error for invalid field")
	}
}

// --- CompileFieldFilter Tests ---

func TestCompileFieldFilter_Eq(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase"))
	filter := sqltypes.Filter{
		Field:   []string{"status", "phase"},
		Op:      sqltypes.Eq,
		Matches: []string{"Running"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."status.phase" = ?`, sql)
	if params[0].(string) != "Running" {
		t.Errorf("expected param 'Running', got %v", params[0])
	}
}

func TestCompileFieldFilter_EqPartial(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase"))
	filter := sqltypes.Filter{
		Field:   []string{"status", "phase"},
		Op:      sqltypes.Eq,
		Matches: []string{"Run"},
		Partial: true,
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."status.phase" LIKE ? ESCAPE '\'`, sql)
	if params[0].(string) != "%Run%" {
		t.Errorf("expected param '%%Run%%', got %v", params[0])
	}
}

func TestCompileFieldFilter_NotEq(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase"))
	filter := sqltypes.Filter{
		Field:   []string{"status", "phase"},
		Op:      sqltypes.NotEq,
		Matches: []string{"Failed"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."status.phase" != ?`, sql)
	if params[0].(string) != "Failed" {
		t.Errorf("expected param 'Failed', got %v", params[0])
	}
}

func TestCompileFieldFilter_Lt(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("spec.replicas"))
	filter := sqltypes.Filter{
		Field:   []string{"spec", "replicas"},
		Op:      sqltypes.Lt,
		Matches: []string{"5"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."spec.replicas" < ?`, sql)
	if params[0].(float64) != 5.0 {
		t.Errorf("expected param 5.0, got %v", params[0])
	}
}

func TestCompileFieldFilter_In(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.namespace"))
	filter := sqltypes.Filter{
		Field:   []string{"metadata", "namespace"},
		Op:      sqltypes.In,
		Matches: []string{"ns1", "ns2", "ns3"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."metadata.namespace" IN (?, ?, ?)`, sql)
	if len(params) != 3 {
		t.Fatalf("expected 3 params, got %d", len(params))
	}
}

func TestCompileFieldFilter_NotIn(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.namespace"))
	filter := sqltypes.Filter{
		Field:   []string{"metadata", "namespace"},
		Op:      sqltypes.NotIn,
		Matches: []string{"kube-system"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := expr.Resolve()
	assertEqual(t, `f."metadata.namespace" NOT IN (?)`, sql)
}

func TestCompileFieldFilter_Contains(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("spec.containers.image"))
	filter := sqltypes.Filter{
		Field:   []string{"spec", "containers", "image"},
		Op:      sqltypes.Contains,
		Matches: []string{"nginx"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `hasBarredValue(f."spec.containers.image", ?)`, sql)
	if params[0].(string) != "nginx" {
		t.Errorf("expected param 'nginx', got %v", params[0])
	}
}

func TestCompileFieldFilter_NotContains(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("spec.containers.image"))
	filter := sqltypes.Filter{
		Field:   []string{"spec", "containers", "image"},
		Op:      sqltypes.NotContains,
		Matches: []string{"nginx"},
	}
	expr, err := CompileFieldFilter(filter, reg, "f")
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := expr.Resolve()
	assertEqual(t, `NOT (hasBarredValue(f."spec.containers.image", ?))`, sql)
}

// --- CompileLabelFilter Tests ---

func TestCompileLabelFilter_Eq(t *testing.T) {
	filter := sqltypes.Filter{
		Field:   []string{"metadata", "labels", "app"},
		Op:      sqltypes.Eq,
		Matches: []string{"nginx"},
	}
	expr, err := CompileLabelFilter(filter, "lt1", "f", false, "test_db")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `(lt1."label" = ?) AND (lt1."value" = ?)`, sql)
	if params[0].(string) != "app" || params[1].(string) != "nginx" {
		t.Errorf("unexpected params: %v", params)
	}
}

func TestCompileLabelFilter_Exists(t *testing.T) {
	filter := sqltypes.Filter{
		Field: []string{"metadata", "labels", "app"},
		Op:    sqltypes.Exists,
	}
	expr, err := CompileLabelFilter(filter, "lt1", "f", false, "test_db")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `lt1."label" = ?`, sql)
	if params[0].(string) != "app" {
		t.Errorf("expected param 'app', got %v", params[0])
	}
}

func TestCompileLabelFilter_In(t *testing.T) {
	filter := sqltypes.Filter{
		Field:   []string{"metadata", "labels", "tier"},
		Op:      sqltypes.In,
		Matches: []string{"frontend", "backend"},
	}
	expr, err := CompileLabelFilter(filter, "lt2", "f", false, "test_db")
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `(lt2."label" = ?) AND (lt2."value" IN (?, ?))`, sql)
	if len(params) != 3 {
		t.Fatalf("expected 3 params, got %d", len(params))
	}
}

// --- CompilePartitions Tests ---

func TestCompilePartitions_Empty(t *testing.T) {
	expr := CompilePartitions("", nil, "f")
	sql, _ := expr.Resolve()
	assertEqual(t, "FALSE", sql)
}

func TestCompilePartitions_Passthrough(t *testing.T) {
	partitions := []partition.Partition{{Passthrough: true}}
	expr := CompilePartitions("", partitions, "f")
	if expr != nil {
		t.Error("expected nil for passthrough partition")
	}
}

func TestCompilePartitions_AllInAllNamespaces(t *testing.T) {
	partitions := []partition.Partition{{All: true}}
	expr := CompilePartitions("", partitions, "f")
	if expr != nil {
		t.Error("expected nil for All=true with no namespace filter")
	}
}

func TestCompilePartitions_RestrictedNames(t *testing.T) {
	partitions := []partition.Partition{
		{Namespace: "ns1", Names: sets.New("pod1", "pod2")},
	}
	expr := CompilePartitions("", partitions, "f")
	if expr == nil {
		t.Fatal("expected non-nil expr")
	}
	sql, params := expr.Resolve()
	// Should have both namespace IN and name IN
	if len(params) != 3 { // ns1, pod1, pod2
		t.Errorf("expected 3 params, got %d: %v", len(params), params)
	}
	_ = sql // just verify it resolves without error
}

func TestCompilePartitions_MultipleNamespaces(t *testing.T) {
	partitions := []partition.Partition{
		{Namespace: "ns1", All: true},
		{Namespace: "ns2", All: true},
	}
	expr := CompilePartitions("", partitions, "f")
	if expr == nil {
		t.Fatal("expected non-nil expr")
	}
	sql, params := expr.Resolve()
	// Should have namespace IN (ns1, ns2)
	if len(params) != 2 {
		t.Errorf("expected 2 params, got %d: %v\nsql: %s", len(params), params, sql)
	}
}

// --- CompileSort Tests ---

func TestCompileSort_Default_Namespaced(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name"))
	orderBy, ctes, err := CompileSort(sqltypes.SortList{}, reg, "f", true, NewJoinContext("db", "f"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ctes) != 0 {
		t.Error("expected no CTEs")
	}
	if len(orderBy) != 1 {
		t.Fatal("expected 1 order by")
	}
	sql, _ := orderBy[0].Resolve()
	assertEqual(t, "f.id ASC", sql)
}

func TestCompileSort_Default_ClusterScoped(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name"))
	orderBy, _, err := CompileSort(sqltypes.SortList{}, reg, "f", false, NewJoinContext("db", "f"))
	if err != nil {
		t.Fatal(err)
	}
	if len(orderBy) != 1 {
		t.Fatal("expected 1 order by")
	}
	sql, _ := orderBy[0].Resolve()
	assertEqual(t, `f."metadata.name" ASC`, sql)
}

func TestCompileSort_FieldSort(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase"))
	sortList := sqltypes.SortList{
		SortDirectives: []sqltypes.Sort{
			{Fields: []string{"status", "phase"}, Order: sqltypes.DESC},
		},
	}
	orderBy, _, err := CompileSort(sortList, reg, "f", true, NewJoinContext("db", "f"))
	if err != nil {
		t.Fatal(err)
	}
	if len(orderBy) != 1 {
		t.Fatal("expected 1 order by")
	}
	sql, _ := orderBy[0].Resolve()
	assertEqual(t, `f."status.phase" DESC`, sql)
}

// --- CompileOrFilter Tests ---

func TestCompileOrFilter_SingleField(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase"))
	orFilter := sqltypes.OrFilter{
		Filters: []sqltypes.Filter{
			{Field: []string{"status", "phase"}, Op: sqltypes.Eq, Matches: []string{"Running"}},
		},
	}
	jc := NewJoinContext("db", "f")
	expr, err := CompileOrFilter(orFilter, reg, "f", false, "db", jc)
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	assertEqual(t, `f."status.phase" = ?`, sql)
	if params[0].(string) != "Running" {
		t.Errorf("expected 'Running', got %v", params[0])
	}
}

func TestCompileOrFilter_MultipleFields(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("status.phase", "metadata.name"))
	orFilter := sqltypes.OrFilter{
		Filters: []sqltypes.Filter{
			{Field: []string{"status", "phase"}, Op: sqltypes.Eq, Matches: []string{"Running"}},
			{Field: []string{"metadata", "name"}, Op: sqltypes.Eq, Matches: []string{"test"}},
		},
	}
	jc := NewJoinContext("db", "f")
	expr, err := CompileOrFilter(orFilter, reg, "f", false, "db", jc)
	if err != nil {
		t.Fatal(err)
	}
	sql, params := expr.Resolve()
	// Should be OR'd
	assertEqual(t, `(f."status.phase" = ?) OR (f."metadata.name" = ?)`, sql)
	if len(params) != 2 {
		t.Fatalf("expected 2 params, got %d", len(params))
	}
}

// --- CompileListQuery Integration Test ---

func TestCompileListQuery_Basic(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name", "metadata.namespace", "status.phase"))
	lo := &sqltypes.ListOptions{
		Filters: []sqltypes.OrFilter{
			{Filters: []sqltypes.Filter{
				{Field: []string{"status", "phase"}, Op: sqltypes.Eq, Matches: []string{"Running"}},
			}},
		},
		Pagination: sqltypes.Pagination{PageSize: 25, Page: 2},
	}
	partitions := []partition.Partition{{Passthrough: true}}

	cq, err := CompileListQuery(lo, partitions, "cattle-system", "test_db", reg, true)
	if err != nil {
		t.Fatal(err)
	}

	query, params, countQuery, countParams := cq.Resolve()

	// Should have: status.phase = 'Running' AND metadata.namespace = 'cattle-system'
	if len(params) == 0 {
		t.Fatal("expected params")
	}
	if query == "" {
		t.Fatal("expected non-empty query")
	}
	if !cq.HasPagination {
		t.Fatal("expected pagination")
	}
	if countQuery == "" {
		t.Fatal("expected count query")
	}
	if len(countParams) == 0 {
		t.Fatal("expected count params")
	}
	_ = query
}

func TestCompileListQuery_WithLabelFilter(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name", "metadata.namespace"))
	lo := &sqltypes.ListOptions{
		Filters: []sqltypes.OrFilter{
			{Filters: []sqltypes.Filter{
				{Field: []string{"metadata", "labels", "app"}, Op: sqltypes.Eq, Matches: []string{"nginx"}},
			}},
		},
	}
	partitions := []partition.Partition{{Passthrough: true}}

	cq, err := CompileListQuery(lo, partitions, "", "test_db", reg, true)
	if err != nil {
		t.Fatal(err)
	}

	query, params, _, _ := cq.Resolve()

	// Should have DISTINCT (because of label join)
	if !cq.Query.Distinct {
		t.Error("expected DISTINCT")
	}
	// Should have a label join
	if len(cq.Query.Joins) < 2 { // base join + label join
		t.Errorf("expected at least 2 joins, got %d", len(cq.Query.Joins))
	}
	_ = query
	_ = params
}

func TestCompileListQuery_NoFilters(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name"))
	lo := &sqltypes.ListOptions{}
	partitions := []partition.Partition{{Passthrough: true}}

	cq, err := CompileListQuery(lo, partitions, "", "test_db", reg, true)
	if err != nil {
		t.Fatal(err)
	}

	query, params, _, _ := cq.Resolve()

	// No WHERE
	if cq.Query.Where != nil {
		t.Error("expected nil WHERE for no filters + passthrough")
	}
	_ = query
	_ = params
}

// --- JoinContext Tests ---

func TestJoinContext_EnsureLabelJoin(t *testing.T) {
	jc := NewJoinContext("test_db", "f")
	alias1 := jc.EnsureLabelJoin("app")
	assertEqual(t, "lt1", alias1)

	alias2 := jc.EnsureLabelJoin("tier")
	assertEqual(t, "lt2", alias2)

	// Same label returns same alias
	alias1Again := jc.EnsureLabelJoin("app")
	assertEqual(t, "lt1", alias1Again)

	if !jc.UsesLabels {
		t.Error("expected UsesLabels to be true")
	}
	if len(jc.Joins()) != 2 {
		t.Errorf("expected 2 joins, got %d", len(jc.Joins()))
	}
}

// --- Resolve output inspection ---

func TestCompileListQuery_VerifySQL(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name", "metadata.namespace", "status.phase"))
	lo := &sqltypes.ListOptions{
		Filters: []sqltypes.OrFilter{
			{Filters: []sqltypes.Filter{
				{Field: []string{"status", "phase"}, Op: sqltypes.Eq, Matches: []string{"Running"}},
			}},
			{Filters: []sqltypes.Filter{
				{Field: []string{"metadata", "namespace"}, Op: sqltypes.In, Matches: []string{"ns1", "ns2"}},
			}},
		},
	}
	partitions := []partition.Partition{{Passthrough: true}}

	cq, err := CompileListQuery(lo, partitions, "", "test_db", reg, true)
	if err != nil {
		t.Fatal(err)
	}

	query, params, _, _ := cq.Resolve()

	// Verify the SQL contains expected clauses
	for _, expected := range []string{"SELECT", "FROM", "JOIN", "WHERE", "ORDER BY"} {
		if !contains(query, expected) {
			t.Errorf("expected query to contain %q:\n%s", expected, query)
		}
	}

	// Verify params: "Running", "ns1", "ns2"
	if len(params) != 3 {
		t.Fatalf("expected 3 params, got %d: %v", len(params), params)
	}
	if params[0].(string) != "Running" {
		t.Errorf("expected params[0]='Running', got %v", params[0])
	}
	if params[1].(string) != "ns1" {
		t.Errorf("expected params[1]='ns1', got %v", params[1])
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- formatMatchTarget Tests ---

func TestFormatMatchTarget_Escaping(t *testing.T) {
	filter := sqltypes.Filter{
		Matches: []string{`test_value%with\special`},
		Partial: true,
	}
	result := formatMatchTarget(filter)
	expected := `%test\_value\%with\\special%`
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

// --- End-to-end verification with known SQL patterns ---

func TestCompileListQuery_PartitionRestricted(t *testing.T) {
	reg := NewFieldRegistry(newMockFields("metadata.name", "metadata.namespace"))
	lo := &sqltypes.ListOptions{}
	partitions := []partition.Partition{
		{Namespace: "ns1", Names: sets.New("pod1")},
	}

	cq, err := CompileListQuery(lo, partitions, "", "test_db", reg, true)
	if err != nil {
		t.Fatal(err)
	}

	_, params, _, _ := cq.Resolve()
	// Should have partition params: ns1, pod1
	if len(params) < 2 {
		t.Errorf("expected at least 2 params (partition), got %d: %v", len(params), params)
	}
}

// Verify that Select columns resolve correctly
func TestSelectColumns(t *testing.T) {
	s := sqlexpr.Select{
		Columns: []sqlexpr.Expr{
			sqlexpr.Raw{SQL: "o.object"},
			sqlexpr.Raw{SQL: "o.objectnonce"},
			sqlexpr.Raw{SQL: "o.dekid"},
		},
		From: sqlexpr.TableRef{Name: "test_db", Alias: "o"},
		Joins: []sqlexpr.Join{
			{
				Kind:  sqlexpr.InnerJoin,
				Table: sqlexpr.TableRef{Name: "test_db_fields", Alias: "f"},
				On:    sqlexpr.Compare{Left: sqlexpr.Col{Table: "o", Name: "key"}, Op: "=", Right: sqlexpr.Col{Table: "f", Name: "key"}},
			},
		},
	}
	sql, _ := s.Resolve()
	if !containsSubstring(sql, "o.object") {
		t.Errorf("expected 'o.object' in:\n%s", sql)
	}
}
