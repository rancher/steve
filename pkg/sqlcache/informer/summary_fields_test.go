/*
Copyright 2026 SUSE LLC
*/

package informer

import (
	"fmt"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConstructSummaryQueryForField(t *testing.T) {
	type testCase struct {
		description      string
		summaryField     []string
		fieldNum         int
		mainFieldPrefix  string
		filterComponents *filterComponentsT
		joinTableIndex   map[string]int
		expectedStmt     string
		expectedStmtArgs []any
		expectedErr      string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: refused to build a summary on an unrecognized field",
		summaryField: []string{"metadata", "snorkel"},
		expectedErr:  fmt.Sprintf("column is invalid [%s]: supplied column is invalid", "metadata.snorkel"),
	})
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: refused to build a summary on a sql injection",
		summaryField: []string{"spec", "blip; system('rm -fr /home/hardware/wars);"},
		// The parser mangles the field name in the error message, but not a big issue
		expectedErr: fmt.Sprintf("column is invalid [%s]: supplied column is invalid", "spec[blip; system('rm -fr /home/hardware/wars);]"),
	})
	// summary=metadata.labels.status
	tests = append(tests, testCase{
		description:      "TestConstructSummaryQueryForField: builds a query for a label summary with no filter-components: summary=metadata.labels.status",
		summaryField:     []string{"metadata", "labels", "status"},
		fieldNum:         1,
		filterComponents: &filterComponentsT{isEmpty: true},
		expectedStmt: `SELECT 'metadata.labels.status' AS p, COUNT(*) AS c, value AS k
	FROM "something_labels"
	WHERE label = ? AND k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"status"},
		expectedErr:      "",
	})
	// summary=metadata.queryField1
	tests = append(tests, testCase{
		description:      "TestConstructSummaryQueryForField: builds a query for a field summary with no filter-components",
		summaryField:     []string{"metadata", "queryField1"},
		fieldNum:         1,
		filterComponents: &filterComponentsT{isEmpty: true},
		expectedStmt: `SELECT 'metadata.queryField1' AS p, COUNT(*) AS c, "metadata.queryField1" AS k
	FROM "something_fields"
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{},
		expectedErr:      "",
	})
	// summary=spec.containers.image[3]
	tests = append(tests, testCase{
		description:      "TestConstructSummaryQueryForField: builds a query for a field summary on an indexed implicit array: summary=spec.containers.image[3]",
		summaryField:     []string{"spec", "containers", "image", "3"},
		fieldNum:         1,
		mainFieldPrefix:  "f1",
		filterComponents: &filterComponentsT{isEmpty: true},
		expectedStmt: `SELECT 'spec.containers.image[3]' AS p, COUNT(*) AS c, extractBarredValue("spec.containers.image", 3) AS k
	FROM "something_fields"
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{},
		expectedErr:      "",
	})
	// summary=metadata.queryField1&filter=metadata.namespace=cars
	// As soon as we have a non-empty body build a with-statement
	tests = append(tests, testCase{
		description:     "TestConstructSummaryQuery: builds a query for a summary on a standard field with a standard filter: summary=metadata.queryField1&filter=metadata.namespace=cars",
		summaryField:    []string{"metadata", "queryField1"},
		fieldNum:        1,
		mainFieldPrefix: "f1",
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1."metadata.namespace" = ?)`},
			params:       []any{"cars"},
			isEmpty:      false,
		},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT f1.key, f1."metadata.queryField1" FROM "something_fields" f1
	WHERE (f1."metadata.namespace" = ?)
)
SELECT 'metadata.queryField1' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"cars"},
		expectedErr:      "",
	})
	// summary=metadata.queryField1&filter=metadata.labels.status=cars
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: builds a query for a summary on a standard field with a labels filter: summary=metadata.queryField1&filter=metadata.labels.status=cars",
		summaryField: []string{"metadata", "queryField1"},
		fieldNum:     1,
		filterComponents: &filterComponentsT{
			whereClauses: []string{"(lt1.label = ? AND lt1.value = ?)"},
			joinParts:    standardJoinParts,
			params:       []any{"status", "cars"},
		},
		joinTableIndex: map[string]int{"status": 1},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, f1."metadata.queryField1" FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
	WHERE (lt1.label = ? AND lt1.value = ?)
)
SELECT 'metadata.queryField1' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"status", "cars"},
		expectedErr:      "",
	})
	// summary=spec.containers.image[3]&filter=metadata.namespace=cars&spec.containers.image[2]>0
	tests = append(tests, testCase{
		description:     "TestConstructSummaryQueryForField: builds a query for a field summary on an indexed implicit array with complex filters: summary=spec.containers.image[3]&filter=metadata.namespace=cars&spec.containers.image[2]>0",
		summaryField:    []string{"spec", "containers", "image", "4"},
		fieldNum:        1,
		mainFieldPrefix: "f1",
		joinTableIndex:  map[string]int{},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1."metadata.queryField1" = ?) OR (extractBarredValue(f1."spec.containers.image", "5") = ?)`},
			params:       []any{"boxes", "sticks"},
		},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT f1.key, extractBarredValue(f1."spec.containers.image", 4) FROM "something_fields" f1
	WHERE (f1."metadata.queryField1" = ?) OR (extractBarredValue(f1."spec.containers.image", "5") = ?)
)
SELECT 'spec.containers.image[4]' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"boxes", "sticks"},
		expectedErr:      "",
	})
	// summary=metadata.labels.status&filter=metadata.namespace=trains
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: builds a query for a summary on a label field with a std filter: summary=metadata.labels.status&filter=metadata.namespace=trains",
		summaryField: []string{"metadata", "labels", "status"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1."metadata.namespace" = ?)`},
			params:       []any{"trains"},
		},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt1.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
	WHERE ((f1."metadata.namespace" = ?))
		AND (lt1.label = ?)
)
SELECT 'metadata.labels.status' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"trains", "status"},
		expectedErr:      "",
	})
	// summary=metadata.labels.status&filter=spec.containers.image[3]=planes
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: builds a query for a label summary with a std filter on an implicit array: summary=metadata.labels.status&filter=spec.containers.image[7]=planes",
		summaryField: []string{"metadata", "labels", "status"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(extractBarredValue(f1."spec.containers.image", "7") = ?)`},
			params:       []any{"planes"},
		},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt1.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
	WHERE ((extractBarredValue(f1."spec.containers.image", "7") = ?))
		AND (lt1.label = ?)
)
SELECT 'metadata.labels.status' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"planes", "status"},
		expectedErr:      "",
	})
	// summary=metadata.labels.status&filter=metadata.labels.transportation=boats
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: builds a query for a label summary with a label filter: summary=metadata.labels.status&filter=metadata.labels.transportation=boats",
		summaryField: []string{"metadata", "labels", "status"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{"(lt1.label = ? AND lt1.value = ?)"},
			params:       []any{"transportation", "boats"},
			joinParts:    standardJoinParts,
		},
		joinTableIndex: map[string]int{"transportation": 1},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt2.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON f1.key = lt2.key
	WHERE ((lt1.label = ? AND lt1.value = ?))
		AND (lt2.label = ?)
)
SELECT 'metadata.labels.status' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"transportation", "boats", "status"},
		expectedErr:      "",
	})
	// summary=metadata.labels.status&filter=metadata.labels.transportation!=jets
	tests = append(tests, testCase{
		description:  "TestConstructSummaryQuery: builds a query for a label summary with a negative label filter: summary=metadata.labels.status&filter=metadata.labels.transportation!=jets",
		summaryField: []string{"metadata", "labels", "status"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
				LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
				WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)`},
			params:    []any{"transportation", "transportation", "jets"},
			joinParts: standardJoinParts,
		},
		joinTableIndex: map[string]int{"transportation": 1},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt2.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON f1.key = lt2.key
	WHERE ((f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
				LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
				WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?))
		AND (lt2.label = ?)
)
SELECT 'metadata.labels.status' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"transportation", "transportation", "jets", "status"},
		expectedErr:      "",
	})
	// summary=metadata.labels.atlantic&filter=metadata.labels[kubernetes.io/metadata.name]~kube
	tests = append(tests, testCase{
		description:  "TestConstructSummaryTestFilters: handles complex label summary/label filter: summary=metadata.labels.atlantic&filter=metadata.labels[kubernetes.io/metadata.name]~kube",
		summaryField: []string{"metadata", "labels", "atlantic"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
				LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
				WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)`},
			params:    []any{"kubernetes.io/metadata.name", "kubernetes.io/metadata.name", "kube"},
			joinParts: standardJoinParts,
		},
		joinTableIndex: map[string]int{"kubernetes.io/metadata.name": 1},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt2.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON f1.key = lt2.key
	WHERE ((f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
				LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
				WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?))
		AND (lt2.label = ?)
)
SELECT 'metadata.labels.atlantic' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,
		expectedStmtArgs: []any{"kubernetes.io/metadata.name", "kubernetes.io/metadata.name", "kube", "atlantic"},
		expectedErr:      "",
	})

	// summary=metadata.labels.pacific&filter=metadata.labels.knot!=hitch&filter=metadata.queryField1~g
	tests = append(tests, testCase{
		description:  "TestConstructSummaryTestFilters: handles complex label summary/label filter: summary=metadata.labels.pacific&filter=metadata.labels.knot!=hitch&filter=metadata.queryField1~g",
		summaryField: []string{"metadata", "labels", "pacific"},
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
		LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)`,
				`f1."metadata.queryField1" LIKE ? ESCAPE '\'`}, //'
			params:    []any{"knot", "knot", "hitch", "%g%"},
			joinParts: standardJoinParts,
		},
		joinTableIndex: map[string]int{"knot": 1},
		expectedStmt: `WITH w1(key, finalField) AS (
	SELECT DISTINCT f1.key, lt2.value FROM "something_fields" f1
  LEFT OUTER JOIN "something_labels" lt1 ON f1.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON f1.key = lt2.key
	WHERE ((f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
		LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?))
		AND (f1."metadata.queryField1" LIKE ? ESCAPE '\')
		AND (lt2.label = ?)
)
SELECT 'metadata.labels.pacific' AS p, COUNT(*) AS c, w1.finalField AS k FROM w1
	WHERE k != ""
	GROUP BY k`,//'
		expectedStmtArgs: []any{"knot", "knot", "hitch", "%g%", "pacific"},
		expectedErr:      "",
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			store.EXPECT().GetName().Return("something").AnyTimes()
			dbName := "something"
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: toIndexedFieldsFromColumnNames("metadata.name", "metadata.namespace", "metadata.queryField1", "metadata.state.name", "spec.containers.image"),
			}
			fieldNum := test.fieldNum
			if fieldNum == 0 {
				fieldNum = 1
			}
			mainFieldPrefix := test.mainFieldPrefix
			if mainFieldPrefix == "" {
				mainFieldPrefix = fmt.Sprintf("f%d", fieldNum)
			}
			joinTableIndex := test.joinTableIndex
			if joinTableIndex == nil {
				joinTableIndex = map[string]int{}
			}
			queryInfo, err := lii.constructSummaryQueryForField(test.summaryField, fieldNum, dbName, test.filterComponents, mainFieldPrefix, joinTableIndex, false)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			if queryInfo.params == nil {
				queryInfo.params = []any{}
			}
			if test.expectedStmtArgs == nil {
				test.expectedStmtArgs = []any{}
			}
			assert.Equal(t, len(test.expectedStmtArgs), len(queryInfo.params))
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, "", queryInfo.countQuery)
			assert.Equal(t, 0, len(queryInfo.countParams))
		})
	}
}
func TestConstructNamespacedSummaryQuery(t *testing.T) {
	type testCase struct {
		description      string
		summaryField     []string
		fieldNum         int
		mainFieldPrefix  string
		filterComponents *filterComponentsT
		joinTableIndex   map[string]int
		expectedStmt     string
		expectedStmtArgs []any
		expectedErr      string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description:     "TestConstructSummaryQuery: builds a query for a summary on a standard field with a standard filter: summary=metadata.queryField1&filter=metadata.namespace~cars",
		summaryField:    []string{"metadata", "queryField1"},
		fieldNum:        1,
		mainFieldPrefix: "f1",
		filterComponents: &filterComponentsT{
			whereClauses: []string{`(f1."metadata.name" LIKE ? ESCAPE '\')`},
			params:       []any{"%cars%"},
			isEmpty:      false,
		},
		expectedStmt: `WITH w1(key, finalField, namespace) AS (
	SELECT f1.key, f1."metadata.queryField1", f1."metadata.namespace" FROM "something_fields" f1
	WHERE (f1."metadata.name" LIKE ? ESCAPE '\')
)
SELECT 'metadata.queryField1' AS p, COUNT(*) AS c, w1.finalField AS k, w1.namespace AS ns FROM w1
	WHERE k != ""
	GROUP BY k, ns`,
		expectedStmtArgs: []any{"%cars%"},
		expectedErr:      "",
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			store.EXPECT().GetName().Return("something").AnyTimes()
			dbName := "something"
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: toIndexedFieldsFromColumnNames("metadata.name", "metadata.namespace", "metadata.queryField1", "metadata.state.name", "spec.containers.image"),
			}
			fieldNum := test.fieldNum
			if fieldNum == 0 {
				fieldNum = 1
			}
			mainFieldPrefix := test.mainFieldPrefix
			if mainFieldPrefix == "" {
				mainFieldPrefix = fmt.Sprintf("f%d", fieldNum)
			}
			joinTableIndex := test.joinTableIndex
			if joinTableIndex == nil {
				joinTableIndex = map[string]int{}
			}
			queryInfo, err := lii.constructSummaryQueryForField(test.summaryField, fieldNum, dbName, test.filterComponents, mainFieldPrefix, joinTableIndex, true)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			if queryInfo.params == nil {
				queryInfo.params = []any{}
			}
			if test.expectedStmtArgs == nil {
				test.expectedStmtArgs = []any{}
			}
			assert.Equal(t, len(test.expectedStmtArgs), len(queryInfo.params))
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, "", queryInfo.countQuery)
			assert.Equal(t, 0, len(queryInfo.countParams))
		})
	}
}

func TestConstructSummaryTestFilters(t *testing.T) {
	type testCase struct {
		description       string
		listOptions       sqltypes.ListOptions
		partitions        []partition.Partition
		ns                string
		expectedFilters   *filterComponentsT
		expectedErr       string
		expectedJoinTable map[string]int
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: returns nothing with no filters",
		listOptions: sqltypes.ListOptions{},
		partitions:  []partition.Partition{{All: true}},
		ns:          "",
		expectedErr: "",
		expectedFilters: &filterComponentsT{joinParts: make([]joinPart, 0),
			isEmpty: true},
	})
	// filter=metadata.queryField1 - error
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: can propagate an error from filter processing: filter=metadata.queryField1",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    sqltypes.Exists,
					},
				},
			},
		}},
		partitions:  []partition.Partition{{All: true}},
		ns:          "",
		expectedErr: "NULL and NOT NULL tests aren't supported for non-label queries",
	})
	// filter=metadata.queryField1=toys,metadata.labels.animals=starfish
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: returns JOINs and WHEREs: filter=metadata.queryField1=toys,metadata.labels.animals=starfish",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      sqltypes.Eq,
						Matches: []string{"toys"},
					},
					{
						Field:   []string{"metadata", "labels", "animals"},
						Op:      sqltypes.Eq,
						Matches: []string{"starfish"},
					},
				},
			},
		}},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			joinParts: []joinPart{
				{
					joinCommand:    "LEFT OUTER JOIN",
					tableName:      "something_labels",
					tableNameAlias: "lt1",
					onPrefix:       "f1",
					onField:        "key",
					otherPrefix:    "lt1",
					otherField:     "key",
				},
			},
			whereClauses:    []string{`(f1."metadata.queryField1" = ?) OR (lt1.label = ? AND lt1.value = ?)`},
			params:          []any{"toys", "animals", "starfish"},
			isEmpty:         false,
			queryUsesLabels: true,
		},
		expectedJoinTable: map[string]int{"animals": 1},
	})
	// filter=metadata.queryField1=books,metadata.labels.pigs!=boars
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: mixed JOINs and WHEREs, prefixes are propagated into negative tests: filter=metadata.queryField1=books,metadata.labels.pigs!=boars",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      sqltypes.NotEq,
						Matches: []string{"books"},
					},
					{
						Field:   []string{"metadata", "labels", "pigs"},
						Op:      sqltypes.NotEq,
						Matches: []string{"boars"},
					},
				},
			},
		}},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			joinParts: standardJoinParts,
			whereClauses: []string{`(f1."metadata.queryField1" != ?) OR ((f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
		LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?))`},
			params:          []any{"books", "pigs", "pigs", "boars"},
			isEmpty:         false,
			queryUsesLabels: true,
		},
		expectedJoinTable: map[string]int{"pigs": 1},
	})
	// filter=metadata.queryField1=boxes,spec.containers.image[3]=sticks
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: handles implicit array indexing, no labels: filter=metadata.queryField1=boxes,spec.containers.image[3]=sticks",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      sqltypes.Eq,
						Matches: []string{"boxes"},
					},
					{
						Field:   []string{"spec", "containers", "image", "3"},
						Op:      sqltypes.Eq,
						Matches: []string{"sticks"},
					},
				},
			},
		}},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			whereClauses: []string{`(f1."metadata.queryField1" = ?) OR (extractBarredValue(f1."spec.containers.image", "3") = ?)`},
			params:       []any{"boxes", "sticks"},
			isEmpty:      false,
		},
	})
	// filter=metadata.labels[kubernetes.io/metadata.name]~kube
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: handles complex label filter: filter=metadata.labels[kubernetes.io/metadata.name]~kube",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "kubernetes.io/metadata.name"},
						Op:      sqltypes.Eq,
						Partial: true,
						Matches: []string{"kube"},
					},
				},
			},
		}},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			whereClauses:    []string{`lt1.label = ? AND lt1.value LIKE ? ESCAPE '\'`}, //'
			joinParts:       standardJoinParts,
			params:          []any{"kubernetes.io/metadata.name", "%kube%"},
			isEmpty:         false,
			queryUsesLabels: true,
		},
		expectedJoinTable: map[string]int{"kubernetes.io/metadata.name": 1},
	})
	// filter=metadata.labels.knot!=hitch&filter=metadata.queryField1~g
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: another mixed filter: returns JOINs and WHEREs: filter=metadata.labels.knot!=hitch&filter=metadata.queryField1~g",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "knot"},
						Op:      sqltypes.NotEq,
						Matches: []string{"hitch"},
					},
				},
			},
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      sqltypes.Eq,
						Matches: []string{"g"},
						Partial: true,
					},
				},
			},
		}},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			joinParts: standardJoinParts,
			whereClauses: []string{`(f1.key NOT IN (SELECT f11.key FROM "something_fields" f11
		LEFT OUTER JOIN "something_labels" lt1i1 ON f11.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)`,
				`f1."metadata.queryField1" LIKE ? ESCAPE '\'`}, //'
			params:          []any{"knot", "knot", "hitch", "%g%"},
			isEmpty:         false,
			queryUsesLabels: true,
		},
		expectedJoinTable: map[string]int{"knot": 1},
	})
	// filter=metadata.labels.knot!=granny&pagesize=8
	tests = append(tests, testCase{
		description: "TestConstructSummaryTestFilters: pagesize requires sorting: filter=metadata.labels.knot=granny&pagesize=8",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "knot"},
							Op:      sqltypes.Eq,
							Matches: []string{"granny"},
						},
					},
				},
			},
			Pagination: sqltypes.Pagination{PageSize: 8},
		},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedFilters: &filterComponentsT{
			joinParts:       standardJoinParts,
			whereClauses:    []string{"lt1.label = ? AND lt1.value = ?"},
			orderByClauses:  []string{`f1."metadata.name" ASC`},
			params:          []any{"knot", "granny"},
			limitClause:     "\n  LIMIT 8",
			limitParam:      8,
			isEmpty:         false,
			queryUsesLabels: true,
		},
		expectedJoinTable: map[string]int{"knot": 1},
	})
	t.Parallel()
	dbName := "something"
	mainFieldPrefix := "f1"
	const includeSort = false
	const isSummaryFilter = true
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			store.EXPECT().GetName().Return("something").AnyTimes()
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: toIndexedFieldsFromColumnNames("metadata.name", "metadata.namespace", "metadata.queryField1", "metadata.state.name", "spec.containers.image"),
			}
			joinTableIndexByLabelName := make(map[string]int)
			filterComponents, err := lii.compileQuery(&test.listOptions, test.partitions, test.ns, dbName, mainFieldPrefix, joinTableIndexByLabelName, includeSort, isSummaryFilter)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
				return
			}
			require.Nil(t, err)
			expectedFilters := test.expectedFilters
			if expectedFilters.joinParts == nil {
				expectedFilters.joinParts = make([]joinPart, 0)
			}
			if expectedFilters.whereClauses == nil {
				expectedFilters.whereClauses = make([]string, 0)
			}
			if expectedFilters.params == nil {
				expectedFilters.params = make([]any, 0)
			}
			if test.expectedJoinTable == nil {
				test.expectedJoinTable = make(map[string]int)
			}
			assert.Equal(t, test.expectedFilters, filterComponents)
			assert.Equal(t, test.expectedJoinTable, joinTableIndexByLabelName)
		})
	}
}

func TestPopulateSummaryObject(t *testing.T) {
	type testCase struct {
		description        string
		itemLists          [][]string
		summaryNamespaced  bool
		expectedAPISummary *types.APISummary
		expectedErr        string
	}

	var tests []testCase
	// summary=metadata.labels.status&summarynamespaced
	// summary=metadata.queryField1&filter=metadata.namespace=cars
	// As soon as we have a non-empty body build a with-statement
	tests = append(tests, testCase{
		description: "TestPopulateSummaryObject: one summary field, not namespaced",
		itemLists: [][]string{
			{"language", "2", "english"},
			{"language", "5", "french"},
			{"language", "1", "latverian"},
		},
		expectedAPISummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				types.SummaryEntry{
					Property: "language",
					Counts: map[string]types.SummaryWithBreakdown{
						"english":   types.SummaryWithBreakdown{Total: 2},
						"french":    types.SummaryWithBreakdown{Total: 5},
						"latverian": types.SummaryWithBreakdown{Total: 1},
					},
				},
			},
		},
	})
	tests = append(tests, testCase{
		description: "TestPopulateSummaryObject: three summary fields sort correctly, not namespaced",
		itemLists: [][]string{
			{"language", "2", "english"},
			{"animal", "7", "zebra"},
			{"language", "5", "french"},
			{"boat", "6", "ferry"},
			{"language", "1", "latverian"},
			{"boat", "2", "catamaran"},
			{"animal", "5", "aardvark"},
		},
		expectedAPISummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				types.SummaryEntry{
					Property: "animal",
					Counts: map[string]types.SummaryWithBreakdown{
						"aardvark": types.SummaryWithBreakdown{Total: 5},
						"zebra":    types.SummaryWithBreakdown{Total: 7},
					},
				},
				types.SummaryEntry{
					Property: "boat",
					Counts: map[string]types.SummaryWithBreakdown{
						"ferry":     types.SummaryWithBreakdown{Total: 6},
						"catamaran": types.SummaryWithBreakdown{Total: 2},
					},
				},
				types.SummaryEntry{
					Property: "language",
					Counts: map[string]types.SummaryWithBreakdown{
						"english":   types.SummaryWithBreakdown{Total: 2},
						"french":    types.SummaryWithBreakdown{Total: 5},
						"latverian": types.SummaryWithBreakdown{Total: 1},
					},
				},
			},
		},
	})
	tests = append(tests, testCase{
		description: "TestPopulateSummaryObject: multiple summary fields, namespaced",
		itemLists: [][]string{
			{"language", "2", "english", "ns1"},
			{"language", "3", "english", "ns2"},
			{"language", "4", "english", "ns3"},
			{"language", "5", "french", "ns1"},
			{"language", "6", "french", "ns2"},
			{"language", "11", "latverian", "ns1"},
			{"language", "12", "latverian", "ns2"},
			{"language", "13", "latverian", "ns3"},
			{"language", "14", "latverian", "ns4"},
		},
		summaryNamespaced: true,
		expectedAPISummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				types.SummaryEntry{
					Property: "language",
					Counts: map[string]types.SummaryWithBreakdown{
						"english": types.SummaryWithBreakdown{
							Total:     9,
							Namespace: map[string]int{"ns1": 2, "ns2": 3, "ns3": 4},
						},
						"french": types.SummaryWithBreakdown{
							Total:     11,
							Namespace: map[string]int{"ns1": 5, "ns2": 6},
						},
						"latverian": types.SummaryWithBreakdown{
							Total:     50,
							Namespace: map[string]int{"ns1": 11, "ns2": 12, "ns3": 13, "ns4": 14},
						},
					},
				},
			},
		},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			summary := types.APISummary{}
			err := populateSummaryObject(test.itemLists, test.summaryNamespaced, &summary)
			if test.expectedErr != "" {
				assert.Error(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			require.NoError(t, err)
			sortedSummary := sortSummaries(&summary)
			assert.Equal(t, *test.expectedAPISummary, *sortedSummary)
		})
	}
}
