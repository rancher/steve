/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/rancher/lasso/pkg/cache/sql/partition"
)

func TestNewListOptionIndexer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "NewListOptionIndexer() with no errors returned, should return no error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().RegisterAfterDelete(gomock.Any())

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		// create field table
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		// create field table indexes
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil)
		txClient.EXPECT().Commit().Return(nil)

		loi, err := NewListOptionIndexer(fields, store, true)
		assert.Nil(t, err)
		assert.NotNil(t, loi)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from NewIndxer(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from Begin(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().RegisterAfterDelete(gomock.Any())

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Exec() when creating fields table, should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().RegisterAfterDelete(gomock.Any())

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Commit(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().RegisterAfterDelete(gomock.Any())

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, true)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestListByOptions(t *testing.T) {
	type testCase struct {
		description           string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedList          *unstructured.UnstructuredList
		returnList            []any
		expectedContToken     string
		expectedErr           error
	}

	testObject := testStoreObject{Id: "something", Val: "a"}
	unstrTestObjectMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&testObject)
	assert.Nil(t, err)
	// unstrTestObject
	var tests []testCase
	tests = append(tests, testCase{
		description: "ListByOptions() with no errors returned, should not return an error",
		listOptions: ListOptions{},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with an empty filter, should not return an error",
		listOptions: ListOptions{
			Filters: []OrFilter{{[]Filter{}}},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with ChunkSize set should set limit in prepared sql.Stmt",
		listOptions: ListOptions{ChunkSize: 2},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?`,
		expectedStmtArgs: []interface{}{2},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC )`,
		expectedCountStmtArgs: []interface{}{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Resume set should set offset in prepared sql.Stmt",
		listOptions: ListOptions{Resume: "4"},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  OFFSET ?`,
		expectedStmtArgs: []interface{}{4},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC )`,
		expectedCountStmtArgs: []interface{}{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter should select where that filter is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "somefield"},
						Match: "somevalue",
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"somevalue"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Op set top NotEq should select where that filter is not true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "somefield"},
						Match: "somevalue",
						Op:    NotEq,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" NOT LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"somevalue"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Partial set to true should select where that partial match on that filter's value is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Match:   "somevalue",
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with multiple filters should select where any of those filters are true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Match:   "somevalue",
						Partial: true,
					},
					{
						Field: []string{"metadata", "somefield"},
						Match: "someothervalue",
					},
					{
						Field: []string{"metadata", "somefield"},
						Match: "somethirdvalue",
						Op:    NotEq,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\' OR f."metadata.somefield" LIKE ? ESCAPE '\' OR f."metadata.somefield" NOT LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%", "someothervalue", "somethirdvalue"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with multiple OrFilters set should select where all OrFilters contain one filter that is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Match:   "somevalue",
						Partial: true,
					},
					{
						Field: []string{"status", "someotherfield"},
						Match: "someothervalue",
						Op:    NotEq,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field: []string{"metadata", "somefield"},
						Match: "somethirdvalue",
						Op:    Eq,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test4",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\' OR f."status.someotherfield" NOT LIKE ? ESCAPE '\') AND
    (f."metadata.somefield" LIKE ? ESCAPE '\') AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%", "someothervalue", "somethirdvalue", "test4"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.PrimaryField set only should sort on that field only, in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				PrimaryField: []string{"metadata", "somefield"},
			},
		},
		partitions: []partition.Partition{},
		ns:         "test5",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.somefield" ASC`,
		expectedStmtArgs:  []any{"test5"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.SecondaryField set only should sort on that field only, in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				SecondaryField: []string{"metadata", "somefield"},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.PrimaryField and Sort.SecondaryField set should sort on PrimaryField in ascending order first and then sort on SecondaryField in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				PrimaryField:   []string{"metadata", "somefield"},
				SecondaryField: []string{"status", "someotherfield"},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" ASC, f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.PrimaryField and Sort.SecondaryField set and PrimaryOrder set to DESC should sort on PrimaryField in descending order first and then sort on SecondaryField in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				PrimaryField:   []string{"metadata", "somefield"},
				SecondaryField: []string{"status", "someotherfield"},
				PrimaryOrder:   DESC,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" DESC, f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.SecondaryField set and Sort.PrimaryOrder set to descending should sort on that SecondaryField in ascending order only and ignore PrimaryOrder in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				SecondaryField: []string{"status", "someotherfield"},
				PrimaryOrder:   DESC,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Sort.PrimaryOrder set only should sort on default primary and secondary fields in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				PrimaryOrder: DESC,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.PageSize set should set limit to PageSize in prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				PageSize: 10,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?`,
		expectedStmtArgs: []any{10},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC )`,
		expectedCountStmtArgs: []interface{}{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and no PageSize set should not add anything to prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				Page: 2,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and PageSize set limit to PageSize and offset to PageSize * (Page - 1) in prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				PageSize: 10,
				Page:     2,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?
  OFFSET ?`,
		expectedStmtArgs: []any{10, 10},

		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC )`,
		expectedCountStmtArgs: []interface{}{},

		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Namespace Partition should select only items where metadata.namespace is equal to Namespace and all other conditions are met in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Namespace: "somens",
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ? AND FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"somens"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a All Partition should select all items that meet all other conditions in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Passthrough Partition should select all items that meet all other conditions prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Passthrough: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Names Partition should select only items where metadata.name equals an items in Names and all other conditions are met in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Names: sets.New[string]("someid", "someotherid"),
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.name" IN (?, ?))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"someid", "someotherid"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			txClient := NewMockTXClient(gomock.NewController(t))
			store := NewMockStore(gomock.NewController(t))
			stmts := NewMockStmt(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.somefield", "status.someotherfield"},
			}
			stmt := &sql.Stmt{}
			rows := &sql.Rows{}
			objType := reflect.TypeOf(testObject)
			store.EXPECT().BeginTx(gomock.Any(), false).Return(txClient, nil)
			txClient.EXPECT().Stmt(gomock.Any()).Return(stmts).AnyTimes()
			store.EXPECT().GetName().Return("something").AnyTimes()
			store.EXPECT().Prepare(test.expectedStmt).Do(func(a ...any) {
				fmt.Println(a)
			}).Return(stmt)
			if args := test.expectedStmtArgs; args != nil {
				stmts.EXPECT().QueryContext(gomock.Any(), gomock.Any()).Return(rows, nil).AnyTimes()
			} else if strings.Contains(test.expectedStmt, "LIMIT") {
				stmts.EXPECT().QueryContext(gomock.Any(), args...).Return(rows, nil)
				txClient.EXPECT().Stmt(gomock.Any()).Return(stmts)
				stmts.EXPECT().QueryContext(gomock.Any()).Return(rows, nil)
			} else {
				stmts.EXPECT().QueryContext(gomock.Any()).Return(rows, nil)
			}
			store.EXPECT().GetType().Return(objType)
			store.EXPECT().GetShouldEncrypt().Return(false)
			store.EXPECT().ReadObjects(rows, objType, false).Return(test.returnList, nil)
			store.EXPECT().CloseStmt(stmt).Return(nil)

			if test.expectedCountStmt != "" {
				store.EXPECT().Prepare(test.expectedCountStmt).Return(stmt)
				//store.EXPECT().QueryForRows(context.TODO(), stmt, test.expectedCountStmtArgs...).Return(rows, nil)
				store.EXPECT().ReadInt(rows).Return(len(test.expectedList.Items), nil)
				store.EXPECT().CloseStmt(stmt).Return(nil)
			}
			txClient.EXPECT().Commit()
			list, total, contToken, err := lii.ListByOptions(context.TODO(), test.listOptions, test.partitions, test.ns)
			if test.expectedErr == nil {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, len(test.expectedList.Items), total)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}
