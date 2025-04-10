/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
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
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		// create field table
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		// create field table indexes
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		loi, err := NewListOptionIndexer(context.Background(), fields, store, true)
		assert.Nil(t, err)
		assert.NotNil(t, loi)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		_, err := NewListOptionIndexer(context.Background(), fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from Begin(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(context.Background(), fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Exec() when creating fields table, should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, fmt.Errorf("error"))
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err == nil {
					t.Fail()
				}
			})

		_, err := NewListOptionIndexer(context.Background(), fields, store, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from create-labels, should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, fmt.Errorf("error"))
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err == nil {
					t.Fail()
				}
			})

		_, err := NewListOptionIndexer(context.Background(), fields, store, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Commit(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		_, err := NewListOptionIndexer(context.Background(), fields, store, true)
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
		extraIndexedFields    []string
		expectedList          *unstructured.UnstructuredList
		returnList            []any
		expectedContToken     string
		expectedErr           error
	}

	testObject := testStoreObject{Id: "something", Val: "a"}
	unstrTestObjectMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&testObject)
	assert.Nil(t, err)

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
  ORDER BY f."metadata.name" ASC`,
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
  ORDER BY f."metadata.name" ASC`,
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
    (FALSE))`,
		expectedCountStmtArgs: []any{},
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
    (FALSE))`,
		expectedCountStmtArgs: []any{},
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
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      Eq,
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
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"%somevalue%"},
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
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
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
    (f."metadata.somefield" NOT LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"%somevalue%"},
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
						Matches: []string{"somevalue"},
						Op:      Eq,
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
  ORDER BY f."metadata.name" ASC`,
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
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"someothervalue"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somethirdvalue"},
						Op:      NotEq,
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
    ((f."metadata.somefield" LIKE ? ESCAPE '\') OR (f."metadata.somefield" LIKE ? ESCAPE '\') OR (f."metadata.somefield" NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"%somevalue%", "%someothervalue%", "%somethirdvalue%"},
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
						Matches: []string{"value1"},
						Op:      Eq,
						Partial: false,
					},
					{
						Field:   []string{"status", "someotherfield"},
						Matches: []string{"value2"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"value3"},
						Op:      Eq,
						Partial: false,
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
    ((f."metadata.somefield" = ?) OR (f."status.someotherfield" != ?)) AND
    (f."metadata.somefield" = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"value1", "value2", "value3", "test4"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with labels filter should select the label in the prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "guard.cattle.io"},
						Matches: []string{"lodgepole"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test41",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`, //'
		expectedStmtArgs:  []any{"guard.cattle.io", "%lodgepole%", "test41"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with two labels filters should use a self-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"milk"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "horses"},
						Matches: []string{"saddles"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test42",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (lt2.label = ? AND lt2.value = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"cows", "milk", "horses", "saddles", "test42"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with a mix of one label and one non-label query can still self-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"butter"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"wheat"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test43",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (f."metadata.somefield" = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs:  []any{"cows", "butter", "wheat", "test43"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with only one Sort.Field set should sort on that field only, in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  ASC,
					},
				},
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
		description: "sort one field descending",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  DESC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "test5a",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.somefield" DESC`,
		expectedStmtArgs:  []any{"test5a"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "sort one unbound label descending",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "labels", "flip"},
						Order:  DESC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "test5a",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY (CASE lt1.label WHEN ? THEN lt1.value ELSE NULL END) DESC NULLS FIRST`,
		expectedStmtArgs:  []any{"flip", "test5a", "flip"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions sorting on two complex fields should sort on the first field in ascending order first and then sort on the second labels field in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "fields", "3"},
						Order:  ASC,
					},
					{
						Fields: []string{"metadata", "labels", "stub.io/candy"},
						Order:  ASC,
					},
				},
			},
		},
		extraIndexedFields: []string{"metadata.fields[3]", "metadata.labels[stub.io/candy]"},
		partitions:         []partition.Partition{},
		ns:                 "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ?) AND
    (FALSE)
  ORDER BY f."metadata.fields[3]" ASC, (CASE lt1.label WHEN ? THEN lt1.value ELSE NULL END) ASC NULLS LAST`,
		expectedStmtArgs:  []any{"stub.io/candy", "stub.io/candy"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in ascending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  ASC,
					},
					{
						Fields: []string{"status", "someotherfield"},
						Order:  ASC,
					},
				},
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
		description: "ListByOptions sorting on two fields should sort on the first field in descending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  DESC,
					},
					{
						Fields: []string{"status", "someotherfield"},
						Order:  ASC,
					},
				},
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
    (FALSE))`,
		expectedCountStmtArgs: []any{},
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
  ORDER BY f."metadata.name" ASC`,
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
    (FALSE))`,
		expectedCountStmtArgs: []any{},

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
  ORDER BY f."metadata.name" ASC`,
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
  ORDER BY f."metadata.name" ASC`,
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
  ORDER BY f."metadata.name" ASC`,
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
  ORDER BY f."metadata.name" ASC`,
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
			if len(test.extraIndexedFields) > 0 {
				lii.indexedFields = append(lii.indexedFields, test.extraIndexedFields...)
			}
			if test.description == "ListByOptions with labels filter should select the label in the prepared sql.Stmt" {
				fmt.Printf("stop here")
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, "something")
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			if test.expectedStmtArgs == nil {
				test.expectedStmtArgs = []any{}
			}
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)

			stmt := &sql.Stmt{}
			rows := &sql.Rows{}
			objType := reflect.TypeOf(testObject)
			txClient.EXPECT().Stmt(gomock.Any()).Return(stmts).AnyTimes()
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

			store.EXPECT().WithTransaction(gomock.Any(), false, gomock.Any()).Return(nil).Do(
				func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
					err := f(txClient)
					if test.expectedErr == nil {
						assert.Nil(t, err)
					} else {
						assert.Equal(t, test.expectedErr, err)
					}
				})

			if test.expectedCountStmt != "" {
				store.EXPECT().Prepare(test.expectedCountStmt).Return(stmt)
				store.EXPECT().ReadInt(rows).Return(len(test.expectedList.Items), nil)
				store.EXPECT().CloseStmt(stmt).Return(nil)
			}
			list, total, contToken, err := lii.executeQuery(context.Background(), queryInfo)
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

func TestConstructQuery(t *testing.T) {
	type testCase struct {
		description           string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           error
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles IN statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      In,
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
    (f."metadata.queryField1" IN (?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOT-IN statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      NotIn,
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
    (f."metadata.queryField1" NOT IN (?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    Exists,
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedErr: errors.New("NULL and NOT NULL tests aren't supported for non-label queries"),
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOT-EXISTS statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    NotExists,
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedErr: errors.New("NULL and NOT NULL tests aren't supported for non-label queries"),
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualFull"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelEqualFull", "somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements, match partial",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`, //'
		expectedStmtArgs: []any{"labelEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualFull"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelNotEqualFull", "labelNotEqualFull", "somevalue"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements, match partial",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`, //'
		expectedStmtArgs: []any{"labelNotEqualPartial", "labelNotEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles multiple != statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual1"},
						Matches: []string{"value1"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual2"},
						Matches: []string{"value2"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)) AND
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt2i1 ON o1.key = lt2i1.key
		WHERE lt2i1.label = ?)) OR (lt2.label = ? AND lt2.value != ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"notEqual1", "notEqual1", "value1", "notEqual2", "notEqual2", "value2"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles IN statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      In,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value IN (?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTIN statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      NotIn,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value NOT IN (?, ?))) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelNOTIN", "labelNOTIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEXISTS"},
						Matches: []string{},
						Op:      Exists,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelEXISTS"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTEXISTS statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTEXISTS"},
						Matches: []string{},
						Op:      NotExists,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"labelNOTEXISTS"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles LessThan statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"5"},
						Op:      Lt,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value < ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"numericThing", float64(5)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles GreaterThan statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"35"},
						Op:      Gt,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value > ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"numericThing", float64(35)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters with a positive label test and a negative non-label test still outer-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "junta"},
						Matches: []string{"esther"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"golgi"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"status", "queryField2"},
						Matches: []string{"gold"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') OR (f."metadata.queryField1" NOT LIKE ? ESCAPE '\')) AND
    (f."status.queryField2" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"junta", "%esther%", "%golgi%", "gold"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters and or-filters with a positive label test and a negative non-label test still outer-join and have correct AND/ORs",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "nectar"},
						Matches: []string{"stash"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"landlady"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "lawn"},
						Matches: []string{"reba", "coil"},
						Op:      In,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      Gt,
						Matches: []string{"2"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') OR (f."metadata.queryField1" != ?)) AND
    ((lt2.label = ? AND lt2.value IN (?, ?)) OR (f."metadata.queryField1" > ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`, //'
		expectedStmtArgs: []any{"nectar", "%stash%", "landlady", "lawn", "reba", "coil", float64(2)},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements, match partial, sort on metadata.queryField1",
		listOptions: ListOptions{
			Filters: []OrFilter{
				{
					[]Filter{
						{
							Field:   []string{"metadata", "labels", "labelEqualPartial"},
							Matches: []string{"somevalue"},
							Op:      Eq,
							Partial: true,
						},
					},
				},
			},
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "queryField1"},
						Order:  ASC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.queryField1" ASC`,
		expectedStmtArgs: []any{"labelEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: sort on label statements with no query",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "labels", "this"},
						Order:  ASC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ?) AND
    (FALSE)
  ORDER BY (CASE lt1.label WHEN ? THEN lt1.value ELSE NULL END) ASC NULLS LAST`, //'
		expectedStmtArgs: []any{"this", "this"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: sort and query on both labels and non-labels without overlap",
		listOptions: ListOptions{
			Filters: []OrFilter{
				{
					[]Filter{
						{
							Field:   []string{"metadata", "queryField1"},
							Matches: []string{"toys"},
							Op:      Eq,
						},
						{
							Field:   []string{"metadata", "labels", "jamb"},
							Matches: []string{"juice"},
							Op:      Eq,
						},
					},
				},
			},
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields: []string{"metadata", "labels", "this"},
						Order:  ASC,
					},
					{
						Fields: []string{"status", "queryField2"},
						Order:  DESC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((f."metadata.queryField1" = ?) OR (lt1.label = ? AND lt1.value = ?) OR (lt2.label = ?)) AND
    (FALSE)
  ORDER BY (CASE lt2.label WHEN ? THEN lt2.value ELSE NULL END) ASC NULLS LAST, f."status.queryField2" DESC`,
		expectedStmtArgs: []any{"toys", "jamb", "juice", "this", "this"},
		expectedErr:      nil,
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			if test.description == "TestConstructQuery: sort and query on both labels and non-labels without overlap" {
				fmt.Printf("stop here")
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, "something")
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}
}

func TestSmartJoin(t *testing.T) {
	type testCase struct {
		description       string
		fieldArray        []string
		expectedFieldName string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description:       "single-letter names should be dotted",
		fieldArray:        []string{"metadata", "labels", "a"},
		expectedFieldName: "metadata.labels.a",
	})
	tests = append(tests, testCase{
		description:       "underscore should be dotted",
		fieldArray:        []string{"metadata", "labels", "_"},
		expectedFieldName: "metadata.labels._",
	})
	tests = append(tests, testCase{
		description:       "simple names should be dotted",
		fieldArray:        []string{"metadata", "labels", "queryField2"},
		expectedFieldName: "metadata.labels.queryField2",
	})
	tests = append(tests, testCase{
		description:       "a numeric field should be bracketed",
		fieldArray:        []string{"metadata", "fields", "43"},
		expectedFieldName: "metadata.fields[43]",
	})
	tests = append(tests, testCase{
		description:       "a field starting with a number should be bracketed",
		fieldArray:        []string{"metadata", "fields", "46days"},
		expectedFieldName: "metadata.fields[46days]",
	})
	tests = append(tests, testCase{
		description:       "compound names should be bracketed",
		fieldArray:        []string{"metadata", "labels", "rancher.cattle.io/moo"},
		expectedFieldName: "metadata.labels[rancher.cattle.io/moo]",
	})
	tests = append(tests, testCase{
		description:       "space-separated names should be bracketed",
		fieldArray:        []string{"metadata", "labels", "space here"},
		expectedFieldName: "metadata.labels[space here]",
	})
	tests = append(tests, testCase{
		description:       "already-bracketed terms cause double-bracketing and should never be used",
		fieldArray:        []string{"metadata", "labels[k8s.io/deepcode]"},
		expectedFieldName: "metadata[labels[k8s.io/deepcode]]",
	})
	tests = append(tests, testCase{
		description:       "an empty array should be an empty string",
		fieldArray:        []string{},
		expectedFieldName: "",
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			gotFieldName := smartJoin(test.fieldArray)
			assert.Equal(t, test.expectedFieldName, gotFieldName)
		})
	}
}

func TestBuildSortLabelsClause(t *testing.T) {
	type testCase struct {
		description               string
		labelName                 string
		joinTableIndexByLabelName map[string]int
		direction                 bool
		expectedStmt              string
		expectedParam             string
		expectedErr               string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "TestBuildSortClause: empty index list errors",
		labelName:   "emptyListError",
		expectedErr: `internal error: no join-table index given for labelName "emptyListError"`,
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit ascending",
		labelName:                 "testBSL1",
		joinTableIndexByLabelName: map[string]int{"db:testBSL1": 3},
		direction:                 true,
		expectedStmt:              `(CASE lt3.label WHEN ? THEN lt3.value ELSE NULL END) ASC NULLS LAST`,
		expectedParam:             "testBSL1",
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit descending",
		labelName:                 "testBSL2",
		joinTableIndexByLabelName: map[string]int{"db:testBSL2": 4},
		direction:                 false,
		expectedStmt:              `(CASE lt4.label WHEN ? THEN lt4.value ELSE NULL END) DESC NULLS FIRST`,
		expectedParam:             "testBSL2",
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			stmt, param, err := buildSortLabelsClause(test.labelName, "db", test.joinTableIndexByLabelName, test.direction)
			if test.expectedErr != "" {
				assert.Equal(t, test.expectedErr, err.Error())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.expectedStmt, stmt)
				assert.Equal(t, test.expectedParam, param)
			}
		})
	}
}

func TestConstructIndirectLabelFilterQuery(t *testing.T) {
	type testCase struct {
		description           string
		dbname                string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect Eq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
		WHERE (lt1.label = ? AND ext2."spec.displayName" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", "System"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect NotEq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"System"},
						Op:             NotEq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
	JOIN "_v1_Namespace_fields" f ON o.key = f.key
	LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
		WHERE (lt1.label = ? AND ext2."spec.displayName" != ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", "System"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect Lt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"10"},
						Op:             Lt,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
  WHERE (lt1.label = ? AND ext2."spec.displayName" < ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", float64(10)},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect Gt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"11"},
						Op:             Gt,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
  WHERE (lt1.label = ? AND ext2."spec.displayName" > ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", float64(11)},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect Exists",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Op:             Exists,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
    WHERE (lt1.label = ? AND ext2."spec.displayName" != NULL) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect Not-Exists",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Op:             NotExists,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
		WHERE (lt1.label = ? AND ext2."spec.displayName" == NULL) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect In-Set",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"fish", "cows", "ships"},
						Op:             In,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
		WHERE (lt1.label = ? AND ext2."spec.displayName" IN (?, ?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", "fish", "cows", "ships"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: simple redirect NotIn",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"balloons", "clubs", "cheese"},
						Op:             NotIn,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key 
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
	WHERE (lt1.label = ? AND ext2."spec.displayName" NOT IN (?, ?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", "balloons", "clubs", "cheese"},
		expectedErr:      "",
	})
	// There's no easy way to safely parameterize column names, as opposed to values,
	// so verify that the code generator checked them for whitelisted characters.
	// Allow only [-a-zA-Z0-9$_\[\].]+
	tests = append(tests, testCase{
		description: "IndirectFilterQuery: verify the injected field name is safe",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "projects", "name ; drop database marks ; select * from _v1_Namespace", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "invalid database column name 'name ; drop database marks ; select * from _v1_Namespace'",
	})

	t.Parallel()
	ptn := regexp.MustCompile(`\s+`)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			dbname := test.dbname
			if dbname == "" {
				dbname = "sometable"
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, dbname)
			if test.expectedErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			assert.Nil(t, err)
			expectedStmt := ptn.ReplaceAllString(strings.TrimSpace(test.expectedStmt), " ")
			receivedStmt := ptn.ReplaceAllString(strings.TrimSpace(queryInfo.query), " ")
			assert.Equal(t, expectedStmt, receivedStmt)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}

}

func TestConstructIndirectNonLabelFilterQuery(t *testing.T) {
	type testCase struct {
		description           string
		dbname                string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect Eq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key 
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
  WHERE (ext1."spec.displayName" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"System"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect NotEq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"System"},
						Op:             NotEq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
  WHERE (ext1."spec.displayName" != ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"System"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect Lt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"10"},
						Op:             Lt,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
  WHERE (ext1."spec.displayName" < ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{float64(10)},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect Gt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"11"},
						Op:             Gt,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
  WHERE (ext1."spec.displayName" > ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{float64(11)},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect Exists",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Op:             Exists,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
    WHERE (ext1."spec.displayName" != NULL) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect Not-Exists",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Op:             NotExists,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
		WHERE (ext1."spec.displayName" == NULL) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect In-Set",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"fish", "cows", "ships"},
						Op:             In,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
		WHERE (ext1."spec.displayName" IN (?, ?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"fish", "cows", "ships"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: simple redirect NotIn",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"balloons", "clubs", "cheese"},
						Op:             NotIn,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    JOIN "management.cattle.io_v3_Project_fields" ext1 ON f."metadata.queryField1" = ext1."metadata.name"
	WHERE (ext1."spec.displayName" NOT IN (?, ?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"balloons", "clubs", "cheese"},
		expectedErr:      "",
	})
	// There's no easy way to safely parameterize column names, as opposed to values,
	// so verify that the code generator checked them for whitelisted characters.
	// Allow only [-a-zA-Z0-9$_\[\].]+
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: verify the injected external field name is safe",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "projects", "name ; drop database marks ; select * from _v1_Namespace", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "invalid database column name 'name ; drop database marks ; select * from _v1_Namespace'",
	})
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - no label: verify the injected selecting field name is safe",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:          []string{"metadata", "queryField1 ; drop database thought-this-is=-checked"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "projects", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "column is invalid [metadata[queryField1 ; drop database thought-this-is=-checked]]: supplied column is invalid",
	})

	t.Parallel()
	ptn := regexp.MustCompile(`\s+`)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			dbname := test.dbname
			if dbname == "" {
				dbname = "sometable"
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, dbname)
			if test.expectedErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			require.Nil(t, err)
			expectedStmt := ptn.ReplaceAllString(strings.TrimSpace(test.expectedStmt), " ")
			receivedStmt := ptn.ReplaceAllString(strings.TrimSpace(queryInfo.query), " ")
			assert.Equal(t, expectedStmt, receivedStmt)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}
}

func TestConstructMixedLabelIndirect(t *testing.T) {
	type testCase struct {
		description           string
		dbname                string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - one label, one redirect Eq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "radio"},
						Matches: []string{"fish"},
						Op:      Eq,
					},
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"System"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON f."metadata.queryField1" = ext2."metadata.name"
		WHERE ((lt1.label = ? AND lt1.value = ?) OR (ext2."spec.displayName" = ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"radio", "fish", "System"},
		expectedErr:      "",
	})

	t.Parallel()
	ptn := regexp.MustCompile(`\s+`)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			dbname := test.dbname
			if dbname == "" {
				dbname = "sometable"
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, dbname)
			if test.expectedErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			require.Nil(t, err)
			expectedStmt := ptn.ReplaceAllString(strings.TrimSpace(test.expectedStmt), " ")
			receivedStmt := ptn.ReplaceAllString(strings.TrimSpace(queryInfo.query), " ")
			assert.Equal(t, expectedStmt, receivedStmt)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}
}

func TestConstructMixedMultiTypes(t *testing.T) {
	type testCase struct {
		description           string
		dbname                string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "IndirectFilterQuery - mix of label,non-label x direct,indirect Eq",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "suitcase"},
						Matches: []string{"valid"},
						Op:      Eq,
					},
					{
						Field:          []string{"metadata", "queryField1"},
						Matches:        []string{"sprint"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.displayName"},
					},
					{
						Field:   []string{"status", "queryField2"},
						Matches: []string{"moisture"},
						Op:      Eq,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:          []string{"metadata", "labels", "green"},
						Matches:        []string{"squalor"},
						Op:             Eq,
						IsIndirect:     true,
						IndirectFields: []string{"tournaments.cattle.io/v3", "Diary", "metadata.name", "spocks.brain"},
					},
				},
			},
		}},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "_v1_Namespace" o
    JOIN "_v1_Namespace_fields" f ON o.key = f.key
    LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    JOIN "management.cattle.io_v3_Project_fields" ext2 ON f."metadata.queryField1" = ext2."metadata.name"
    LEFT OUTER JOIN "_v1_Namespace_labels" lt3 ON o.key = lt3.key
    JOIN "tournaments.cattle.io_v3_Diary_fields" ext4 ON lt3.value = ext4."metadata.name"
		WHERE ((lt1.label = ? AND lt1.value = ?)
			    OR (ext2."spec.displayName" = ?)
			    OR (f."status.queryField2" = ?))
		AND (lt3.label = ? AND ext4."spocks.brain" = ?)
		AND (FALSE)
  ORDER BY f."metadata.name" ASC`,
		expectedStmtArgs: []any{"suitcase", "valid", "sprint", "moisture", "green", "squalor"},
		expectedErr:      "",
	})

	t.Parallel()
	ptn := regexp.MustCompile(`\s+`)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			dbname := test.dbname
			if dbname == "" {
				dbname = "sometable"
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, dbname)
			if test.expectedErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			require.Nil(t, err)
			expectedStmt := ptn.ReplaceAllString(strings.TrimSpace(test.expectedStmt), " ")
			receivedStmt := ptn.ReplaceAllString(strings.TrimSpace(queryInfo.query), " ")
			assert.Equal(t, expectedStmt, receivedStmt)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}
}

func TestConstructSimpleIndirectSort(t *testing.T) {
	type testCase struct {
		description           string
		dbname                string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "SimpleIndirectSort - one sort, no filters, happy path",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields:         []string{"metadata", "labels", "field.cattle.io/projectId"},
						Order:          ASC,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "spec.clusterName"},
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		dbname:     "_v1_Namespace",
		expectedStmt: `SELECT __ix_object, __ix_objectnonce, __ix_dekid FROM (
  SELECT o.object AS __ix_object, o.objectnonce AS __ix_objectnonce, o.dekid AS __ix_dekid, ext2."spec.clusterName" AS __ix_ext2_spec_clusterName, f."metadata.name" AS __ix_f_metadata_name FROM
    "_v1_Namespace" o
      JOIN "_v1_Namespace_fields" f ON o.key = f.key
      LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
      JOIN "management.cattle.io_v3_Project_fields" ext2 ON lt1.value = ext2."metadata.name"
    WHERE (lt1.label = ?)
UNION ALL
  SELECT o.object AS __ix_object, o.objectnonce AS __ix_objectnonce, o.dekid AS __ix_dekid, NULL AS __ix_ext2_spec_clusterName, NULL AS __ix_f_metadata_name FROM
    "_v1_Namespace" o
      JOIN "_v1_Namespace_fields" f ON o.key = f.key
      LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON o.key = lt1.key
    WHERE (o.key NOT IN (SELECT o1.key FROM "_v1_Namespace" o1
		JOIN "_v1_Namespace_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "_v1_Namespace_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?))
)
WHERE FALSE
  ORDER BY __ix_ext2_spec_clusterName ASC NULLS LAST, __ix_f_metadata_name ASC`,
		expectedStmtArgs: []any{"field.cattle.io/projectId", "field.cattle.io/projectId"},
		expectedErr:      "",
	})
	tests = append(tests, testCase{
		description: "SimpleIndirectSort - one sort, invalid external selector-column",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields:         []string{"metadata", "labels", "field.cattle.io/projectId"},
						Order:          ASC,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "foo; drop database bobby1", "spec.clusterName"},
					},
				},
			},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "invalid database column name 'foo; drop database bobby1'",
	})
	tests = append(tests, testCase{
		description: "SimpleIndirectSort - one sort, invalid external selector-column",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields:         []string{"metadata", "labels", "field.cattle.io/projectId"},
						Order:          ASC,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "bar; drop database bobby2"},
					},
				},
			},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "invalid database column name 'bar; drop database bobby2'",
	})
	tests = append(tests, testCase{
		description: "SimpleIndirectSort - one sort, not enough indirect fields",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields:         []string{"metadata", "labels", "field.cattle.io/projectId"},
						Order:          ASC,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name"},
					},
				},
			},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "expected indirect sort directive to have 4 indirect fields, got 3",
	})
	tests = append(tests, testCase{
		description: "SimpleIndirectSort - one sort, too many indirect fields",
		listOptions: ListOptions{
			SortList: SortList{
				SortDirectives: []Sort{
					{
						Fields:         []string{"metadata", "labels", "field.cattle.io/projectId"},
						Order:          ASC,
						IsIndirect:     true,
						IndirectFields: []string{"management.cattle.io/v3", "Project", "metadata.name", "little", "bobby-tables"},
					},
				},
			},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		dbname:      "_v1_Namespace",
		expectedErr: "expected indirect sort directive to have 4 indirect fields, got 5",
	})
	t.Parallel()
	ptn := regexp.MustCompile(`\s+`)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			dbname := test.dbname
			if dbname == "" {
				dbname = "sometable"
			}
			queryInfo, err := lii.constructQuery(&test.listOptions, test.partitions, test.ns, dbname)
			if test.expectedErr != "" {
				require.NotNil(t, err)
				assert.Equal(t, test.expectedErr, err.Error())
				return
			}
			require.Nil(t, err)
			expectedStmt := ptn.ReplaceAllString(strings.TrimSpace(test.expectedStmt), " ")
			receivedStmt := ptn.ReplaceAllString(strings.TrimSpace(queryInfo.query), " ")
			assert.Equal(t, expectedStmt, receivedStmt)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
		})
	}
}
