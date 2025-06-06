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
	"os"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/rancher/steve/pkg/sqlcache/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func makeListOptionIndexer(ctx context.Context, opts ListOptionIndexerOptions) (*ListOptionIndexer, string, error) {
	gvk := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "ConfigMap",
	}
	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)
	name := informerNameFromGVK(gvk)
	m, err := encryption.NewManager()
	if err != nil {
		return nil, "", err
	}

	db, dbPath, err := db.NewClient(nil, m, m, true)
	if err != nil {
		return nil, "", err
	}

	s, err := store.NewStore(ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, false, gvk, name, nil, nil)
	if err != nil {
		return nil, "", err
	}

	listOptionIndexer, err := NewListOptionIndexer(ctx, s, opts)
	if err != nil {
		return nil, "", err
	}

	return listOptionIndexer, dbPath, nil
}

func cleanTempFiles(basePath string) {
	os.Remove(basePath)
	os.Remove(basePath + "-shm")
	os.Remove(basePath + "-wal")
}

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
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)

		// create events table
		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
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

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		loi, err := NewListOptionIndexer(context.Background(), store, opts)
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

		opts := ListOptionIndexerOptions{
			Fields: fields,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
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
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))

		opts := ListOptionIndexerOptions{
			Fields: fields,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
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
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, fmt.Errorf("error"))
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err == nil {
					t.Fail()
				}
			})

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
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
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
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

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
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
		store.EXPECT().RegisterAfterAdd(gomock.Any())
		store.EXPECT().RegisterAfterUpdate(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(4)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)

		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
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

		opts := ListOptionIndexerOptions{
			Fields:       fields,
			IsNamespaced: true,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestNewListOptionIndexerEasy(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		description string
		listOptions sqltypes.ListOptions
		partitions  []partition.Partition
		ns          string

		items []*unstructured.Unstructured

		extraIndexedFields [][]string
		expectedList       *unstructured.UnstructuredList
		expectedTotal      int
		expectedContToken  string
		expectedErr        error
	}
	foo := map[string]any{
		"metadata": map[string]any{
			"name":      "obj1",
			"namespace": "ns-a",
			"somefield": "foo",
			"sortfield": "4",
		},
	}
	bar := map[string]any{
		"metadata": map[string]any{
			"name":      "obj2",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "1",
			"labels": map[string]any{
				"cows":   "milk",
				"horses": "saddles",
			},
		},
	}
	baz := map[string]any{
		"metadata": map[string]any{
			"name":      "obj3",
			"namespace": "ns-a",
			"somefield": "baz",
			"sortfield": "2",
			"labels": map[string]any{
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"someotherfield": "helloworld",
		},
	}
	toto := map[string]any{
		"metadata": map[string]any{
			"name":      "obj4",
			"namespace": "ns-a",
			"somefield": "toto",
			"sortfield": "2",
			"labels": map[string]any{
				"cows": "milk",
			},
		},
	}
	lodgePole := map[string]any{
		"metadata": map[string]any{
			"name":      "obj5",
			"namespace": "ns-b",
			"unknown":   "hi",
			"labels": map[string]any{
				"guard.cattle.io": "lodgepole",
			},
		},
	}

	makeList := func(t *testing.T, objs ...map[string]any) *unstructured.UnstructuredList {
		t.Helper()

		if len(objs) == 0 {
			return &unstructured.UnstructuredList{Object: map[string]any{"items": []any{}}, Items: []unstructured.Unstructured{}}
		}

		var items []any
		for _, obj := range objs {
			items = append(items, obj)
		}

		list := &unstructured.Unstructured{
			Object: map[string]any{
				"items": items,
			},
		}

		itemList, err := list.ToList()
		require.NoError(t, err)

		return itemList
	}
	itemList := makeList(t, foo, bar, baz, toto, lodgePole)

	var tests []testCase
	tests = append(tests, testCase{
		description:       "ListByOptions() with no errors returned, should not return an error",
		listOptions:       sqltypes.ListOptions{},
		partitions:        []partition.Partition{},
		ns:                "",
		expectedList:      makeList(t),
		expectedTotal:     0,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with an empty filter, should not return an error",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{{[]sqltypes.Filter{}}},
		},
		partitions:        []partition.Partition{},
		ns:                "",
		expectedList:      makeList(t),
		expectedTotal:     0,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter should select where that filter is true in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"foo"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Op set top NotEq should select where that filter is not true in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"foo"},
						Op:      sqltypes.NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, bar, baz, toto, lodgePole),
		expectedTotal:     4,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Partial set to true should select where that partial match on that filter's value is true in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"o"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo, toto),
		expectedTotal:     2,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with multiple filters should select where any of those filters are true in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"foo"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"bar"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"toto"},
						Op:      sqltypes.NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo, bar, baz, lodgePole),
		expectedTotal:     4,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with multiple OrFilters set should select where all OrFilters contain one filter that is true in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"foo"},
						Op:      sqltypes.Eq,
						Partial: false,
					},
					{
						Field:   []string{"status", "someotherfield"},
						Matches: []string{"helloworld"},
						Op:      sqltypes.NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"toto"},
						Op:      sqltypes.Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, toto),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with labels filter should select the label in the prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "guard.cattle.io"},
						Matches: []string{"lodgepole"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, lodgePole),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with two labels filters should use a self-join",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "cows"},
							Matches: []string{"milk"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
				{
					Filters: []sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "horses"},
							Matches: []string{"saddles"},
							Op:      sqltypes.Eq,
							Partial: false,
						},
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, bar),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a mix of one label and one non-label query can still self-join",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"milk"},
						Op:      sqltypes.Eq,
						Partial: false,
					},
				},
			},
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"toto"},
						Op:      sqltypes.Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, toto),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with only one Sort.Field set should sort on that field only, in ascending order in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, lodgePole, bar, baz, foo, toto),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "sort one field descending",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "somefield"},
						Order:  sqltypes.DESC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, toto, foo, baz, bar, lodgePole),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "sort one unbound field descending",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "unknown"},
						Order:  sqltypes.DESC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, lodgePole, toto, baz, bar, foo),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	// tests = append(tests, testCase{
	// 	description: "sort one unbound label descending",
	// 	listOptions: sqltypes.ListOptions{
	// 		SortList: sqltypes.SortList{
	// 			SortDirectives: []sqltypes.Sort{
	// 				{
	// 					Fields: []string{"metadata", "labels", "flip"},
	// 					Order:  sqltypes.DESC,
	// 				},
	// 			},
	// 		},
	// 	},
	// 	partitions:        []partition.Partition{{All: true}},
	// 	ns:                "",
	// 	expectedList:      makeList(t, lodgePole, toto, baz, bar, foo),
	// expectedTotal:     5,
	// 	expectedContToken: "",
	// 	expectedErr:       nil,
	// })
	// tests = append(tests, testCase{
	// 	description: "ListByOptions sorting on two complex fields should sort on the first field in ascending order first and then sort on the second labels field in ascending order in prepared sql.Stmt",
	// 	listOptions: sqltypes.ListOptions{
	// 		SortList: sqltypes.SortList{
	// 			SortDirectives: []sqltypes.Sort{
	// 				{
	// 					Fields: []string{"metadata", "sortfield"},
	// 					Order:  sqltypes.ASC,
	// 				},
	// 				{
	// 					Fields: []string{"metadata", "labels", "cows"},
	// 					Order:  sqltypes.ASC,
	// 				},
	// 			},
	// 		},
	// 	},
	// 	partitions:        []partition.Partition{{All: true}},
	// 	ns:                "",
	// 	expectedList:      makeList(t),
	// expectedTotal:     5,
	// 	expectedContToken: "",
	// 	expectedErr:       nil,
	// })
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in ascending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "sortfield"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "somefield"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, lodgePole, bar, baz, toto, foo),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in descending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "sortfield"},
						Order:  sqltypes.DESC,
					},
					{
						Fields: []string{"metadata", "somefield"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo, baz, toto, bar, lodgePole),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.PageSize set should set limit to PageSize in prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{
			Pagination: sqltypes.Pagination{
				PageSize: 3,
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo, bar, baz),
		expectedTotal:     5,
		expectedContToken: "3",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and no PageSize set should not add anything to prepared sql.Stmt",
		listOptions: sqltypes.ListOptions{
			Pagination: sqltypes.Pagination{
				Page: 2,
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, foo, bar, baz, toto, lodgePole),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	// tests = append(tests, testCase{
	// 	description: "ListByOptions with a Namespace Partition should select only items where metadata.namespace is equal to Namespace and all other conditions are met in prepared sql.Stmt",
	// 	partitions: []partition.Partition{
	// 		{
	// 			Namespace: "ns-b",
	// 		},
	// 	},
	// 	// XXX: Why do I need to specify the namespace here too?
	// 	ns:                "ns-b",
	// 	expectedList:      makeList(t, lodgePole),
	// 	expectedTotal:     1,
	// 	expectedContToken: "",
	// 	expectedErr:       nil,
	// })
	tests = append(tests, testCase{
		description: "ListByOptions with a All Partition should select all items that meet all other conditions in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns:                "",
		expectedList:      makeList(t, foo, bar, baz, toto, lodgePole),
		expectedTotal:     5,
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
		ns:                "",
		expectedList:      makeList(t, foo, bar, baz, toto, lodgePole),
		expectedTotal:     5,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Names Partition should select only items where metadata.name equals an items in Names and all other conditions are met in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Names: sets.New("obj1", "obj2"),
			},
		},
		ns:                "",
		expectedList:      makeList(t, foo, bar),
		expectedTotal:     2,
		expectedContToken: "",
		expectedErr:       nil,
	})
	t.Parallel()

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"metadata", "somefield"},
				{"status", "someotherfield"},
				{"metadata", "unknown"},
				{"metadata", "sortfield"},
			}
			fields = append(fields, test.extraIndexedFields...)

			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, opts)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			list, total, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}

			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedTotal, total)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func TestUserDefinedExtractFunction(t *testing.T) {
	makeObj := func(name string, barSeparatedHosts string) map[string]any {
		h1 := map[string]any{
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"rules": map[string]any{
					"host": barSeparatedHosts,
				},
			},
		}
		return h1
	}
	ctx := context.Background()

	type testCase struct {
		description string
		listOptions sqltypes.ListOptions
		partitions  []partition.Partition
		ns          string

		items []*unstructured.Unstructured

		extraIndexedFields [][]string
		expectedList       *unstructured.UnstructuredList
		expectedTotal      int
		expectedContToken  string
		expectedErr        error
	}

	obj01 := makeObj("obj01", "dogs|horses|humans")
	obj02 := makeObj("obj02", "dogs|cats|fish")
	obj03 := makeObj("obj03", "camels|clowns|zebras")
	obj04 := makeObj("obj04", "aardvarks|harps|zyphyrs")
	allObjects := []map[string]any{obj01, obj02, obj03, obj04}
	makeList := func(t *testing.T, objs ...map[string]any) *unstructured.UnstructuredList {
		t.Helper()

		if len(objs) == 0 {
			return &unstructured.UnstructuredList{Object: map[string]any{"items": []any{}}, Items: []unstructured.Unstructured{}}
		}

		var items []any
		for _, obj := range objs {
			items = append(items, obj)
		}

		list := &unstructured.Unstructured{
			Object: map[string]any{
				"items": items,
			},
		}

		itemList, err := list.ToList()
		require.NoError(t, err)

		return itemList
	}
	itemList := makeList(t, allObjects...)

	var tests []testCase
	tests = append(tests, testCase{
		description: "find dogs in the first substring",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"spec", "rules", "0", "host"},
						Matches: []string{"dogs"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj01, obj02),
		expectedTotal:     2,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "extractBarredValue on item 0 should work",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "rules", "0", "host"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj04, obj03, obj01, obj02),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "extractBarredValue on item 1 should work",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "rules", "1", "host"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02, obj03, obj04, obj01),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "extractBarredValue on item 2 should work",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "rules", "2", "host"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02, obj01, obj03, obj04),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "extractBarredValue on item 3 should fall back to default sorting",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "rules", "3", "host"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, allObjects...),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "extractBarredValue on item -2 should result in a compile error",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "rules", "-2", "host"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:  []partition.Partition{{All: true}},
		ns:          "",
		expectedErr: errors.New("column is invalid [spec.rules.-2.host]: supplied column is invalid"),
	})
	t.Parallel()

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"spec", "rules", "host"},
			}
			fields = append(fields, test.extraIndexedFields...)

			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, opts)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			list, total, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedTotal, total)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func TestConstructQuery(t *testing.T) {
	type testCase struct {
		description           string
		listOptions           sqltypes.ListOptions
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
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.In,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOT-IN statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.NotIn,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    sqltypes.Exists,
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
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    sqltypes.NotExists,
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
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualFull"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.Eq,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEqualFull", "somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements, match partial",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.Eq,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualFull"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.NotEq,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNotEqualFull", "labelNotEqualFull", "somevalue"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements, match partial",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      sqltypes.NotEq,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNotEqualPartial", "labelNotEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles multiple != statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual1"},
						Matches: []string{"value1"},
						Op:      sqltypes.NotEq,
						Partial: false,
					},
				},
			},
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual2"},
						Matches: []string{"value2"},
						Op:      sqltypes.NotEq,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"notEqual1", "notEqual1", "value1", "notEqual2", "notEqual2", "value2"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles IN statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      sqltypes.In,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTIN statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      sqltypes.NotIn,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNOTIN", "labelNOTIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelEXISTS"},
						Matches: []string{},
						Op:      sqltypes.Exists,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEXISTS"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTEXISTS statements for label statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTEXISTS"},
						Matches: []string{},
						Op:      sqltypes.NotExists,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNOTEXISTS"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles LessThan statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"5"},
						Op:      sqltypes.Lt,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"numericThing", float64(5)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles GreaterThan statements",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"35"},
						Op:      sqltypes.Gt,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"numericThing", float64(35)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: uses the extractBarredValue custom function for penultimate indexer",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"spec", "containers", "3", "image"},
						Matches: []string{"nginx-happy"},
						Op:      sqltypes.Eq,
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
    (extractBarredValue(f."spec.containers.image", "3") = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"nginx-happy"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: uses the extractBarredValue custom function for penultimate indexer when sorting",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "containers", "16", "image"},
						Order:  sqltypes.ASC,
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
  ORDER BY extractBarredValue(f."spec.containers.image", "16") ASC`,
		expectedStmtArgs: []any{},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: uses the extractBarredValue custom function for penultimate indexer when both filtering and sorting",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"spec", "containers", "3", "image"},
							Matches: []string{"nginx-happy"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "containers", "16", "image"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (extractBarredValue(f."spec.containers.image", "3") = ?) AND
    (FALSE)
  ORDER BY extractBarredValue(f."spec.containers.image", "16") ASC`,
		expectedStmtArgs: []any{"nginx-happy"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters with a positive label test and a negative non-label test still outer-join",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "junta"},
						Matches: []string{"esther"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"golgi"},
						Op:      sqltypes.NotEq,
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
    ((lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') OR (f."metadata.queryField1" NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"junta", "%esther%", "%golgi%"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters and or-filters with a positive label test and a negative non-label test still outer-join and have correct AND/ORs",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "nectar"},
						Matches: []string{"stash"},
						Op:      sqltypes.Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"landlady"},
						Op:      sqltypes.NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "lawn"},
						Matches: []string{"reba", "coil"},
						Op:      sqltypes.In,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      sqltypes.Gt,
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
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"nectar", "%stash%", "landlady", "lawn", "reba", "coil", float64(2)},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements, match partial, sort on metadata.queryField1",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"metadata", "labels", "labelEqualPartial"},
							Matches: []string{"somevalue"},
							Op:      sqltypes.Eq,
							Partial: true,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "queryField1"},
						Order:  sqltypes.ASC,
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
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "this"},
						Order:  sqltypes.ASC,
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
  ORDER BY (CASE lt1.label WHEN ? THEN lt1.value ELSE NULL END) ASC NULLS LAST`,
		expectedStmtArgs: []any{"this", "this"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: sort and query on both labels and non-labels without overlap",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"metadata", "queryField1"},
							Matches: []string{"toys"},
							Op:      sqltypes.Eq,
						},
						{
							Field:   []string{"metadata", "labels", "jamb"},
							Matches: []string{"juice"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "this"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"status", "queryField2"},
						Order:  sqltypes.DESC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  LEFT OUTER JOIN "something_labels" lt3 ON o.key = lt3.key
  WHERE
    ((f."metadata.queryField1" = ?) OR (lt2.label = ? AND lt2.value = ?) OR (lt3.label = ?)) AND
    (FALSE)
  ORDER BY (CASE lt3.label WHEN ? THEN lt3.value ELSE NULL END) ASC NULLS LAST, f."status.queryField2" DESC`,
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
				indexedFields: []string{"metadata.queryField1", "status.queryField2", "spec.containers.image"},
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
		joinTableIndexByLabelName: map[string]int{"testBSL1": 3},
		direction:                 true,
		expectedStmt:              `(CASE lt3.label WHEN ? THEN lt3.value ELSE NULL END) ASC NULLS LAST`,
		expectedParam:             "testBSL1",
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit descending",
		labelName:                 "testBSL2",
		joinTableIndexByLabelName: map[string]int{"testBSL2": 4},
		direction:                 false,
		expectedStmt:              `(CASE lt4.label WHEN ? THEN lt4.value ELSE NULL END) DESC NULLS FIRST`,
		expectedParam:             "testBSL2",
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			stmt, param, err := buildSortLabelsClause(test.labelName, test.joinTableIndexByLabelName, test.direction)
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

func TestGetField(t *testing.T) {
	tests := []struct {
		name           string
		obj            any
		field          string
		expectedResult any
		expectedErr    bool
	}{
		{
			name: "simple",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"foo": "bar",
				},
			},
			field:          "foo",
			expectedResult: "bar",
		},
		{
			name: "nested",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"foo": map[string]any{
						"bar": "baz",
					},
				},
			},
			field:          "foo.bar",
			expectedResult: "baz",
		},
		{
			name: "array",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"theList": []any{
						"foo", "bar", "baz",
					},
				},
			},
			field:          "theList[1]",
			expectedResult: "bar",
		},
		{
			name: "array of object",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"theList": []any{
						map[string]any{
							"name": "foo",
						},
						map[string]any{
							"name": "bar",
						},
						map[string]any{
							"name": "baz",
						},
					},
				},
			},
			field:          "theList.name",
			expectedResult: []string{"foo", "bar", "baz"},
		},
		{
			name: "annotation",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"annotations": map[string]any{
						"with.dot.in.it/and-slash": "foo",
					},
				},
			},
			field:          "annotations[with.dot.in.it/and-slash]",
			expectedResult: "foo",
		},
		{
			name: "field not found",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"spec": map[string]any{
						"rules": []any{
							map[string]any{},
							map[string]any{
								"host": "example.com",
							},
						},
					},
				},
			},
			field:          "spec.rules.host",
			expectedResult: []string{"", "example.com"},
		},
		{
			name: "array index invalid",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"theList": []any{
						"foo", "bar", "baz",
					},
				},
			},
			field:       "theList[a]",
			expectedErr: true,
		},
		{
			name: "array index out of bound",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"theList": []any{
						"foo", "bar", "baz",
					},
				},
			},
			field:       "theList[3]",
			expectedErr: true,
		},
		{
			name: "invalid array",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"spec": map[string]any{
						"rules": []any{
							1,
						},
					},
				},
			},
			field:       "spec.rules.host",
			expectedErr: true,
		},
		{
			name: "invalid array nested",
			obj: &unstructured.Unstructured{
				Object: map[string]any{
					"spec": map[string]any{
						"rules": []any{
							map[string]any{
								"host": 1,
							},
						},
					},
				},
			},
			field:       "spec.rules.host",
			expectedErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := getField(test.obj, test.field)
			if test.expectedErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedResult, result)
		})
	}
}

func TestWatchMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	opts := ListOptionIndexerOptions{
		Fields: [][]string{
			{"metadata", "somefield"},
			{"spec", "replicas"},
			{"spec", "minReplicas"},
		},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, opts)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	startWatcher := func(ctx context.Context) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}
	watcher1, errCh1 := startWatcher(ctx)
	events := receiveEvents(watcher1)
	assert.Len(t, events, 0)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"spec": map[string]any{
				"replicas":    int64(1),
				"minReplicas": float64(1.0),
			},
		},
	}
	foo.SetResourceVersion("100")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("120")
	foo2.SetLabels(map[string]string{
		"hello": "world",
	})
	foo3 := foo.DeepCopy()
	foo3.SetResourceVersion("140")
	foo4 := foo2.DeepCopy()
	foo4.SetResourceVersion("160")

	err = loi.Add(foo)
	assert.NoError(t, err)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{{Type: watch.Added, Object: foo}}, events)

	ctx2, cancel2 := context.WithCancel(context.Background())
	watcher2, errCh2 := startWatcher(ctx2)

	err = loi.Update(foo2)
	assert.NoError(t, err)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{{Type: watch.Modified, Object: foo2}}, events)

	events = receiveEvents(watcher2)
	assert.Equal(t, []watch.Event{{Type: watch.Modified, Object: foo2}}, events)

	watcher3, errCh3 := startWatcher(ctx)

	cancel2()
	err = waitStopWatcher(errCh2)
	assert.NoError(t, err)

	err = loi.Delete(foo2)
	assert.NoError(t, err)
	err = loi.Add(foo3)
	assert.NoError(t, err)
	err = loi.Update(foo4)
	assert.NoError(t, err)

	events = receiveEvents(watcher3)
	assert.Equal(t, []watch.Event{
		{Type: watch.Deleted, Object: foo2},
		{Type: watch.Added, Object: foo3},
		{Type: watch.Modified, Object: foo4},
	}, events)

	// Verify cancelled watcher don't receive anything anymore
	events = receiveEvents(watcher2)
	assert.Len(t, events, 0)

	events = receiveEvents(watcher1)
	assert.Equal(t, []watch.Event{
		{Type: watch.Deleted, Object: foo2},
		{Type: watch.Added, Object: foo3},
		{Type: watch.Modified, Object: foo4},
	}, events)

	cancel()
	err = waitStopWatcher(errCh1)
	assert.NoError(t, err)

	err = waitStopWatcher(errCh3)
	assert.NoError(t, err)
}

func TestWatchFilter(t *testing.T) {
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, filter WatchFilter) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{Filter: filter}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	foo := &unstructured.Unstructured{}
	foo.SetName("foo")
	foo.SetNamespace("foo")
	foo.SetLabels(map[string]string{
		"app": "foo",
	})

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetLabels(map[string]string{
		"app": "changed",
	})

	bar := &unstructured.Unstructured{}
	bar.SetName("bar")
	bar.SetNamespace("bar")
	bar.SetLabels(map[string]string{
		"app": "bar",
	})

	appSelector, err := labels.Parse("app=foo")
	assert.NoError(t, err)

	tests := []struct {
		name           string
		filter         WatchFilter
		setupStore     func(store cache.Store) error
		expectedEvents []watch.Event
	}{
		{
			name:   "namespace filter",
			filter: WatchFilter{Namespace: "foo"},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{{Type: watch.Added, Object: foo}},
		},
		{
			name:   "selector filter",
			filter: WatchFilter{Selector: appSelector},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				err = store.Update(fooUpdated)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: foo},
				{Type: watch.Modified, Object: fooUpdated},
			},
		},
		{
			name:   "id filter",
			filter: WatchFilter{ID: "foo"},
			setupStore: func(store cache.Store) error {
				err := store.Add(foo)
				if err != nil {
					return err
				}
				err = store.Add(bar)
				if err != nil {
					return err
				}
				err = store.Update(fooUpdated)
				if err != nil {
					return err
				}
				err = store.Update(foo)
				if err != nil {
					return err
				}
				return nil
			},
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: foo},
				{Type: watch.Modified, Object: fooUpdated},
				{Type: watch.Modified, Object: foo},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			opts := ListOptionIndexerOptions{
				Fields:       [][]string{{"metadata", "somefield"}},
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, opts)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			wCh, errCh := startWatcher(ctx, loi, WatchFilter{
				Namespace: "foo",
			})

			if test.setupStore != nil {
				err = test.setupStore(loi)
				assert.NoError(t, err)
			}

			events := receiveEvents(wCh)
			assert.Equal(t, test.expectedEvents, events)

			cancel()
			err = waitStopWatcher(errCh)
			assert.NoError(t, err)

		})
	}

}

func TestWatchResourceVersion(t *testing.T) {
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, rv string) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{ResourceVersion: rv}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	foo := &unstructured.Unstructured{}
	foo.SetResourceVersion("100")
	foo.SetName("foo")
	foo.SetNamespace("foo")
	foo.SetLabels(map[string]string{
		"app": "foo",
	})

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetResourceVersion("120")
	fooUpdated.SetLabels(map[string]string{
		"app": "changed",
	})

	bar := &unstructured.Unstructured{}
	bar.SetResourceVersion("150")
	bar.SetName("bar")
	bar.SetNamespace("bar")
	bar.SetLabels(map[string]string{
		"app": "bar",
	})

	barDeleted := bar.DeepCopy()
	barDeleted.SetResourceVersion("160")

	barNew := bar.DeepCopy()
	barNew.SetResourceVersion("170")

	parentCtx := context.Background()

	opts := ListOptionIndexerOptions{
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(parentCtx, opts)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	getRV := func(t *testing.T) string {
		t.Helper()
		list, _, _, err := loi.ListByOptions(parentCtx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
		assert.NoError(t, err)
		return list.GetResourceVersion()
	}

	err = loi.Add(foo)
	assert.NoError(t, err)
	rv1 := getRV(t)

	err = loi.Update(fooUpdated)
	assert.NoError(t, err)
	rv2 := getRV(t)

	err = loi.Add(bar)
	assert.NoError(t, err)
	rv3 := getRV(t)

	err = loi.Delete(barDeleted)
	assert.NoError(t, err)
	rv4 := getRV(t)

	err = loi.Add(barNew)
	assert.NoError(t, err)
	rv5 := getRV(t)

	tests := []struct {
		rv             string
		expectedEvents []watch.Event
		expectedErr    error
	}{
		{
			rv: "",
		},
		{
			rv: rv1,
			expectedEvents: []watch.Event{
				{Type: watch.Modified, Object: fooUpdated},
				{Type: watch.Added, Object: bar},
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv2,
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: bar},
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv3,
			expectedEvents: []watch.Event{
				{Type: watch.Deleted, Object: barDeleted},
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv: rv4,
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv:             rv5,
			expectedEvents: nil,
		},
		{
			rv:          "unknown",
			expectedErr: ErrTooOld,
		},
	}

	for _, test := range tests {
		t.Run(test.rv, func(t *testing.T) {
			ctx, cancel := context.WithCancel(parentCtx)
			watcherCh, errCh := startWatcher(ctx, loi, test.rv)
			gotEvents := receiveEvents(watcherCh)

			cancel()
			err := waitStopWatcher(errCh)
			if test.expectedErr != nil {
				assert.ErrorIs(t, err, ErrTooOld)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedEvents, gotEvents)
			}
		})
	}
}

func TestWatchGarbageCollection(t *testing.T) {
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, rv string) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{ResourceVersion: rv}, eventsCh)
			errCh <- watchErr
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	waitStopWatcher := func(errCh chan error) error {
		select {
		case <-time.After(time.Second * 5):
			return fmt.Errorf("not finished in time")
		case err := <-errCh:
			return err
		}
	}

	receiveEvents := func(eventsCh chan watch.Event) []watch.Event {
		timer := time.NewTimer(time.Millisecond * 50)
		var events []watch.Event
		for {
			select {
			case <-timer.C:
				return events
			case ev := <-eventsCh:
				events = append(events, ev)
			}
		}
	}

	foo := &unstructured.Unstructured{}
	foo.SetResourceVersion("100")
	foo.SetName("foo")

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetResourceVersion("120")

	bar := &unstructured.Unstructured{}
	bar.SetResourceVersion("150")
	bar.SetName("bar")

	barDeleted := bar.DeepCopy()
	barDeleted.SetResourceVersion("160")

	barNew := bar.DeepCopy()
	barNew.SetResourceVersion("170")

	parentCtx := context.Background()

	opts := ListOptionIndexerOptions{
		MaximumEventsCount: 2,
	}
	loi, dbPath, err := makeListOptionIndexer(parentCtx, opts)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	getRV := func(t *testing.T) string {
		t.Helper()
		list, _, _, err := loi.ListByOptions(parentCtx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
		assert.NoError(t, err)
		return list.GetResourceVersion()
	}

	err = loi.Add(foo)
	assert.NoError(t, err)
	rv1 := getRV(t)

	err = loi.Update(fooUpdated)
	assert.NoError(t, err)
	rv2 := getRV(t)

	err = loi.Add(bar)
	assert.NoError(t, err)
	rv3 := getRV(t)

	err = loi.Delete(barDeleted)
	assert.NoError(t, err)
	rv4 := getRV(t)

	for _, rv := range []string{rv1, rv2} {
		watcherCh, errCh := startWatcher(parentCtx, loi, rv)
		gotEvents := receiveEvents(watcherCh)
		err = waitStopWatcher(errCh)
		assert.Empty(t, gotEvents)
		assert.ErrorIs(t, err, ErrTooOld)
	}

	tests := []struct {
		rv             string
		expectedEvents []watch.Event
	}{
		{
			rv: rv3,
			expectedEvents: []watch.Event{
				{Type: watch.Deleted, Object: barDeleted},
			},
		},
		{
			rv:             rv4,
			expectedEvents: nil,
		},
	}
	for _, test := range tests {
		ctx, cancel := context.WithCancel(parentCtx)
		watcherCh, errCh := startWatcher(ctx, loi, test.rv)
		gotEvents := receiveEvents(watcherCh)
		cancel()
		err = waitStopWatcher(errCh)
		assert.Equal(t, test.expectedEvents, gotEvents)
		assert.NoError(t, err)
	}

	err = loi.Add(barNew)
	assert.NoError(t, err)
	rv5 := getRV(t)

	for _, rv := range []string{rv1, rv2, rv3} {
		watcherCh, errCh := startWatcher(parentCtx, loi, rv)
		gotEvents := receiveEvents(watcherCh)
		err = waitStopWatcher(errCh)
		assert.Empty(t, gotEvents)
		assert.ErrorIs(t, err, ErrTooOld)
	}

	tests = []struct {
		rv             string
		expectedEvents []watch.Event
	}{
		{
			rv: rv4,
			expectedEvents: []watch.Event{
				{Type: watch.Added, Object: barNew},
			},
		},
		{
			rv:             rv5,
			expectedEvents: nil,
		},
	}
	for _, test := range tests {
		ctx, cancel := context.WithCancel(parentCtx)
		watcherCh, errCh := startWatcher(ctx, loi, test.rv)
		gotEvents := receiveEvents(watcherCh)
		cancel()
		err = waitStopWatcher(errCh)
		assert.Equal(t, test.expectedEvents, gotEvents)
		assert.NoError(t, err)
	}
}

func TestNonNumberResourceVersion(t *testing.T) {
	ctx := context.Background()

	opts := ListOptionIndexerOptions{
		Fields:       [][]string{{"metadata", "somefield"}},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, opts)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
		},
	}
	foo.SetResourceVersion("a")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("b")
	foo2.SetLabels(map[string]string{
		"hello": "world",
	})
	bar := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "bar",
			},
		},
	}
	bar.SetResourceVersion("c")
	err = loi.Add(foo)
	assert.NoError(t, err)
	err = loi.Update(foo2)
	assert.NoError(t, err)
	err = loi.Add(bar)
	assert.NoError(t, err)

	expectedUnstructured := &unstructured.Unstructured{
		Object: map[string]any{
			"items": []any{bar.Object, foo2.Object},
		},
	}
	expectedList, err := expectedUnstructured.ToList()
	require.NoError(t, err)

	list, _, _, err := loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	assert.NoError(t, err)
	assert.Equal(t, expectedList.Items, list.Items)
}
