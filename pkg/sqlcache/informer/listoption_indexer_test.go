/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"context"
	//	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/rancher/steve/pkg/sqlcache/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

var emptyNamespaceList = &unstructured.UnstructuredList{Object: map[string]any{"items": []any{}}, Items: []unstructured.Unstructured{}}

var standardJoinParts = []joinPart{
		{
			joinCommand:    "LEFT OUTER JOIN",
			tableName:      "something_labels",
			tableNameAlias: "lt1",
			onPrefix:       "f1",
			onField:        "key",
			otherPrefix:    "lt1",
			otherField:     "key",
		},
	}

func makeListOptionIndexer(ctx context.Context, gvk schema.GroupVersionKind, opts ListOptionIndexerOptions, shouldEncrypt bool, nsList *unstructured.UnstructuredList) (*ListOptionIndexer, string, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, "", err
	}

	db, dbPath, err := db.NewClient(ctx, nil, m, m, true)
	if err != nil {
		return nil, "", err
	}
	// First create a namespace table so the projectsornamespaces query succeeds
	nsGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Namespace",
	}
	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(nsGVK)
	name := informerNameFromGVK(nsGVK)
	s, err := store.NewStore(ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, nsGVK, name, nil, nil)
	if err != nil {
		return nil, "", err
	}
	ns_opts := ListOptionIndexerOptions{
		Fields:       [][]string{},
		IsNamespaced: false,
	}
	listOptionIndexer, err := NewListOptionIndexer(ctx, s, ns_opts)
	if err != nil {
		return nil, "", err
	}
	if nsList != nil {
		for _, item := range nsList.Items {
			err = listOptionIndexer.Add(&item)
			if err != nil {
				return nil, "", err
			}
		}
	}

	example = &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)
	name = informerNameFromGVK(gvk)

	s, err = store.NewStore(ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, gvk, name, nil, nil)
	if err != nil {
		return nil, "", err
	}
	if opts.IsNamespaced {
		// Can't use slices.Compare because []string doesn't implement comparable
		idEntry := []string{"id"}
		if opts.Fields == nil {
			opts.Fields = [][]string{idEntry}
		} else {
			opts.Fields = append(opts.Fields, idEntry)
		}
	}

	listOptionIndexer, err = NewListOptionIndexer(ctx, s, opts)
	if err != nil {
		return nil, "", err
	}

	go listOptionIndexer.RunGC(ctx)

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
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		// create events table
		txClient.EXPECT().Exec(fmt.Sprintf(dropEventsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
		// create field table
		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" INT`)).Return(nil, nil)
		// create field table indexes
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		// create labels table
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
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
			TypeGuidance: map[string]string{"something": "INT"},
			IsNamespaced: true,
		}
		loi, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.Nil(t, err)
		assert.NotNil(t, loi)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTxClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))

		opts := ListOptionIndexerOptions{
			Fields: fields,
		}
		_, err := NewListOptionIndexer(context.Background(), store, opts)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Exec() when creating fields table, should return an error", test: func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropEventsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
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
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropEventsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
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
		ctrl := gomock.NewController(t)
		txClient := NewMockTxClient(ctrl)
		store := NewMockStore(ctrl)
		stmt := NewMockStmt(ctrl)
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
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

		store.EXPECT().RegisterAfterAdd(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterUpdate(gomock.Any()).Times(3)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(1)
		store.EXPECT().RegisterAfterDeleteAll(gomock.Any()).Times(2)
		store.EXPECT().RegisterBeforeDropAll(gomock.Any()).AnyTimes()

		txClient.EXPECT().Exec(fmt.Sprintf(dropEventsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createEventsTableFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropFieldsFmt, id)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(dropLabelsStmtFmt, id)).Return(nil, nil)
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

func makeList(t *testing.T, objs ...map[string]any) *unstructured.UnstructuredList {
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

func TestNewListOptionIndexerEasy(t *testing.T) {
	ctx := context.Background()
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	type testCase struct {
		description string
		listOptions sqltypes.ListOptions
		partitions  []partition.Partition
		ns          string

		extraIndexedFields [][]string
		expectedList       *unstructured.UnstructuredList
		expectedTotal      int
		expectedContToken  string
		expectedErr        error
		latestRV           string
	}
	obj01_no_labels := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj01_no_labels",
			"namespace": "ns-a",
			"somefield": "foo",
			"sortfield": "400",
		},
		"status": map[string]any{
			"podIP": "99.4.5.6",
		},
	}
	obj02_milk_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02_milk_saddles",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "100",
			"labels": map[string]any{
				"cows":   "milk",
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP": "102.1.2.3",
		},
	}
	obj02a_beef_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02a_beef_saddles",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "110",
			"labels": map[string]any{
				"cows":   "beef",
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP": "102.99.2.3",
		},
	}
	obj02b_milk_shoes := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02b_milk_shoes",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "105",
			"labels": map[string]any{
				"cows":   "milk",
				"horses": "shoes",
			},
		},
		"status": map[string]any{
			"podIP": "102.103.2.3",
		},
	}
	obj03_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj03_saddles",
			"namespace": "ns-a",
			"somefield": "baz",
			"sortfield": "200",
			"labels": map[string]any{
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP":          "77.4.5.6",
			"someotherfield": "helloworld",
		},
	}
	obj03a_shoes := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj03a_shoes",
			"namespace": "ns-a",
			"somefield": "baz",
			"sortfield": "210",
			"labels": map[string]any{
				"horses": "shoes",
			},
		},
		"status": map[string]any{
			"podIP":          "102.99.99.1",
			"someotherfield": "helloworld",
		},
	}
	obj04_milk := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj04_milk",
			"namespace": "ns-a",
			"somefield": "toto",
			"sortfield": "200",
			"labels": map[string]any{
				"cows": "milk",
			},
		},
		"status": map[string]any{
			"podIP": "102.99.105.1",
		},
	}
	obj05__guard_lodgepole := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj05__guard_lodgepole",
			"namespace": "ns-b",
			"unknown":   "hi",
			"labels": map[string]any{
				"guard.cattle.io": "lodgepole",
			},
		},
		"status": map[string]any{
			"podIP": "203.1.2.3",
		},
	}
	allObjects := []map[string]any{
		obj01_no_labels,
		obj02_milk_saddles,
		obj02a_beef_saddles,
		obj02b_milk_shoes,
		obj03_saddles,
		obj03a_shoes,
		obj04_milk,
		obj05__guard_lodgepole,
	}
	ns_a := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": "ns-a",
			"labels": map[string]any{
				"guard.cattle.io": "ponderosa",
			},
		},
	}
	ns_b := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": "ns-b",
			"labels": map[string]any{
				"field.cattle.io/projectId": "ns-b",
			},
		},
	}

	itemList := makeList(t, allObjects...)
	namespaceList := makeList(t, ns_a, ns_b)

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
		description: "ListByOptions with single object matching many labels with AND",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"milk"},
						Op:      sqltypes.Eq,
					},
				},
			},
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "horses"},
						Matches: []string{"shoes"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02b_milk_shoes),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with many objects matching many labels with OR",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"milk"},
						Op:      sqltypes.Eq,
					},
					{
						Field:   []string{"metadata", "labels", "horses"},
						Matches: []string{"shoes"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02_milk_saddles, obj02b_milk_shoes, obj03a_shoes, obj04_milk),
		expectedTotal:     4,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter should select where that filter is true",
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
		expectedList:      makeList(t, obj01_no_labels),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Op set to NotEq should select where that filter is not true",
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
		expectedList:      makeList(t, obj02_milk_saddles, obj02a_beef_saddles, obj02b_milk_shoes, obj03_saddles, obj03a_shoes, obj04_milk, obj05__guard_lodgepole),
		expectedTotal:     7,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Partial set to true should select where that partial match on that filter's value is true",
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
		expectedList:      makeList(t, obj01_no_labels, obj04_milk),
		expectedTotal:     2,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with multiple filters should select where any of those filters are true",
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
		expectedList:      makeList(t, obj01_no_labels, obj02_milk_saddles, obj02a_beef_saddles, obj02b_milk_shoes, obj03_saddles, obj03a_shoes, obj05__guard_lodgepole),
		expectedTotal:     7,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with multiple OrFilters set should select where all OrFilters contain one filter that is true",
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
		expectedList:      makeList(t, obj04_milk),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with labels filter should select the label",
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
		expectedList:      makeList(t, obj05__guard_lodgepole),
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
		expectedList:      makeList(t, obj02_milk_saddles),
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
		expectedList:      makeList(t, obj04_milk),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with only one Sort.Field set should sort on that field only, in ascending order",
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
		expectedList:      makeList(t, obj05__guard_lodgepole, obj02_milk_saddles, obj02a_beef_saddles, obj02b_milk_shoes, obj03_saddles, obj03a_shoes, obj01_no_labels, obj04_milk),
		expectedTotal:     len(allObjects),
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
		expectedList:      makeList(t, obj04_milk, obj01_no_labels, obj03a_shoes, obj03_saddles, obj02b_milk_shoes, obj02a_beef_saddles, obj02_milk_saddles, obj05__guard_lodgepole),
		expectedTotal:     len(allObjects),
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
		expectedList:      makeList(t, obj05__guard_lodgepole, obj04_milk, obj03a_shoes, obj03_saddles, obj02b_milk_shoes, obj02a_beef_saddles, obj02_milk_saddles, obj01_no_labels),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in ascending order first and then sort on the second field in ascending order",
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
		expectedList:      makeList(t, obj05__guard_lodgepole, obj02_milk_saddles, obj02b_milk_shoes, obj02a_beef_saddles, obj03_saddles, obj04_milk, obj03a_shoes, obj01_no_labels),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in descending order first and then sort on the second field in ascending order",
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
		expectedList:      makeList(t, obj01_no_labels, obj03a_shoes, obj03_saddles, obj04_milk, obj02a_beef_saddles, obj02b_milk_shoes, obj02_milk_saddles, obj05__guard_lodgepole),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two existing labels, with no label filters, should sort correctly",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "horses"},
					},
					{
						Fields: []string{"metadata", "labels", "cows"},
					},
				},
			},
		},
		partitions: []partition.Partition{{All: true}},
		expectedList: makeList(t, obj02a_beef_saddles, obj02_milk_saddles, obj03_saddles,
			obj02b_milk_shoes, obj03a_shoes, obj04_milk, obj01_no_labels, obj05__guard_lodgepole),
		expectedTotal: len(allObjects),
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.PageSize set should set limit to PageSize",
		listOptions: sqltypes.ListOptions{
			Pagination: sqltypes.Pagination{
				PageSize: 3,
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj01_no_labels, obj02_milk_saddles, obj02a_beef_saddles),
		expectedTotal:     len(allObjects),
		expectedContToken: "3",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and no PageSize set should not filter anything",
		listOptions: sqltypes.ListOptions{
			Pagination: sqltypes.Pagination{
				Page: 2,
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
		description: "ListByOptions with a All Partition should select all items that meet all other conditions",
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns:                "",
		expectedList:      makeList(t, allObjects...),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Passthrough Partition should select all items that meet all other conditions",
		partitions: []partition.Partition{
			{
				Passthrough: true,
			},
		},
		ns:                "",
		expectedList:      makeList(t, allObjects...),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Names Partition should select only items where metadata.name equals an items in Names and all other conditions are met",
		partitions: []partition.Partition{
			{
				Names: sets.New("obj01_no_labels", "obj02_milk_saddles"),
			},
		},
		ns:                "",
		expectedList:      makeList(t, obj01_no_labels, obj02_milk_saddles),
		expectedTotal:     2,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "sort one unbound label descending",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "flip"},
						Order:  sqltypes.DESC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj01_no_labels, obj02_milk_saddles, obj02a_beef_saddles, obj02b_milk_shoes, obj03_saddles, obj03a_shoes, obj04_milk, obj05__guard_lodgepole),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two complex fields should sort on the cows-labels-field in ascending order first and then sort on the sortfield field in ascending order",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "cows"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "sortfield"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02a_beef_saddles, obj02_milk_saddles, obj02b_milk_shoes, obj04_milk, obj05__guard_lodgepole, obj03_saddles, obj03a_shoes, obj01_no_labels),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions sorting on two existing labels, with a filter on one, should sort correctly",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field: []string{"metadata", "labels", "cows"},
							Op:    sqltypes.Exists,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "labels", "cows"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "labels", "horses"},
						Order:  sqltypes.DESC,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj02a_beef_saddles, obj04_milk, obj02b_milk_shoes, obj02_milk_saddles),
		expectedTotal:     4,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a positive projectsornamespaces test should work",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "name"},
						Matches: []string{"ns-b"},
						Op:      sqltypes.In,
					},
					{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"ns-b"},
						Op:      sqltypes.In,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj05__guard_lodgepole),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a negative projectsornamespaces test should work",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					{
						Field:   []string{"metadata", "name"},
						Matches: []string{"ns-a"},
						Op:      sqltypes.NotIn,
					},
					{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"ns-a"},
						Op:      sqltypes.NotIn,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj05__guard_lodgepole),
		expectedTotal:     1,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with listOptions.Revision set equals to latestRV should work",
		listOptions: sqltypes.ListOptions{
			Revision: "9999",
		},
		latestRV:   "9999",
		partitions: []partition.Partition{},
		ns:         "",
		// setting resource version on unstructured list
		expectedList: func() *unstructured.UnstructuredList {
			list := makeList(t)
			list.SetResourceVersion("9999")
			return list
		}(),
		expectedTotal:     0,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with listOptions.Revision set to lower than latestRV should work",
		listOptions: sqltypes.ListOptions{
			Revision: "9999",
		},
		latestRV:   "10000",
		partitions: []partition.Partition{},
		ns:         "",
		// setting resource version on unstructured list
		expectedList: func() *unstructured.UnstructuredList {
			list := makeList(t)
			list.SetResourceVersion("10000")
			return list
		}(),
		expectedTotal:     0,
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with listOptions.Revision set to higher than latestRV, should return 'unknown revision'",
		listOptions: sqltypes.ListOptions{
			Revision: "10000",
		},
		latestRV:          "9999",
		partitions:        []partition.Partition{},
		ns:                "",
		expectedList:      &unstructured.UnstructuredList{},
		expectedTotal:     0,
		expectedContToken: "",
		expectedErr:       ErrUnknownRevision,
	})

	tests = append(tests, testCase{
		description: "ListByOptions: sorting on ip sorts on the ip octets",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields:   []string{"status", "podIP"},
						Order:    sqltypes.ASC,
						SortAsIP: true,
					},
				},
			},
		},
		partitions:   []partition.Partition{{All: true}},
		ns:           "",
		expectedList: makeList(t, obj03_saddles, obj01_no_labels, obj02_milk_saddles, obj02a_beef_saddles, obj03a_shoes, obj04_milk, obj02b_milk_shoes, obj05__guard_lodgepole),

		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	//tests = append(tests, testCase{
	//	description: "ListByOptions with a Namespace Partition should select only items where metadata.namespace is equal to Namespace and all other conditions are met",
	//	partitions: []partition.Partition{
	//		{
	//			Namespace: "ns-b",
	//		},
	//	},
	//	// XXX: Why do I need to specify the namespace here too?
	//	ns:                "ns-b",
	//	expectedList:      makeList(t, obj05__guard_lodgepole),
	//	expectedTotal:     1,
	//	expectedContToken: "",
	//	expectedErr:       nil,
	//})

	t.Parallel()

	// First curl the namespaces to load up the namespace database tables.

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"metadata", "somefield"},
				{"status", "someotherfield"},
				{"status", "podIP"},
				{"metadata", "unknown"},
				{"metadata", "sortfield"},
			}
			fields = append(fields, test.extraIndexedFields...)

			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, namespaceList)
			defer cleanTempFiles(dbPath)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}
			loi.latestRV = test.latestRV
			list, total, summary, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.Nil(t, err)

			assert.Equal(t, test.expectedTotal, total)
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedContToken, contToken)
			assert.Nil(t, summary)
		})
	}
}

func TestNewListOptionIndexerSummaryInfo(t *testing.T) {
	ctx := context.Background()
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	type testCase struct {
		description string
		listOptions sqltypes.ListOptions
		partitions  []partition.Partition
		ns          string

		extraIndexedFields [][]string
		expectedList       *unstructured.UnstructuredList
		expectedTotal      int
		expectedSummary    *types.APISummary
		expectedContToken  string
		expectedErr        error
		latestRV           string
	}
	obj01_no_labels := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj01_no_labels",
			"namespace": "ns-a",
			"somefield": "foo",
			"sortfield": "400",
		},
		"status": map[string]any{
			"podIP": "99.4.5.6",
		},
	}
	obj02_milk_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02_milk_saddles",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "100",
			"labels": map[string]any{
				"cows":   "milk",
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP": "102.1.2.3",
		},
	}
	obj02a_beef_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02a_beef_saddles",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "110",
			"labels": map[string]any{
				"cows":   "beef",
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP": "102.99.2.3",
		},
	}
	obj02b_milk_shoes := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj02b_milk_shoes",
			"namespace": "ns-a",
			"somefield": "bar",
			"sortfield": "105",
			"labels": map[string]any{
				"cows":   "milk",
				"horses": "shoes",
			},
		},
		"status": map[string]any{
			"podIP": "102.103.2.3",
		},
	}
	obj03_saddles := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj03_saddles",
			"namespace": "ns-a",
			"somefield": "baz",
			"sortfield": "200",
			"labels": map[string]any{
				"horses": "saddles",
			},
		},
		"status": map[string]any{
			"podIP":          "77.4.5.6",
			"someotherfield": "helloworld",
		},
	}
	obj03a_shoes := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj03a_shoes",
			"namespace": "ns-a",
			"somefield": "baz",
			"sortfield": "210",
			"labels": map[string]any{
				"horses": "shoes",
			},
		},
		"status": map[string]any{
			"podIP":          "102.99.99.1",
			"someotherfield": "helloworld",
		},
	}
	obj04_milk := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj04_milk",
			"namespace": "ns-a",
			"somefield": "toto",
			"sortfield": "200",
			"labels": map[string]any{
				"cows": "milk",
			},
		},
		"status": map[string]any{
			"podIP": "102.99.105.1",
		},
	}
	obj05__guard_lodgepole := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":      "obj05__guard_lodgepole",
			"namespace": "ns-b",
			"unknown":   "hi",
			"labels": map[string]any{
				"guard.cattle.io": "lodgepole",
			},
		},
		"status": map[string]any{
			"podIP": "203.1.2.3",
		},
	}
	allObjects := []map[string]any{
		obj01_no_labels,
		obj02_milk_saddles,
		obj02a_beef_saddles,
		obj02b_milk_shoes,
		obj03_saddles,
		obj03a_shoes,
		obj04_milk,
		obj05__guard_lodgepole,
	}
	ns_a := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": "ns-a",
			"labels": map[string]any{
				"guard.cattle.io": "ponderosa",
			},
		},
	}
	ns_b := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": "ns-b",
			"labels": map[string]any{
				"field.cattle.io/projectId": "ns-b",
			},
		},
	}

	itemList := makeList(t, allObjects...)
	namespaceList := makeList(t, ns_a, ns_b)
	defaultPartitions := []partition.Partition{{All: true}}

	var tests []testCase
	var deferredTests []testCase
	tests = append(tests, testCase{
		description: "ListByOptions() with a simple summary filter should return the summary",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
			SummaryFieldList: [][]string{{"metadata", "somefield"}},
		},
		partitions:    defaultPartitions,
		ns:            "",
		expectedList:  itemList,
		expectedTotal: len(allObjects),
		expectedSummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				{
					Property: "metadata.somefield",
					Counts: map[string]int{
						"bar":  3,
						"baz":  2,
						"foo":  1,
						"toto": 1,
					},
				},
			},
		},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with a simple summary filter for labels should return the summary",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
			SummaryFieldList: [][]string{{"metadata", "labels", "horses"}},
		},
		partitions:    defaultPartitions,
		ns:            "",
		expectedList:  itemList,
		expectedTotal: len(allObjects),
		expectedSummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				{
					Property: "metadata.labels.horses",
					Counts: map[string]int{
						"saddles": 3,
						"shoes":   2,
					},
				},
			},
		},
		expectedContToken: "",
		expectedErr:       nil,
	})
	deferredTests = append(deferredTests, testCase{
		description: "ListByOptions() should return summary for multiple fields",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
			SummaryFieldList: [][]string{{"metadata", "labels", "horses"}, {"metadata", "somefield"}, {"status", "someotherfield"}},
		},
		partitions:    defaultPartitions,
		ns:            "",
		expectedList:  itemList,
		expectedTotal: len(allObjects),
		expectedSummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				types.SummaryEntry{
					Property: "metadata.labels.horses",
					Counts: map[string]int{
						"saddles": 3,
						"shoes":   2,
					},
				},
				types.SummaryEntry{
					Property: "metadata.somefield",
					Counts: map[string]int{
						"bar":  3,
						"baz":  2,
						"foo":  1,
						"toto": 1,
					},
				},
				types.SummaryEntry{
					Property: "status.someotherfield",
					Counts: map[string]int{
						"helloworld": 2,
					},
				},
			},
		},
		expectedContToken: "",
		expectedErr:       nil,
	})
	deferredTests = append(deferredTests, testCase{
		description: "ListByOptions with options and summary should return a filtered summary",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field: []string{"metadata", "labels", "cows"},
							Op:    sqltypes.Exists,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
			SummaryFieldList: [][]string{{"metadata", "somefield"}, {"metadata", "labels", "cows"}, {"metadata", "labels", "horses"}, {"status", "someotherfield"}},
		},
		partitions:    defaultPartitions,
		ns:            "",
		expectedList:  makeList(t, obj02_milk_saddles, obj02a_beef_saddles, obj02b_milk_shoes, obj04_milk),
		expectedTotal: 4,
		expectedSummary: &types.APISummary{
			SummaryItems: []types.SummaryEntry{
				{
					Property: "metadata.labels.horses",
					Counts: map[string]int{
						"saddles": 2,
						"shoes":   1,
					},
				},
				{
					Property: "metadata.somefield",
					Counts: map[string]int{
						"bar":  3,
						"toto": 1,
					},
				},
				{
					Property: "status.someotherfield",
					Counts:   map[string]int{},
				},
			},
		},
		expectedContToken: "",
		expectedErr:       nil,
	})

	t.Parallel()
	assert.True(t, len(deferredTests) > 0)

	// First curl the namespaces to load up the namespace database tables.

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"metadata", "somefield"},
				{"status", "someotherfield"},
				{"status", "podIP"},
				{"metadata", "unknown"},
				{"metadata", "sortfield"},
			}
			fields = append(fields, test.extraIndexedFields...)

			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, namespaceList)
			defer cleanTempFiles(dbPath)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			loi.latestRV = test.latestRV
			list, total, summary, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectedTotal, total)
			wantNames := stringsFromULIst(test.expectedList)
			gotNames := stringsFromULIst(list)
			assert.Equal(t, wantNames, gotNames)
			if slices.Equal(wantNames, gotNames) {
				assert.Equal(t, test.expectedList, list)
			}
			assert.Equal(t, test.expectedSummary, summary)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func stringsFromULIst(ulist *unstructured.UnstructuredList) []string {
	names := make([]string, len(ulist.Items))
	for i, item := range ulist.Items {
		names[i] = item.GetName()
	}
	return names
}

func TestNewListOptionIndexerTypeGuidance(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
	obj01 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj01",
			"namespace":        "ns-a",
			"someNumericValue": "1",
			"favoriteFruit":    "14banana",
		},
	}
	obj05 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj05",
			"namespace":        "ns-a",
			"someNumericValue": "5",
			"favoriteFruit":    "130raspberries",
		},
	}
	obj11 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj11",
			"namespace":        "ns-a",
			"someNumericValue": "11",
			"favoriteFruit":    "9lime",
		},
	}
	// obj17: favoriteFruit is entered as a string
	// obj18: favoriteFruit is entered as an integer
	obj17 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj17",
			"namespace":        "ns-a",
			"someNumericValue": "17",
			"favoriteFruit":    "17",
		},
	}
	obj18 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj18",
			"namespace":        "ns-a",
			"someNumericValue": "18",
			"favoriteFruit":    int64(18),
		},
	}
	obj100 := map[string]any{
		"apiVersion": gvk.Version,
		"kind":       gvk.Kind,
		"metadata": map[string]any{
			"name":             "obj100",
			"namespace":        "ns-a",
			"someNumericValue": "100",
			"favoriteFruit":    "guava",
		},
	}
	// construct the source list so it isn't sorted either ASC or DESC
	allObjects := []map[string]any{
		obj18,
		obj01,
		obj11,
		obj05,
		obj17,
		obj100,
	}
	ns_a := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": "ns-a",
		},
	}

	itemList := makeList(t, allObjects...)
	namespaceList := makeList(t, ns_a)
	fields := [][]string{
		{"metadata", "someNumericValue"},
		{"metadata", "favoriteFruit"},
	}
	type testCase struct {
		description          string
		opts                 ListOptionIndexerOptions
		sortFields           []string
		expectedListAscObjs  []map[string]any
		expectedListDescObjs []map[string]any
	}

	var tests []testCase
	tests = append(tests,
		testCase{
			description: "TestNewListOptionIndexerTypeGuidance() with type-guidance INT on non-ints sorts as string",
			opts: ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
				TypeGuidance: map[string]string{
					"metadata.someNumericValue": "INT",
				},
			},
			sortFields:           []string{"metadata", "someNumericValue"},
			expectedListAscObjs:  []map[string]any{obj01, obj05, obj11, obj17, obj18, obj100},
			expectedListDescObjs: []map[string]any{obj100, obj18, obj17, obj11, obj05, obj01},
		})
	tests = append(tests,
		testCase{description: "TestNewListOptionIndexerTypeGuidance() without type-guidance sorts as strings",
			opts: ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			},
			sortFields:           []string{"metadata", "someNumericValue"},
			expectedListAscObjs:  []map[string]any{obj01, obj100, obj11, obj17, obj18, obj05},
			expectedListDescObjs: []map[string]any{obj05, obj18, obj17, obj11, obj100, obj01},
		})
	// This is what's going on with the sorting on a non-numeric value stored as an INT
	// Because some values are non-numeric, sorting is by ASCII
	//sqlite> select "metadata.name", "metadata.favoriteFruit" from _v1_ConfigMap_fields
	//        order by "metadata.favoriteFruit";
	//obj17|17
	//obj18|18
	//obj05|130raspberries
	//obj01|14banana
	//obj11|9lime
	//obj100|guava

	// Sorting is still by ascii -- adding 1 to the value in display shows that
	//sqlite> select "metadata.name", "metadata.favoriteFruit" + 1 from _v1_ConfigMap_fields
	//        order by "metadata.favoriteFruit";
	//obj17|18
	//obj18|19
	//obj05|131
	//obj01|15
	//obj11|10
	//obj100|1

	// This one forces numeric sorting
	//sqlite> select "metadata.name", "metadata.favoriteFruit" + 1 from _v1_ConfigMap_fields
	//        order by "metadata.favoriteFruit" + 1;
	//obj100|1
	//obj11|10
	//obj01|15
	//obj17|18
	//obj18|19
	//obj05|131
	tests = append(tests,
		testCase{description: "TestNewListOptionIndexerTypeGuidance() with type-guidance as int on a non-number sorts as string",
			opts: ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
				TypeGuidance: map[string]string{
					"metadata.favoriteFruit": "INT",
				},
			},
			sortFields:           []string{"metadata", "favoriteFruit"},
			expectedListAscObjs:  []map[string]any{obj17, obj18, obj05, obj01, obj11, obj100},
			expectedListDescObjs: []map[string]any{obj100, obj11, obj01, obj05, obj18, obj17},
		})
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			loi, dbPath, err := makeListOptionIndexer(t.Context(), gvk, test.opts, false, namespaceList)
			require.NoError(t, err)
			defer cleanTempFiles(dbPath)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				require.NoError(t, err)
			}

			expectedList := makeList(t, test.expectedListAscObjs...)
			list, total, summary, _, err := loi.ListByOptions(t.Context(), &sqltypes.ListOptions{
				SortList: sqltypes.SortList{
					SortDirectives: []sqltypes.Sort{
						{
							Fields: test.sortFields,
							Order:  sqltypes.ASC,
						},
					},
				},
			}, []partition.Partition{{All: true}}, "")
			require.NoError(t, err)
			assert.Equal(t, len(allObjects), total)
			assert.Equal(t, expectedList, list)
			require.Nil(t, summary)

			expectedList = makeList(t, test.expectedListDescObjs...)
			list, total, summary, _, err = loi.ListByOptions(t.Context(), &sqltypes.ListOptions{
				SortList: sqltypes.SortList{
					SortDirectives: []sqltypes.Sort{
						{
							Fields: test.sortFields,
							Order:  sqltypes.DESC,
						},
					},
				},
			}, []partition.Partition{{All: true}}, "")
			require.NoError(t, err)
			assert.Equal(t, len(allObjects), total)
			assert.Equal(t, expectedList, list)
			require.Nil(t, summary)
		})
	}
}

// Tests to verify we can sort on say pods on `spec.containers.image[i]`
func TestSortPodsOnArrayAccess(t *testing.T) {
	ctx := context.Background()
	podGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Pod",
	}
	make_pod_obj := func(name, image1, image2 string) map[string]any {
		containers := make(map[string]any)
		if image2 != "" {
			containers["image"] = image1 + "|" + image2
		} else {
			containers["image"] = image1
		}
		return map[string]any{
			"metadata": map[string]any{
				"name":      name,
				"namespace": "ns-a",
			},
			"spec": map[string]any{
				"containers": containers,
			},
		}
	}
	canoe_twix_ritter := make_pod_obj("canoe", "twix", "ritter")
	catamaran_flake_none := make_pod_obj("catamaran", "flake", "")
	clipper_butterfinger_payday := make_pod_obj("clipper", "butterfinger", "payday")
	ferry_twix_yorkie := make_pod_obj("ferry", "twix", "yorkie")
	hydrofoil_coffeecrisp_ritter := make_pod_obj("hydrofoil", "coffeecrisp", "ritter")
	kayak_butterfinger_rocher := make_pod_obj("kayak", "butterfinger", "rocher")
	raft_almondjoy_ragusa := make_pod_obj("raft", "almondjoy", "ragusa")
	schooner_twix_mounds := make_pod_obj("schooner", "twix", "mounds")
	sloop_snickers_toblerone := make_pod_obj("sloop", "snickers", "toblerone")
	trawler_butterfinger_aero := make_pod_obj("trawler", "butterfinger", "aero")

	allObjects := []map[string]any{
		canoe_twix_ritter,
		catamaran_flake_none,
		clipper_butterfinger_payday,
		ferry_twix_yorkie,
		hydrofoil_coffeecrisp_ritter,
		kayak_butterfinger_rocher,
		raft_almondjoy_ragusa,
		schooner_twix_mounds,
		sloop_snickers_toblerone,
		trawler_butterfinger_aero,
	}
	ns_a := map[string]any{
		"metadata": map[string]any{
			"name": "ns-a",
		},
	}
	ns_b := map[string]any{
		"metadata": map[string]any{
			"name": "ns-b",
		},
	}

	itemList := makeList(t, allObjects...)
	namespaceList := makeList(t, ns_a, ns_b)
	fields := [][]string{
		{"spec", "containers", "image"},
	}

	type testCase struct {
		description   string
		listOptions   sqltypes.ListOptions
		expectedList  *unstructured.UnstructuredList
		expectedTotal int
	}

	tests := make([]testCase, 0)
	tests = append(tests, testCase{
		description: "can select for a specific first image",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"spec", "containers", "image", "0"},
							Matches: []string{"butterfinger"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList:  makeList(t, clipper_butterfinger_payday, kayak_butterfinger_rocher, trawler_butterfinger_aero),
		expectedTotal: 3,
	})
	tests = append(tests, testCase{
		description: "can select for a specific second image",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"spec", "containers", "image", "1"},
							Matches: []string{"toblerone"},
							Op:      sqltypes.Eq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList:  makeList(t, sloop_snickers_toblerone),
		expectedTotal: 1,
	})
	tests = append(tests, testCase{
		description: "can sort on the first image",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "containers", "image", "0"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList: makeList(t,
			raft_almondjoy_ragusa,
			clipper_butterfinger_payday,
			kayak_butterfinger_rocher,
			trawler_butterfinger_aero,
			hydrofoil_coffeecrisp_ritter,
			catamaran_flake_none,
			sloop_snickers_toblerone,
			canoe_twix_ritter,
			ferry_twix_yorkie,
			schooner_twix_mounds,
		),
		expectedTotal: len(allObjects),
	})
	tests = append(tests, testCase{
		description: "can select for any second image",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"spec", "containers", "image", "1"},
							Matches: []string{""},
							Op:      sqltypes.NotEq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList: makeList(t, canoe_twix_ritter,
			clipper_butterfinger_payday,
			ferry_twix_yorkie,
			hydrofoil_coffeecrisp_ritter,
			kayak_butterfinger_rocher,
			raft_almondjoy_ragusa,
			schooner_twix_mounds,
			sloop_snickers_toblerone,
			trawler_butterfinger_aero,
		),
		expectedTotal: 9,
	})
	tests = append(tests, testCase{
		description: "can sort on the second image (when present)",
		listOptions: sqltypes.ListOptions{
			Filters: []sqltypes.OrFilter{
				{
					[]sqltypes.Filter{
						{
							Field:   []string{"spec", "containers", "image", "1"},
							Matches: []string{""},
							Op:      sqltypes.NotEq,
						},
					},
				},
			},
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"spec", "containers", "image", "1"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList: makeList(t,
			trawler_butterfinger_aero,
			schooner_twix_mounds,
			clipper_butterfinger_payday,
			raft_almondjoy_ragusa,
			canoe_twix_ritter,
			hydrofoil_coffeecrisp_ritter,
			kayak_butterfinger_rocher,
			sloop_snickers_toblerone,
			ferry_twix_yorkie,
		),
		expectedTotal: 9,
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, podGVK, opts, false, namespaceList)
			require.NoError(t, err)
			defer cleanTempFiles(dbPath)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				require.NoError(t, err)
			}
			list, total, summary, _, err := loi.ListByOptions(ctx, &test.listOptions, []partition.Partition{{All: true}}, "")
			require.NoError(t, err)
			assert.Equal(t, test.expectedTotal, total)
			assert.Equal(t, test.expectedList, list)
			require.Nil(t, summary)
		})
	}
}

func TestDropAll(t *testing.T) {
	ctx := t.Context()

	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	opts := ListOptionIndexerOptions{
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, nil)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	obj1 := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "obj1",
			},
		},
	}
	obj1.SetGroupVersionKind(gvk)
	err = loi.Add(obj1)
	assert.NoError(t, err)

	_, _, _, _, err = loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	assert.NoError(t, err)

	loi.DropAll(ctx)

	_, _, _, _, err = loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	assert.Error(t, err)
}

func makePseudoRandomList(gvk schema.GroupVersionKind, size int) *unstructured.UnstructuredList {
	numLength := 1 + int(math.Floor(math.Log10(float64(size))))
	name_template := fmt.Sprintf("n%%0%dd", numLength)
	// Make a predictable but randomish list of numbers
	// item 0: ns0, n0
	// item 23: ns0, n1
	// item 46: ns0, n2
	// At some point the index will be set back to the start
	// the ns value goes up every <ns_delta> hits
	// the name_val is the index, and i provides the name-value as we walk through the array.
	// Use any size, as long as both name_delta (23) and ns_delta (17) are relatively prime to it.
	// This assures that every index in the array will be initialized to an actual object
	name_val := 0
	name_delta := 23 // space the names out in runs of 23

	ns_val := 0
	ns_block := 0
	ns_delta := 17 // so only 17 namespaces
	namespace_template := "ns%02d"

	items := make([]unstructured.Unstructured, size)
	for i := range size {
		nv := fmt.Sprintf(name_template, i)
		nsv := fmt.Sprintf(namespace_template, ns_block)
		obj := unstructured.Unstructured{
			Object: map[string]any{
				"metadata": map[string]any{
					"name":      nv,
					"namespace": nsv,
				},
				"id": nv + "/" + nsv,
			},
		}
		obj.SetGroupVersionKind(gvk)
		items[name_val] = obj
		name_val += name_delta
		if name_val >= size {
			name_val -= size
		}
		ns_val += ns_delta
		if ns_val >= size {
			ns_val -= size
			ns_block += 1
		}
	}
	ulist := &unstructured.UnstructuredList{
		Items: items,
	}
	ulist.SetGroupVersionKind(gvk)
	return ulist
}

func verifyListIsSorted(b *testing.B, list *unstructured.UnstructuredList, size int) {
	for i := range size - 1 {
		curr := list.Items[i]
		next := list.Items[i+1]
		if curr.GetNamespace() == next.GetNamespace() {
			assert.Less(b, curr.GetName(), next.GetName())
		} else {
			assert.Less(b, curr.GetNamespace(), next.GetNamespace())
		}
	}
}
func BenchmarkNamespaceNameList(b *testing.B) {
	// At 50,000,000 this starts to get very slow
	size := 10000
	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
	itemList := makePseudoRandomList(gvk, size)
	ctx := context.Background()
	opts := ListOptionIndexerOptions{
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(b, err)
	for _, item := range itemList.Items {
		err = loi.Add(&item)
		assert.NoError(b, err)
	}
	b.Run(fmt.Sprintf("sort-%d with explicit namespace/name", size), func(b *testing.B) {
		listOptions := sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "namespace"},
						Order:  sqltypes.ASC,
					},
					{
						Fields: []string{"metadata", "name"},
						Order:  sqltypes.ASC,
					},
				},
			},
		}
		partitions := []partition.Partition{{All: true}}
		ns := ""
		list, total, _, _, err := loi.ListByOptions(ctx, &listOptions, partitions, ns)
		if err != nil {
			b.Fatal("error getting data", err)
		}
		if total != size {
			b.Errorf("expecting %d items, got %d", size, total)
		}
		if len(list.Items) != size {
			b.Errorf("expecting %d items, got %d", size, len(list.Items))
		}
		//verifyListIsSorted(b, list, size)
	})
	b.Run(fmt.Sprintf("sort-%d with explicit id", size), func(b *testing.B) {
		listOptions := sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"id"},
						Order:  sqltypes.ASC,
					},
				},
			},
		}
		partitions := []partition.Partition{{All: true}}
		ns := ""
		list, total, _, _, err := loi.ListByOptions(ctx, &listOptions, partitions, ns)
		if err != nil {
			b.Fatal("error getting data", err)
		}
		if total != size {
			b.Errorf("expecting %d items, got %d", size, total)
		}
		if len(list.Items) != size {
			b.Errorf("expecting %d items, got %d", size, len(list.Items))
		}
		//verifyListIsSorted(b, list, size)
	})

}

func TestUserDefinedExtractFunction(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")
	makeObj := func(name string, barSeparatedHosts string) map[string]any {
		h1 := map[string]any{
			"apiVersion": gvk.Version,
			"kind":       gvk.Kind,
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
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			list, total, summary, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedTotal, total)
			require.Nil(t, summary)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func TestUserDefinedInetToAnonFunction(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")
	makeObj := func(name string, ipAddr string) map[string]any {
		h1 := map[string]any{
			"apiVersion": gvk.Version,
			"kind":       gvk.Kind,
			"metadata": map[string]any{
				"name": name,
			},
			"status": map[string]any{
				"podIP": ipAddr,
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
	obj01 := makeObj("lirdle.com", "145.53.12.123")
	obj02 := makeObj("cyberciti.biz", "2607:f0d0:1002:51::4")
	obj03 := makeObj("zombo.com", "50.28.52.163")
	obj04 := makeObj("not-an-ipaddr", "aardvarks")
	obj05 := makeObj("smaller-cyberciti.biz", "2607:f0d0:997:51::4")
	allObjects := []map[string]any{obj01, obj02, obj03, obj04, obj05}
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
		description: "sort by numeric IP addr value",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields:   []string{"status", "podIP"},
						Order:    sqltypes.ASC,
						SortAsIP: true,
					},
				},
			},
		},
		partitions:        []partition.Partition{{All: true}},
		ns:                "",
		expectedList:      makeList(t, obj04, obj03, obj01, obj05, obj02),
		expectedTotal:     len(allObjects),
		expectedContToken: "",
		expectedErr:       nil,
	})
	t.Parallel()

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"status", "podIP"},
			}
			fields = append(fields, test.extraIndexedFields...)

			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			list, total, summary, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedTotal, total)
			require.Nil(t, summary)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func TestUserDefinedMemoryFunction(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")
	makeObj := func(name string, cpuCount int, memory string, podCount int) map[string]any {
		if cpuCount < 1 {
			cpuCount = 1
		}
		if memory == "" {
			memory = "1Gi"
		}
		if podCount < 1 {
			podCount = 1
		}
		cpuCountAvailable := cpuCount - 1
		if cpuCountAvailable < 1 {
			cpuCountAvailable = 1
		}
		podCountAvailable := podCount - 1
		if podCountAvailable < 1 {
			podCountAvailable = 1
		}
		h1 := map[string]any{
			"apiVersion": gvk.Version,
			"kind":       gvk.Kind,
			"metadata": map[string]any{
				"name": name,
			},
			"status": map[string]any{
				"allocatable": map[string]any{
					"cpu":    fmt.Sprintf("%d", cpuCount),
					"memory": memory,
					"pods":   fmt.Sprintf("%d", podCount),
				},
			},
		}
		lastDigit := name[len(name)-1:]
		val, err := strconv.Atoi(lastDigit)
		if err == nil && val%2 == 1 {
			newMap := map[string]any{
				"cpu":    fmt.Sprintf("%d", cpuCountAvailable),
				"memory": memory,
				"pods":   fmt.Sprintf("%d", podCountAvailable),
			}
			statusMap := h1["status"].(map[string]any)
			statusMap["requested"] = any(newMap)
		}
		return h1
	}

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

	obj01 := makeObj("obj01", 8, "1000", 2)
	obj02 := makeObj("obj02", 7, "12K", 12)
	obj03 := makeObj("obj03", 6, "3Ki", 3)
	obj04 := makeObj("obj04", 5, "8M", 13)
	obj05 := makeObj("obj05", 4, "12Mi", 25)
	obj06 := makeObj("obj06", 3, "71M", 5)
	obj07 := makeObj("obj07", 2, "55G", 24)
	obj08 := makeObj("obj08", 1, "104Gi", 4)
	allObjects := []map[string]any{obj01, obj02, obj03, obj04, obj05, obj06, obj07, obj08}
	ctx := context.Background()

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
		description: "filtering on cpu works",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"status", "allocatable", "cpu"},
						Matches: []string{"7"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		expectedList:  makeList(t, obj02),
		expectedTotal: 1,
	})
	tests = append(tests, testCase{
		description: "filtering on pod-count works",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"status", "allocatable", "pods"},
						Matches: []string{"25"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		expectedList:  makeList(t, obj05),
		expectedTotal: 1,
	})
	tests = append(tests, testCase{
		description: "filtering on memory works",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"status", "allocatable", "memory"},
						Matches: []string{"8M"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		expectedList:  makeList(t, obj04),
		expectedTotal: 1,
	})
	tests = append(tests, testCase{
		description: "sorting on memory does a naive ascii sort",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"status", "allocatable", "memory"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		expectedList:  makeList(t, obj01, obj08, obj02, obj05, obj03, obj07, obj06, obj04),
		expectedTotal: len(allObjects),
	})
	tests = append(tests, testCase{
		description: "filtering on requested pod-count works",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"status", "requested", "pods"},
						Matches: []string{"24"},
						Op:      sqltypes.Eq,
					},
				},
			},
		},
		},
		expectedList:  makeList(t, obj05),
		expectedTotal: 1,
	})
	tests = append(tests, testCase{
		description: "filtering on requested cpu-count works",
		listOptions: sqltypes.ListOptions{Filters: []sqltypes.OrFilter{
			{
				[]sqltypes.Filter{
					{
						Field:   []string{"status", "requested", "cpu"},
						Matches: []string{"1", "3"},
						Op:      sqltypes.In,
					},
				},
			},
		},
		},
		expectedList:  makeList(t, obj07, obj05),
		expectedTotal: 2,
	})

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			fields := [][]string{
				{"status", "allocatable", "cpu"},
				{"status", "allocatable", "memory"},
				{"status", "allocatable", "pods"},
				{"status", "requested", "cpu"},
				{"status", "requested", "memory"},
				{"status", "requested", "pods"},
			}
			fields = append(fields, test.extraIndexedFields...)
			if len(test.partitions) == 0 {
				test.partitions = []partition.Partition{{All: true}}
			}
			opts := ListOptionIndexerOptions{
				Fields:       fields,
				IsNamespaced: true,
			}
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
			defer cleanTempFiles(dbPath)
			assert.NoError(t, err)

			for _, item := range itemList.Items {
				err = loi.Add(&item)
				assert.NoError(t, err)
			}

			list, total, summary, contToken, err := loi.ListByOptions(ctx, &test.listOptions, test.partitions, test.ns)
			if test.expectedErr != nil {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, test.expectedTotal, total)
			require.Nil(t, summary)
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
		description: "TestConstructQuery: handles ProjectOrNamespaces IN",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					sqltypes.Filter{
						Field:   []string{"metadata", "name"},
						Matches: []string{"some_namespace"},
						Op:      sqltypes.In,
					},
					sqltypes.Filter{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"some_namespace"},
						Op:      sqltypes.In,
					},
				},
			},
			Filters: []sqltypes.OrFilter{},
		},
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "_v1_Namespace_fields" nsf ON f."metadata.namespace" = nsf."metadata.name"
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON nsf.key = lt1.key
  WHERE
    ((nsf."metadata.name" IN (?)) OR (lt1.label = ? AND lt1.value IN (?)))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"some_namespace", "field.cattle.io/projectId", "some_namespace"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles ProjectOrNamespaces multiple IN",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					sqltypes.Filter{
						Field:   []string{"metadata", "name"},
						Matches: []string{"some_namespace", "p-example"},
						Op:      sqltypes.In,
					},
					sqltypes.Filter{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"some_namespace", "p-example"},
						Op:      sqltypes.In,
					},
				},
			},
			Filters: []sqltypes.OrFilter{},
		},
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "_v1_Namespace_fields" nsf ON f."metadata.namespace" = nsf."metadata.name"
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON nsf.key = lt1.key
  WHERE
    ((nsf."metadata.name" IN (?, ?)) OR (lt1.label = ? AND lt1.value IN (?, ?)))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"some_namespace", "p-example", "field.cattle.io/projectId", "some_namespace", "p-example"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles ProjectOrNamespaces NOT IN",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					sqltypes.Filter{
						Field:   []string{"metadata", "name"},
						Matches: []string{"some_namespace"},
						Op:      sqltypes.NotIn,
					},
					sqltypes.Filter{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"some_namespace"},
						Op:      sqltypes.NotIn,
					},
				},
			},
			Filters: []sqltypes.OrFilter{},
		},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "_v1_Namespace_fields" nsf ON f."metadata.namespace" = nsf."metadata.name"
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON nsf.key = lt1.key
  WHERE
    ((nsf."metadata.name" NOT IN (?)) AND ((lt1.label = ? AND lt1.value NOT IN (?)) OR (o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "_v1_Namespace_fields" nsf1 ON f1."metadata.namespace" = nsf1."metadata.name"
		LEFT OUTER JOIN "_v1_Namespace_labels" lt1i1 ON nsf1.key = lt1i1.key
		WHERE lt1i1.label = ?))))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"some_namespace", "field.cattle.io/projectId", "some_namespace", "field.cattle.io/projectId"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles ProjectOrNamespaces multiple NOT IN",
		listOptions: sqltypes.ListOptions{
			ProjectsOrNamespaces: sqltypes.OrFilter{
				Filters: []sqltypes.Filter{
					sqltypes.Filter{
						Field:   []string{"metadata", "name"},
						Matches: []string{"some_namespace", "p-example"},
						Op:      sqltypes.NotIn,
					},
					sqltypes.Filter{
						Field:   []string{"metadata", "labels", "field.cattle.io/projectId"},
						Matches: []string{"some_namespace", "p-example"},
						Op:      sqltypes.NotIn,
					},
				},
			},
			Filters: []sqltypes.OrFilter{},
		},
		partitions: []partition.Partition{{All: true}},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "_v1_Namespace_fields" nsf ON f."metadata.namespace" = nsf."metadata.name"
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON nsf.key = lt1.key
  WHERE
    ((nsf."metadata.name" NOT IN (?, ?)) AND ((lt1.label = ? AND lt1.value NOT IN (?, ?)) OR (o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "_v1_Namespace_fields" nsf1 ON f1."metadata.namespace" = nsf1."metadata.name"
		LEFT OUTER JOIN "_v1_Namespace_labels" lt1i1 ON nsf1.key = lt1i1.key
		WHERE lt1i1.label = ?))))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"some_namespace", "p-example", "field.cattle.io/projectId", "some_namespace", "p-example", "field.cattle.io/projectId"},
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
    ((o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt1i1 ON f1.key = lt1i1.key
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
    ((o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt1i1 ON f1.key = lt1i1.key
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
    ((o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt1i1 ON f1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)) AND
    ((o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt2i1 ON f1.key = lt2i1.key
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
    ((o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt1i1 ON f1.key = lt1i1.key
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
    (o.key NOT IN (SELECT f1.key FROM "something_fields" f1
		LEFT OUTER JOIN "something_labels" lt1i1 ON f1.key = lt1i1.key
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
						Fields: []string{"metadata", "labels", "unbound"},
						Order:  sqltypes.ASC,
					},
				},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `WITH lt1(key, value) AS (
SELECT key, value FROM "something_labels"
  WHERE label = ?
)
SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN lt1 ON o.key = lt1.key
  WHERE
    (FALSE)
  ORDER BY lt1.value ASC NULLS LAST`,
		expectedStmtArgs: []any{"unbound"},
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
		expectedStmt: `WITH lt1(key, value) AS (
SELECT key, value FROM "something_labels"
  WHERE label = ?
)
SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((f."metadata.queryField1" = ?) OR (lt2.label = ? AND lt2.value = ?)) AND
    (FALSE)
  ORDER BY lt1.value ASC NULLS LAST, f."status.queryField2" DESC`,
		expectedStmtArgs: []any{"this", "toys", "jamb", "juice"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: sort on an IP-designated field does an inet_aton conversion",
		listOptions: sqltypes.ListOptions{
			SortList: sqltypes.SortList{
				SortDirectives: []sqltypes.Sort{
					{
						Fields: []string{"metadata", "queryField1"},
						Order:  sqltypes.ASC,
					},
					{
						Fields:   []string{"status", "podIP"},
						Order:    sqltypes.ASC,
						SortAsIP: true,
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
  ORDER BY f."metadata.queryField1" ASC, inet_aton(f."status.podIP") ASC`,
		expectedStmtArgs: []any{},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: sort can ip-convert a label field",
		listOptions: sqltypes.ListOptions{
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
		expectedStmt: `WITH lt1(key, value) AS (
SELECT key, value FROM "something_labels"
  WHERE label = ?
)
SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN lt1 ON o.key = lt1.key
  WHERE
    (FALSE)
  ORDER BY lt1.value ASC NULLS LAST, f."status.queryField2" DESC`,
		expectedStmtArgs: []any{"this"},
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
				indexedFields: []string{"metadata.name", "metadata.queryField1", "status.queryField2", "spec.containers.image", "status.podIP", "metadata.namespace"},
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
			whereClauses: []string{`(f1."metadata.queryField1" = ?) OR (lt1.label = ? AND lt1.value = ?)`},
			params:       []any{"toys", "animals", "starfish"},
			isEmpty:      false,
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
			params:  []any{"books", "pigs", "pigs", "boars"},
			isEmpty: false,
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
			whereClauses: []string{`lt1.label = ? AND lt1.value LIKE ? ESCAPE '\'`}, //'
			joinParts: standardJoinParts,
			params:  []any{"kubernetes.io/metadata.name", "%kube%"},
			isEmpty: false,
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
			params:  []any{"knot", "knot", "hitch", "%g%"},
			isEmpty: false,
		},
		expectedJoinTable: map[string]int{"knot": 1},
	})
	t.Parallel()
	dbName := "something"
	mainFieldPrefix := "f1"
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			store.EXPECT().GetName().Return("something").AnyTimes()
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.name", "metadata.namespace", "metadata.queryField1", "metadata.state.name", "spec.containers.image"},
			}
			joinTableIndexByLabelName := make(map[string]int)
			filterComponents, err := lii.constructSummaryTestFilters(&test.listOptions, test.partitions, test.ns, dbName, mainFieldPrefix, joinTableIndexByLabelName)
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
	GROUP BY k`,
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
				indexedFields: []string{"metadata.name", "metadata.namespace", "metadata.queryField1", "metadata.state.name", "spec.containers.image"},
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
			queryInfo, err := lii.constructSummaryQueryForField(test.summaryField, fieldNum, dbName, test.filterComponents, mainFieldPrefix, joinTableIndex)
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
		sortAsIP                  bool
		expectedStmt              string
		expectedErr               string
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "TestBuildSortClause: empty index list errors",
		labelName:   "emptyListError",
		expectedErr: `internal error: no join-table index given for label "emptyListError"`,
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit ascending",
		labelName:                 "testBSL1",
		joinTableIndexByLabelName: map[string]int{"testBSL1": 3},
		direction:                 true,
		expectedStmt:              `lt3.value ASC NULLS LAST`,
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit descending",
		labelName:                 "testBSL2",
		joinTableIndexByLabelName: map[string]int{"testBSL2": 4},
		direction:                 false,
		expectedStmt:              `lt4.value DESC NULLS FIRST`,
	})
	tests = append(tests, testCase{
		description:               "TestBuildSortClause: hit descending",
		labelName:                 "testBSL3",
		joinTableIndexByLabelName: map[string]int{"testBSL3": 5},
		direction:                 false,
		sortAsIP:                  true,
		expectedStmt:              `inet_aton(lt5.value) DESC NULLS FIRST`,
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			stmt, err := buildSortLabelsClause(test.labelName, test.joinTableIndexByLabelName, test.direction, test.sortAsIP)
			if test.expectedErr != "" {
				assert.Equal(t, test.expectedErr, err.Error())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.expectedStmt, stmt)
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

func TestWatchEncryption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	opts := ListOptionIndexerOptions{
		Fields: [][]string{
			{"metadata", "somefield"},
			{"spec", "replicas"},
			{"spec", "minReplicas"},
		},
		IsNamespaced: true,
	}
	// shouldEncrypt = true to ensure we can write + read from encrypted events
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, true, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"spec": map[string]any{
				"replicas": int64(1),
			},
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("120")

	startWatcher := func(ctx context.Context) (chan watch.Event, chan error) {
		errCh := make(chan error, 1)
		eventsCh := make(chan watch.Event, 100)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{
				// Make a watch request to this specific resource version to be sure we go get from SQL database
				ResourceVersion: "100",
			}, eventsCh)
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

	err = loi.Add(foo)
	assert.NoError(t, err)
	err = loi.Update(foo2)
	assert.NoError(t, err)

	watcher1, errCh1 := startWatcher(ctx)
	events := receiveEvents(watcher1)
	assert.Len(t, events, 1)
	assert.Equal(t, []watch.Event{
		{
			Type:   watch.Modified,
			Object: foo2,
		},
	}, events)

	cancel()

	err = waitStopWatcher(errCh1)
	assert.NoError(t, err)
}

func TestWatchMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gvk := corev1.SchemeGroupVersion.WithKind("Pod")

	opts := ListOptionIndexerOptions{
		Fields: [][]string{
			{"metadata", "somefield"},
			{"spec", "replicas"},
			{"spec", "minReplicas"},
		},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
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
	foo.SetGroupVersionKind(gvk)
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
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
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
	foo.SetGroupVersionKind(gvk)
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
	bar.SetGroupVersionKind(gvk)
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
			loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
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
	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")
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
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo.SetName("foo")
	foo.SetNamespace("foo")
	foo.Object["id"] = "foo/foo"
	foo.SetLabels(map[string]string{
		"app": "foo",
	})

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetResourceVersion("120")
	fooUpdated.SetLabels(map[string]string{
		"app": "changed",
	})

	bar := &unstructured.Unstructured{}
	bar.SetGroupVersionKind(gvk)
	bar.SetResourceVersion("150")
	bar.SetName("bar")
	bar.SetNamespace("bar")
	bar.Object["id"] = "bar/bar"
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
	loi, dbPath, err := makeListOptionIndexer(parentCtx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	getRV := func(t *testing.T) string {
		t.Helper()
		list, _, _, _, err := loi.ListByOptions(parentCtx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
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
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
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
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")
	foo.SetName("foo")

	fooUpdated := foo.DeepCopy()
	fooUpdated.SetResourceVersion("120")

	bar := &unstructured.Unstructured{}
	bar.SetGroupVersionKind(gvk)
	bar.SetResourceVersion("150")
	bar.SetName("bar")

	barDeleted := bar.DeepCopy()
	barDeleted.SetResourceVersion("160")

	barNew := bar.DeepCopy()
	barNew.SetResourceVersion("170")

	parentCtx := context.Background()

	opts := ListOptionIndexerOptions{
		GCInterval:  40 * time.Millisecond,
		GCKeepCount: 2,
	}
	loi, dbPath, err := makeListOptionIndexer(parentCtx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	getRV := func(t *testing.T) string {
		t.Helper()
		list, _, _, _, err := loi.ListByOptions(parentCtx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
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

	// Make sure GC runs
	time.Sleep(2 * opts.GCInterval)

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

	// Make sure GC runs
	time.Sleep(2 * opts.GCInterval)

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
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")

	opts := ListOptionIndexerOptions{
		Fields:       [][]string{{"metadata", "somefield"}},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
			"id": "/foo",
		},
	}
	foo.SetGroupVersionKind(gvk)
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
			"id": "/bar",
		},
	}
	bar.SetGroupVersionKind(gvk)
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

	list, _, _, _, err := loi.ListByOptions(ctx, &sqltypes.ListOptions{}, []partition.Partition{{All: true}}, "")
	require.NoError(t, err)
	assert.Equal(t, expectedList.Items, list.Items)
}

// Test that we don't panic in case the transaction fails but stil manages to add a watcher
func TestWatchCancel(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("TestKind")
	startWatcher := func(ctx context.Context, loi *ListOptionIndexer, rv string) (chan watch.Event, chan error) {
		eventsCh := make(chan watch.Event, 1)
		errCh := make(chan error, 1)
		go func() {
			watchErr := loi.Watch(ctx, WatchOptions{ResourceVersion: rv}, eventsCh)
			errCh <- watchErr
			close(eventsCh)
		}()
		time.Sleep(100 * time.Millisecond)
		return eventsCh, errCh
	}

	ctx := context.Background()

	opts := ListOptionIndexerOptions{
		Fields:       [][]string{{"metadata", "somefield"}},
		IsNamespaced: true,
	}
	loi, dbPath, err := makeListOptionIndexer(ctx, gvk, opts, false, emptyNamespaceList)
	defer cleanTempFiles(dbPath)
	assert.NoError(t, err)

	foo := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "foo",
			},
		},
	}
	foo.SetGroupVersionKind(gvk)
	foo.SetResourceVersion("100")

	foo2 := foo.DeepCopy()
	foo2.SetResourceVersion("200")

	foo3 := foo.DeepCopy()
	foo3.SetResourceVersion("300")

	err = loi.Add(foo)
	assert.NoError(t, err)
	loi.Add(foo2)
	assert.NoError(t, err)
	loi.Add(foo3)
	assert.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)

	eventsCh, errCh := startWatcher(watchCtx, loi, "100")

	<-eventsCh

	watchCancel()

	<-eventsCh

	go func() {
		foo4 := foo.DeepCopy()
		foo4.SetResourceVersion("400")
		loi.Add(foo4)
	}()
	<-errCh
	time.Sleep(1 * time.Second)
}

func Test_watcherWithBackfill(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	eventsCh := make(chan int, 10)
	w, doneCb, closeWatcher := watcherWithBackfill(ctx, eventsCh, 100)
	defer closeWatcher()

	eventsCh <- 1
	eventsCh <- 2
	w <- 5
	eventsCh <- 3
	w <- 6
	eventsCh <- 4
	doneCb()

	time.Sleep(10 * time.Millisecond)
	w <- 7
	w <- 8

	time.Sleep(10 * time.Millisecond)
	cancel()

	close(eventsCh)
	res := make([]int, 0, len(eventsCh))
	for n := range eventsCh {
		res = append(res, n)
	}

	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, res)
}

// This test aims to detect a very specific race condition, see https://github.com/rancher/steve/pull/879 for details
func Test_watcherWithBackfillCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	eventsCh := make(chan int)
	w, doneCb, closeWatcher := watcherWithBackfill(ctx, eventsCh, 100)
	defer closeWatcher()
	doneCb()

	select {
	case w <- 1:
		t.Fatal("expected blocking trying to write")
	default:
	}

	doneWriting := make(chan struct{})
	go func() {
		w <- 1 // should be discarded
		close(doneWriting)
	}()
	time.Sleep(100 * time.Millisecond)

	cancel()
	select {
	case <-doneWriting:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected writing to not block when context is canceled")
	}
}
