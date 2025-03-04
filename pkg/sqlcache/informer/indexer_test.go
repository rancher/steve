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
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/client-go/tools/cache"
)

//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./sql_mocks_test.go github.com/rancher/steve/pkg/sqlcache/informer Store
//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Rows,Client
//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./transaction_mocks_test.go -mock_names Client=MockTXClient github.com/rancher/steve/pkg/sqlcache/db/transaction Stmt,Client

type testStoreObject struct {
	Id  string
	Val string
}

func TestNewIndexer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "NewIndexer() with no errors returned from Store or Client, should return no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))

		objKey := "objKey"
		indexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		storeName := "someStoreName"

		store.EXPECT().GetName().AnyTimes().Return(storeName)
		client.EXPECT().Exec(fmt.Sprintf(createTableFmt, storeName, storeName)).Return(nil, nil)
		client.EXPECT().Exec(fmt.Sprintf(createIndexFmt, storeName, storeName)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(client)
				if err != nil {
					t.Fail()
				}
			})
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(fmt.Sprintf(deleteIndicesFmt, storeName))
		store.EXPECT().Prepare(fmt.Sprintf(addIndexFmt, storeName))
		store.EXPECT().Prepare(fmt.Sprintf(listByIndexFmt, storeName, storeName))
		store.EXPECT().Prepare(fmt.Sprintf(listKeyByIndexFmt, storeName))
		store.EXPECT().Prepare(fmt.Sprintf(listIndexValuesFmt, storeName))
		indexer, err := NewIndexer(context.Background(), indexers, store)
		assert.Nil(t, err)
		assert.Equal(t, cache.Indexers(indexers), indexer.indexers)
	}})
	tests = append(tests, testCase{description: "NewIndexer() with WithTransaction() error, should return error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))

		objKey := "objKey"
		indexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		store.EXPECT().GetName().AnyTimes().Return("someStoreName")
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))
		_, err := NewIndexer(context.Background(), indexers, store)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewIndexer() with Client Exec() error on first call to Exec(), should return error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))

		objKey := "objKey"
		indexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		storeName := "someStoreName"
		store.EXPECT().GetName().AnyTimes().Return(storeName)
		client.EXPECT().Exec(fmt.Sprintf(createTableFmt, storeName, storeName)).Return(nil, fmt.Errorf("error"))

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(client)
				if err == nil {
					t.Fail()
				}
			})
		_, err := NewIndexer(context.Background(), indexers, store)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewIndexer() with Client Exec() error on second call to Exec(), should return error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))

		objKey := "objKey"
		indexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		storeName := "someStoreName"
		store.EXPECT().GetName().AnyTimes().Return(storeName)
		client.EXPECT().Exec(fmt.Sprintf(createTableFmt, storeName, storeName)).Return(nil, nil)
		client.EXPECT().Exec(fmt.Sprintf(createIndexFmt, storeName, storeName)).Return(nil, fmt.Errorf("error"))

		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(client)
				if err == nil {
					t.Fail()
				}
			})

		_, err := NewIndexer(context.Background(), indexers, store)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewIndexer() with Client Commit() error, should return error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))

		objKey := "objKey"
		indexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		storeName := "someStoreName"
		store.EXPECT().GetName().AnyTimes().Return(storeName)
		client.EXPECT().Exec(fmt.Sprintf(createTableFmt, storeName, storeName)).Return(nil, nil)
		client.EXPECT().Exec(fmt.Sprintf(createIndexFmt, storeName, storeName)).Return(nil, nil)
		store.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(client)
				if err != nil {
					t.Fail()
				}
			})
		_, err := NewIndexer(context.Background(), indexers, store)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestAfterUpsert(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "AfterUpsert() with no errors returned from Client should return no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))
		objKey := "key"
		deleteIndicesStmt := NewMockStmt(gomock.NewController(t))
		addIndexStmt := NewMockStmt(gomock.NewController(t))
		indexer := &Indexer{
			ctx:   context.Background(),
			Store: store,
			indexers: map[string]cache.IndexFunc{
				"a": func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		key := "somekey"
		client.EXPECT().Stmt(indexer.deleteIndicesStmt).Return(deleteIndicesStmt)
		deleteIndicesStmt.EXPECT().Exec(key).Return(nil, nil)
		client.EXPECT().Stmt(indexer.addIndexStmt).Return(addIndexStmt)
		addIndexStmt.EXPECT().Exec("a", objKey, key).Return(nil, nil)
		testObject := testStoreObject{Id: "something", Val: "a"}
		err := indexer.AfterUpsert(key, testObject, client)
		assert.Nil(t, err)
	}})
	tests = append(tests, testCase{description: "AfterUpsert() with error returned from Client StmtExec() should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))
		objKey := "key"
		deleteIndicesStmt := NewMockStmt(gomock.NewController(t))
		indexer := &Indexer{
			ctx:   context.Background(),
			Store: store,

			indexers: map[string]cache.IndexFunc{
				"a": func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		key := "somekey"
		client.EXPECT().Stmt(indexer.deleteIndicesStmt).Return(deleteIndicesStmt)
		deleteIndicesStmt.EXPECT().Exec(key).Return(nil, fmt.Errorf("error"))
		testObject := testStoreObject{Id: "something", Val: "a"}
		err := indexer.AfterUpsert(key, testObject, client)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "AfterUpsert() with error returned from Client second StmtExec() call should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		client := NewMockTXClient(gomock.NewController(t))
		deleteIndicesStmt := NewMockStmt(gomock.NewController(t))
		addIndexStmt := NewMockStmt(gomock.NewController(t))
		objKey := "key"
		indexer := &Indexer{
			ctx:   context.Background(),
			Store: store,
			indexers: map[string]cache.IndexFunc{
				"a": func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		key := "somekey"
		client.EXPECT().Stmt(indexer.deleteIndicesStmt).Return(deleteIndicesStmt)
		deleteIndicesStmt.EXPECT().Exec(key).Return(nil, nil)
		client.EXPECT().Stmt(indexer.addIndexStmt).Return(addIndexStmt)
		addIndexStmt.EXPECT().Exec("a", objKey, key).Return(nil, fmt.Errorf("error"))
		testObject := testStoreObject{Id: "something", Val: "a"}
		err := indexer.AfterUpsert(key, testObject, client)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestIndex(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "Index() with no errors returned from store and 1 object returned by ReadObjects(), should return one obj and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject}, nil)
		objs, err := indexer.Index(indexName, testObject)
		assert.Nil(t, err)
		assert.Equal(t, []any{testObject}, objs)
	}})
	tests = append(tests, testCase{description: "Index() with no errors returned from store and multiple objects returned by ReadObjects(), should return multiple objects and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject, testObject}, nil)
		objs, err := indexer.Index(indexName, testObject)
		assert.Nil(t, err)
		assert.Equal(t, []any{testObject, testObject}, objs)
	}})
	tests = append(tests, testCase{description: "Index() with no errors returned from store and no objects returned by ReadObjects(), should return no objects and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{}, nil)
		objs, err := indexer.Index(indexName, testObject)
		assert.Nil(t, err)
		assert.Equal(t, []any{}, objs)
	}})
	tests = append(tests, testCase{description: "Index() where index name is not in indexers, should return error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		_, err := indexer.Index("someotherindexname", testObject)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "Index() with an error returned from store QueryForRows, should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(nil, fmt.Errorf("error"))
		_, err := indexer.Index(indexName, testObject)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "Index() with an errors returned from store ReadObjects(), should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject}, fmt.Errorf("error"))
		_, err := indexer.Index(indexName, testObject)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "Index() with no errors returned from store and multiple keys returned from index func, should return one obj and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
			indexers: map[string]cache.IndexFunc{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{objKey, objKey + "2"}, nil
				},
			},
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().GetName().Return("name")
		stmt := &sql.Stmt{}
		store.EXPECT().Prepare(fmt.Sprintf(selectQueryFmt, "name", ", ?")).Return(stmt)
		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey, objKey+"2").Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject}, nil)
		store.EXPECT().CloseStmt(stmt).Return(nil)
		objs, err := indexer.Index(indexName, testObject)
		assert.Nil(t, err)
		assert.Equal(t, []any{testObject}, objs)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestByIndex(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "IndexBy() with no errors returned from store and 1 object returned by ReadObjects(), should return one obj and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject}, nil)
		objs, err := indexer.ByIndex(indexName, objKey)
		assert.Nil(t, err)
		assert.Equal(t, []any{testObject}, objs)
	}})
	tests = append(tests, testCase{description: "IndexBy() with no errors returned from store and multiple objects returned by ReadObjects(), should return multiple objects and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject, testObject}, nil)
		objs, err := indexer.ByIndex(indexName, objKey)
		assert.Nil(t, err)
		assert.Equal(t, []any{testObject, testObject}, objs)
	}})
	tests = append(tests, testCase{description: "IndexBy() with no errors returned from store and no objects returned by ReadObjects(), should return no objects and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{}, nil)
		objs, err := indexer.ByIndex(indexName, objKey)
		assert.Nil(t, err)
		assert.Equal(t, []any{}, objs)
	}})
	tests = append(tests, testCase{description: "IndexBy() with an error returned from store QueryForRows, should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(nil, fmt.Errorf("error"))
		_, err := indexer.ByIndex(indexName, objKey)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "IndexBy() with an errors returned from store ReadObjects(), should return an error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		objKey := "key"
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		testObject := testStoreObject{Id: "something", Val: "a"}

		store.EXPECT().QueryForRows(context.Background(), indexer.listByIndexStmt, indexName, objKey).Return(rows, nil)
		store.EXPECT().GetType().Return(reflect.TypeOf(testObject))
		store.EXPECT().GetShouldEncrypt().Return(false)
		store.EXPECT().ReadObjects(rows, reflect.TypeOf(testObject), false).Return([]any{testObject}, fmt.Errorf("error"))
		_, err := indexer.ByIndex(indexName, objKey)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestListIndexFuncValues(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "ListIndexFuncvalues() with no errors returned from store and 1 object returned by ReadObjects(), should return one obj and no error", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		store.EXPECT().QueryForRows(context.Background(), indexer.listIndexValuesStmt, indexName).Return(rows, nil)
		store.EXPECT().ReadStrings(rows).Return([]string{"somestrings"}, nil)
		vals := indexer.ListIndexFuncValues(indexName)
		assert.Equal(t, []string{"somestrings"}, vals)
	}})
	tests = append(tests, testCase{description: "ListIndexFuncvalues() with QueryForRows() error returned from store, should panic", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		listStmt := &sql.Stmt{}
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		store.EXPECT().QueryForRows(context.Background(), indexer.listIndexValuesStmt, indexName).Return(nil, fmt.Errorf("error"))
		assert.Panics(t, func() { indexer.ListIndexFuncValues(indexName) })
	}})
	tests = append(tests, testCase{description: "ListIndexFuncvalues() with ReadStrings() error returned from store, should panic", test: func(t *testing.T) {
		store := NewMockStore(gomock.NewController(t))
		rows := &sql.Rows{}
		listStmt := &sql.Stmt{}
		indexName := "someindexname"
		indexer := &Indexer{
			ctx:             context.Background(),
			Store:           store,
			listByIndexStmt: listStmt,
		}
		store.EXPECT().QueryForRows(context.Background(), indexer.listIndexValuesStmt, indexName).Return(rows, nil)
		store.EXPECT().ReadStrings(rows).Return([]string{"somestrings"}, fmt.Errorf("error"))
		assert.Panics(t, func() { indexer.ListIndexFuncValues(indexName) })
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestGetIndexers(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "GetIndexers() should return indexers fron indexers field", test: func(t *testing.T) {
		objKey := "key"
		expectedIndexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		indexer := &Indexer{
			ctx:      context.Background(),
			indexers: expectedIndexers,
		}
		indexers := indexer.GetIndexers()
		assert.Equal(t, cache.Indexers(expectedIndexers), indexers)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestAddIndexers(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "GetIndexers() should return indexers fron indexers field", test: func(t *testing.T) {
		objKey := "key"
		expectedIndexers := map[string]cache.IndexFunc{
			"a": func(obj interface{}) ([]string, error) {
				return []string{objKey}, nil
			},
		}
		indexer := &Indexer{}
		err := indexer.AddIndexers(expectedIndexers)
		assert.Nil(t, err)
		assert.ObjectsAreEqual(cache.Indexers(expectedIndexers), indexer.indexers)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
