/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package store

// Mocks for this test are generated with the following command.
//go:generate mockgen --build_flags=--mod=mod -package store -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Rows,Client
//go:generate mockgen --build_flags=--mod=mod -package store -destination ./transaction_mocks_test.go -mock_names Client=MockTXClient github.com/rancher/steve/pkg/sqlcache/db/transaction Stmt,Client

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/db/transaction"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func testStoreKeyFunc(obj interface{}) (string, error) {
	return obj.(testStoreObject).Id, nil
}

type testStoreObject struct {
	Id  string
	Val string
}

func TestAdd(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Add with no DB client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Add(testObject)
		assert.Nil(t, err)
	},
	})

	tests = append(tests, testCase{description: "Add with no DB client errors and an afterAdd function", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		var count int
		store.afterAdd = append(store.afterAdd, func(key string, object any, tx transaction.Client) error {
			count++
			return nil
		})
		err := store.Add(testObject)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	},
	})

	tests = append(tests, testCase{description: "Add with no DB client errors and an afterAdd function that returns error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})

		store.afterAdd = append(store.afterAdd, func(key string, object any, txC transaction.Client) error {
			return fmt.Errorf("error")
		})
		err := store.Add(testObject)
		assert.NotNil(t, err)
	},
	})

	tests = append(tests, testCase{description: "Add with DB client WithTransaction error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("failed"))

		store := SetupStore(t, c, shouldEncrypt)
		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Add with DB client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("failed")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})
		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Add with DB client Commit() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("failed")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Update updates the given object in the accumulator associated with the given object's key
func TestUpdate(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Update with no DB client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})
		err := store.Update(testObject)
		assert.Nil(t, err)
	},
	})

	tests = append(tests, testCase{description: "Update with no DB client errors and an afterUpdate function", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		var count int
		store.afterUpdate = append(store.afterUpdate, func(key string, object any, txC transaction.Client) error {
			count++
			return nil
		})
		err := store.Update(testObject)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	},
	})

	tests = append(tests, testCase{description: "Update with no DB client errors and an afterUpdate function that returns error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})

		store.afterUpdate = append(store.afterUpdate, func(key string, object any, txC transaction.Client) error {
			return fmt.Errorf("error")
		})
		err := store.Update(testObject)
		assert.NotNil(t, err)
	},
	})

	tests = append(tests, testCase{description: "Update with DB client WithTransaction returning error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))

		store := SetupStore(t, c, shouldEncrypt)
		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Update with DB client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})
		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Delete deletes the given object from the accumulator associated with the given object's key
func TestDelete(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Delete with no DB client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		stmt := NewMockStmt(gomock.NewController(t))
		txC.EXPECT().Stmt(store.deleteStmt).Return(stmt)
		stmt.EXPECT().Exec(testObject.Id).Return(nil, nil)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Delete(testObject)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with DB client WithTransaction returning error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))
		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with TX client Exec() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		stmt := NewMockStmt(gomock.NewController(t))
		txC.EXPECT().Stmt(store.deleteStmt).Return(stmt)
		stmt.EXPECT().Exec(testObject.Id).Return(nil, fmt.Errorf("error"))

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})

		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with DB client Commit() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		stmt := NewMockStmt(gomock.NewController(t))
		txC.EXPECT().Stmt(store.deleteStmt).Return(stmt)
		stmt.EXPECT().Exec(testObject.Id).Return(nil, nil)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// List returns a list of all the currently non-empty accumulators
func TestList(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	tests = append(tests, testCase{description: "List with no DB client errors and no items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		items := store.List()
		assert.Len(t, items, 0)
	},
	})
	tests = append(tests, testCase{description: "List with no DB client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		fakeItemsToReturn := []any{"something1", 2, false}
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(fakeItemsToReturn, nil)
		items := store.List()
		assert.Equal(t, fakeItemsToReturn, items)
	},
	})
	tests = append(tests, testCase{description: "List with DB client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		defer func() {
			recover()
		}()
		_ = store.List()
		assert.Fail(t, "Store list should panic when ReadObjects returns an error")
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// ListKeys returns a list of all the keys currently associated with non-empty accumulators
func TestListKeys(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "ListKeys with no DB client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{"a", "b", "c"}, nil)
		keys := store.ListKeys()
		assert.Len(t, keys, 3)
	},
	})

	tests = append(tests, testCase{description: "ListKeys with DB client ReadStrings() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return(nil, fmt.Errorf("error"))
		keys := store.ListKeys()
		assert.Len(t, keys, 0)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Get returns the accumulator associated with the given object's key
func TestGet(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "Get with no DB client errors and object exists", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{testObject}, nil)
		item, exists, err := store.Get(testObject)
		assert.Nil(t, err)
		assert.Equal(t, item, testObject)
		assert.True(t, exists)
	},
	})
	tests = append(tests, testCase{description: "Get with no DB client errors and object does not exist", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		item, exists, err := store.Get(testObject)
		assert.Nil(t, err)
		assert.Equal(t, item, nil)
		assert.False(t, exists)
	},
	})
	tests = append(tests, testCase{description: "Get with DB client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		_, _, err := store.Get(testObject)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// GetByKey returns the accumulator associated with the given key
func TestGetByKey(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "GetByKey with no DB client errors and item exists", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{testObject}, nil)
		item, exists, err := store.GetByKey(testObject.Id)
		assert.Nil(t, err)
		assert.Equal(t, item, testObject)
		assert.True(t, exists)
	},
	})
	tests = append(tests, testCase{description: "GetByKey with no DB client errors and item does not exist", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		item, exists, err := store.GetByKey(testObject.Id)
		assert.Nil(t, err)
		assert.Equal(t, nil, item)
		assert.False(t, exists)
	},
	})
	tests = append(tests, testCase{description: "GetByKey with DB client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.Background(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		_, _, err := store.GetByKey(testObject.Id)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Replace will delete the contents of the store, using instead the
// given list. Store takes ownership of the list, you should not reference
// it after calling this function.
func TestReplace(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "Replace with no DB client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		stmt := NewMockStmt(gomock.NewController(t))

		txC.EXPECT().Stmt(store.deleteAllStmt).Return(stmt)
		stmt.EXPECT().Exec()
		c.EXPECT().Upsert(txC, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Replace([]any{testObject}, testObject.Id)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with no DB client errors and no items", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		stmt := NewMockStmt(gomock.NewController(t))
		txC.EXPECT().Stmt(store.deleteAllStmt).Return(stmt)
		stmt.EXPECT().Exec()
		c.EXPECT().Upsert(txC, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt)

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})

		err := store.Replace([]any{testObject}, testObject.Id)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with DB client WithTransaction returning error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with DB client deleteAllStmt error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)

		deleteAllStmt := NewMockStmt(gomock.NewController(t))

		txC.EXPECT().Stmt(store.deleteAllStmt).Return(deleteAllStmt)
		deleteAllStmt.EXPECT().Exec().Return(nil, fmt.Errorf("error"))

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})

		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with DB client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		deleteAllStmt := NewMockStmt(gomock.NewController(t))

		txC.EXPECT().Stmt(store.deleteAllStmt).Return(deleteAllStmt)
		deleteAllStmt.EXPECT().Exec()
		c.EXPECT().Upsert(txC, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt).Return(fmt.Errorf("error"))

		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err == nil {
					t.Fail()
				}
			})

		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Resync is meaningless in the terms appearing here but has
// meaning in some implementations that have non-trivial
// additional behavior (e.g., DeltaFIFO).
func TestResync(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "Resync shouldn't call the client, panic, or do anything else", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		err := store.Resync()
		assert.Nil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

type StringMatcher struct {
	expected string
}

var ptn = regexp.MustCompile(`\s\s+`)

func dropWhiteSpace(s string) string {
	s1 := strings.TrimSpace(s)
	s2 := strings.ReplaceAll(s1, "\n", " ")
	s3 := strings.ReplaceAll(s2, "\r", " ")
	return ptn.ReplaceAllString(s3, " ")
}

func (m StringMatcher) Matches(x any) bool {
	s, ok := x.(string)
	if !ok {
		return false
	}
	return dropWhiteSpace(s) == m.expected
}

func (m StringMatcher) String() string {
	return m.expected
}

func WSIgnoringMatcher(expected string) gomock.Matcher {
	return StringMatcher{
		expected: dropWhiteSpace(expected),
	}
}

func TestAddWithExternalUpdates(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	testObject := testStoreObject{Id: "testStoreObject", Val: "a"}
	var tests []testCase
	tests = append(tests, testCase{description: "Add with no DB client errors", test: func(t *testing.T) {
		c, txC := SetupMockDB(t)
		stmts := NewMockStmt(gomock.NewController(t))
		store := SetupStoreWithExternalDependencies(t, c, true, false)

		c.EXPECT().Upsert(txC, store.upsertStmt, "testStoreObject", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			}).Times(2)
		rawStmt := `SELECT DISTINCT f.key, ex2."spec.displayName" FROM "_v1_Namespace_fields" f
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON f.key = lt1.key
  JOIN "management.cattle.io_v3_Project_fields" ex2 ON lt1.value = ex2."metadata.name"
  WHERE lt1.label = ? AND f."spec.displayName" != ex2."spec.displayName"`
		c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt))
		results1 := []any{"field.cattle.io/projectId"}
		c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results1)
		c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields1", "moose1"}}, nil)
		rawStmt2 := `UPDATE "_v1_Namespace_fields" SET "spec.displayName" = ? WHERE key = ?`
		c.EXPECT().Prepare(rawStmt2)
		txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
		stmts.EXPECT().Exec("moose1", "lego.cattle.io/fields1")

		rawStmt3 := `SELECT f.key, ex2."spec.projectName" FROM "_v1_Pods_fields" f
  JOIN "provisioner.cattle.io_v3_Cluster_fields" ex2 ON f."field.cattle.io/fixer" = ex2."metadata.name"
  WHERE f."spec.projectName" != ex2."spec.projectName"`
		c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt3))
		results2 := []any{"field.cattle.io/fixer"}
		c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results2)

		c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields2", "moose2"}}, nil)
		rawStmt4 := `UPDATE "_v1_Pods_fields" SET "spec.projectName" = ? WHERE key = ?`
		c.EXPECT().Prepare(rawStmt4)
		txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
		stmts.EXPECT().Exec("moose2", "lego.cattle.io/fields2")

		err := store.Add(testObject)
		assert.Nil(t, err)
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			test.test(t)
		})
	}
}

func TestAddWithSelfUpdates(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	testObject := testStoreObject{Id: "testStoreObject", Val: "a"}
	var tests []testCase
	tests = append(tests, testCase{description: "Add with no DB client errors", test: func(t *testing.T) {
		c, txC := SetupMockDB(t)
		stmts := NewMockStmt(gomock.NewController(t))
		store := SetupStoreWithExternalDependencies(t, c, false, true)

		c.EXPECT().Upsert(txC, store.upsertStmt, "testStoreObject", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			}).Times(2)
		rawStmt := `SELECT DISTINCT f.key, ex2."spec.clusterName" FROM "_v1_Namespace_fields" f
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON f.key = lt1.key
  JOIN "management.cattle.io_v3_Project_fields" ex2 ON lt1.value = ex2."metadata.name"
  WHERE lt1.label = ? AND f."spec.clusterName" != ex2."spec.clusterName"`
		c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt))
		results1 := []any{testObject.Id, "field.cattle.io/projectId"}
		c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results1)
		c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields1", "moose1"}}, nil)
		rawStmt2 := `UPDATE "_v1_Namespace_fields" SET "spec.clusterName" = ? WHERE key = ?`
		c.EXPECT().Prepare(rawStmt2)
		txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
		stmts.EXPECT().Exec("moose1", "lego.cattle.io/fields1")

		rawStmt3 := `SELECT f.key, ex2."spec.projectName" FROM "_v1_Pods_fields" f
  JOIN "provisioner.cattle.io_v3_Cluster_fields" ex2 ON f."field.cattle.io/fixer" = ex2."metadata.name"
  WHERE f."spec.projectName" != ex2."spec.projectName"`
		c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt3))
		results2 := []any{testObject.Id, "field.cattle.io/fixer"}
		c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results2)

		c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields2", "moose2"}}, nil)
		rawStmt4 := `UPDATE "_v1_Pods_fields" SET "spec.projectName" = ? WHERE key = ?`
		c.EXPECT().Prepare(rawStmt4)
		txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
		stmts.EXPECT().Exec("moose2", "lego.cattle.io/fields2")

		err := store.Add(testObject)
		assert.Nil(t, err)
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			test.test(t)
		})
	}
}

func TestAddWithBothUpdates(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}
	testObject := testStoreObject{Id: "testStoreObject", Val: "a"}
	var tests []testCase
	tests = append(tests, testCase{description: "Add with no DB client errors", test: func(t *testing.T) {
		c, txC := SetupMockDB(t)
		stmts := NewMockStmt(gomock.NewController(t))
		store := SetupStoreWithExternalDependencies(t, c, true, true)

		rawStmt := `SELECT DISTINCT f.key, ex2."spec.clusterName" FROM "_v1_Namespace_fields" f
  LEFT OUTER JOIN "_v1_Namespace_labels" lt1 ON f.key = lt1.key
  JOIN "management.cattle.io_v3_Project_fields" ex2 ON lt1.value = ex2."metadata.name"
  WHERE lt1.label = ? AND f."spec.clusterName" != ex2."spec.clusterName"`
		rawStmt3 := `SELECT f.key, ex2."spec.projectName" FROM "_v1_Pods_fields" f
  JOIN "provisioner.cattle.io_v3_Cluster_fields" ex2 ON f."field.cattle.io/fixer" = ex2."metadata.name"
  WHERE f."spec.projectName" != ex2."spec.projectName"`

		c.EXPECT().Upsert(txC, store.upsertStmt, "testStoreObject", testObject, store.shouldEncrypt).Return(nil)
		c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txC)
				if err != nil {
					t.Fail()
				}
			})
		for _ = range 2 {
			c.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
				func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
					err := f(txC)
					if err != nil {
						t.Fail()
					}
				})
			c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt))
			results1 := []any{testObject.Id, "field.cattle.io/projectId"}
			c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results1)
			c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields1", "moose1"}}, nil)
			rawStmt2 := `UPDATE "_v1_Namespace_fields" SET "spec.clusterName" = ? WHERE key = ?`
			c.EXPECT().Prepare(rawStmt2)
			txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
			stmts.EXPECT().Exec("moose1", "lego.cattle.io/fields1")

			c.EXPECT().Prepare(WSIgnoringMatcher(rawStmt3))
			results2 := []any{testObject.Id, "field.cattle.io/fixer"}
			c.EXPECT().QueryForRows(gomock.Any(), gomock.Any(), results2)

			c.EXPECT().ReadStrings2(gomock.Any()).Return([][]string{{"lego.cattle.io/fields2", "moose2"}}, nil)
			rawStmt4 := `UPDATE "_v1_Pods_fields" SET "spec.projectName" = ? WHERE key = ?`
			c.EXPECT().Prepare(rawStmt4)
			txC.EXPECT().Stmt(gomock.Any()).Return(stmts)
			stmts.EXPECT().Exec("moose2", "lego.cattle.io/fields2")
			// And again for the other object
		}

		err := store.Add(testObject)
		assert.Nil(t, err)
	},
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			test.test(t)
		})
	}
}

func SetupMockDB(t *testing.T) (*MockClient, *MockTXClient) {
	dbC := NewMockClient(gomock.NewController(t)) // add functionality once store expectation are known
	txC := NewMockTXClient(gomock.NewController(t))
	txC.EXPECT().Exec(fmt.Sprintf(createTableFmt, "testStoreObject")).Return(nil, nil)
	dbC.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
		func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
			err := f(txC)
			if err != nil {
				t.Fail()
			}
		})

	// use stmt mock here
	dbC.EXPECT().Prepare(fmt.Sprintf(upsertStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(deleteStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(deleteAllStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(getStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(listStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(listKeysStmtFmt, "testStoreObject")).Return(&sql.Stmt{})

	return dbC, txC
}
func SetupStore(t *testing.T, client *MockClient, shouldEncrypt bool) *Store {
	name := "testStoreObject"
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: name}
	store, err := NewStore(context.Background(), testStoreObject{}, testStoreKeyFunc, client, shouldEncrypt, gvk, name, nil, nil)
	if err != nil {
		t.Error(err)
	}
	return store
}

func gvkKey(group, version, kind string) string {
	return group + "_" + version + "_" + kind
}

func SetupStoreWithExternalDependencies(t *testing.T, client *MockClient, updateExternal bool, updateSelf bool) *Store {
	name := "testStoreObject"
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: name}
	namespaceProjectLabelDep := sqltypes.ExternalLabelDependency{
		SourceGVK:            gvkKey("", "v1", "Namespace"),
		SourceLabelName:      "field.cattle.io/projectId",
		TargetGVK:            gvkKey("management.cattle.io", "v3", "Project"),
		TargetKeyFieldName:   "metadata.name",
		TargetFinalFieldName: "spec.displayName",
	}
	namespaceNonLabelDep := sqltypes.ExternalDependency{
		SourceGVK:            gvkKey("", "v1", "Pods"),
		SourceFieldName:      "field.cattle.io/fixer",
		TargetGVK:            gvkKey("provisioner.cattle.io", "v3", "Cluster"),
		TargetKeyFieldName:   "metadata.name",
		TargetFinalFieldName: "spec.projectName",
	}
	updateInfo := sqltypes.ExternalGVKUpdates{
		AffectedGVK:               schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
		ExternalDependencies:      []sqltypes.ExternalDependency{namespaceNonLabelDep},
		ExternalLabelDependencies: []sqltypes.ExternalLabelDependency{namespaceProjectLabelDep},
	}
	externalUpdateInfo := &updateInfo
	selfUpdateInfo := &updateInfo
	if !updateExternal {
		externalUpdateInfo = nil
	}
	if !updateSelf {
		selfUpdateInfo = nil
	}
	store, err := NewStore(context.Background(), testStoreObject{}, testStoreKeyFunc, client, false, gvk, name, externalUpdateInfo, selfUpdateInfo)
	if err != nil {
		t.Error(err)
	}
	return store
}
