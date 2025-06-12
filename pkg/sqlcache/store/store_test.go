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
	"reflect"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/db/transaction"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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
	store, err := NewStore(context.Background(), testStoreObject{}, testStoreKeyFunc, client, shouldEncrypt, "testStoreObject")
	if err != nil {
		t.Error(err)
	}
	return store
}
