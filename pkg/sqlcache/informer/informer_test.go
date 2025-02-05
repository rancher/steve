package informer

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./informer_mocks_test.go github.com/rancher/steve/pkg/sqlcache/informer ByOptionsLister
//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface

func TestNewInformer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "NewInformer() with no errors returned, should return no error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		// NewListOptionIndexer() logic. This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		informer, err := NewInformer(dynamicClient, fields, nil, gvk, dbClient, false, true)
		assert.Nil(t, err)
		assert.NotNil(t, informer.ByOptionsLister)
		assert.NotNil(t, informer.SharedIndexInformer)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewStore(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		_, err := NewInformer(dynamicClient, fields, nil, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		_, err := NewInformer(dynamicClient, fields, nil, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewListOptionIndexer(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		// NewListOptionIndexer() logic. This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(fmt.Errorf("error")).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		_, err := NewInformer(dynamicClient, fields, nil, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewInformer() with transform func", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		mockInformer := mockInformer{}
		testNewInformer := func(lw cache.ListerWatcher,
			exampleObject runtime.Object,
			defaultEventHandlerResyncPeriod time.Duration,
			indexers cache.Indexers) cache.SharedIndexInformer {
			return &mockInformer
		}
		newInformer = testNewInformer

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		// NewListOptionIndexer() logic. This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		dbClient.EXPECT().WithTransaction(gomock.Any(), true, gomock.Any()).Return(nil).Do(
			func(ctx context.Context, shouldEncrypt bool, f db.WithTransactionFunction) {
				err := f(txClient)
				if err != nil {
					t.Fail()
				}
			})

		transformFunc := func(input interface{}) (interface{}, error) {
			return "someoutput", nil
		}
		informer, err := NewInformer(dynamicClient, fields, transformFunc, gvk, dbClient, false, true)
		assert.Nil(t, err)
		assert.NotNil(t, informer.ByOptionsLister)
		assert.NotNil(t, informer.SharedIndexInformer)
		assert.NotNil(t, mockInformer.transformFunc)

		// we can't test func == func, so instead we check if the output was as expected
		input := "someinput"
		ouput, err := mockInformer.transformFunc(input)
		assert.Nil(t, err)
		outputStr, ok := ouput.(string)
		assert.True(t, ok, "ouput from transform was expected to be a string")
		assert.Equal(t, "someoutput", outputStr)

		newInformer = cache.NewSharedIndexInformer
	}})
	tests = append(tests, testCase{description: "NewInformer() unable to set transform func", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		mockInformer := mockInformer{
			setTranformErr: fmt.Errorf("some error"),
		}
		testNewInformer := func(lw cache.ListerWatcher,
			exampleObject runtime.Object,
			defaultEventHandlerResyncPeriod time.Duration,
			indexers cache.Indexers) cache.SharedIndexInformer {
			return &mockInformer
		}
		newInformer = testNewInformer

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		transformFunc := func(input interface{}) (interface{}, error) {
			return "someoutput", nil
		}
		_, err := NewInformer(dynamicClient, fields, transformFunc, gvk, dbClient, false, true)
		assert.Error(t, err)
		newInformer = cache.NewSharedIndexInformer
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestInformerListByOptions(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "ListByOptions() with no errors returned, should return no error and return value from indexer's ListByOptions()", test: func(t *testing.T) {
		indexer := NewMockByOptionsLister(gomock.NewController(t))
		informer := &Informer{
			ByOptionsLister: indexer,
		}
		lo := ListOptions{}
		var partitions []partition.Partition
		ns := "somens"
		expectedList := &unstructured.UnstructuredList{
			Object: map[string]interface{}{"s": 2},
			Items: []unstructured.Unstructured{{
				Object: map[string]interface{}{"s": 2},
			}},
		}
		expectedTotal := len(expectedList.Items)
		expectedContinueToken := "123"
		indexer.EXPECT().ListByOptions(context.TODO(), lo, partitions, ns).Return(expectedList, expectedTotal, expectedContinueToken, nil)
		list, total, continueToken, err := informer.ListByOptions(context.TODO(), lo, partitions, ns)
		assert.Nil(t, err)
		assert.Equal(t, expectedList, list)
		assert.Equal(t, len(expectedList.Items), total)
		assert.Equal(t, expectedContinueToken, continueToken)
	}})
	tests = append(tests, testCase{description: "ListByOptions() with indexer ListByOptions error, should return error", test: func(t *testing.T) {
		indexer := NewMockByOptionsLister(gomock.NewController(t))
		informer := &Informer{
			ByOptionsLister: indexer,
		}
		lo := ListOptions{}
		var partitions []partition.Partition
		ns := "somens"
		indexer.EXPECT().ListByOptions(context.TODO(), lo, partitions, ns).Return(nil, 0, "", fmt.Errorf("error"))
		_, _, _, err := informer.ListByOptions(context.TODO(), lo, partitions, ns)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

// Note: SQLite based caching uses an Informer that unsafely sets the Indexer as the ability to set it is not present
// in client-go at the moment. Long term, we look forward contribute a patch to client-go to make that configurable.
// Until then, we are adding this canary test that will panic in case the indexer cannot be set.
func TestUnsafeSet(t *testing.T) {
	listWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return &unstructured.UnstructuredList{}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return dummyWatch{}, nil
		},
	}

	sii := cache.NewSharedIndexInformer(listWatcher, &unstructured.Unstructured{}, 0, cache.Indexers{})

	// will panic if SharedIndexInformer stops having a *Indexer field called "indexer"
	UnsafeSet(sii, "indexer", &Indexer{})
}

type dummyWatch struct{}

func (dummyWatch) Stop() {
}

func (dummyWatch) ResultChan() <-chan watch.Event {
	result := make(chan watch.Event)
	defer close(result)
	return result
}

// mockInformer is a mock of cache.SharedIndexInformer. Unlike other types, we can't generate this using mockgen because we use a unsafeSet to replace the
// indexer field, which is a struct field. This won't exist on the mock, producing an error. So we need to implement our own mock which actually has this field.
type mockInformer struct {
	transformFunc  cache.TransformFunc
	setTranformErr error
	indexer        cache.Indexer
}

func (m *mockInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}
func (m *mockInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}
func (m *mockInformer) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}
func (m *mockInformer) GetStore() cache.Store                                      { return nil }
func (m *mockInformer) GetController() cache.Controller                            { return nil }
func (m *mockInformer) Run(stopCh <-chan struct{})                                 {}
func (m *mockInformer) HasSynced() bool                                            { return false }
func (m *mockInformer) LastSyncResourceVersion() string                            { return "" }
func (m *mockInformer) SetWatchErrorHandler(handler cache.WatchErrorHandler) error { return nil }
func (m *mockInformer) IsStopped() bool                                            { return false }
func (m *mockInformer) AddIndexers(indexers cache.Indexers) error                  { return nil }
func (m *mockInformer) GetIndexer() cache.Indexer                                  { return nil }
func (m *mockInformer) SetTransform(handler cache.TransformFunc) error {
	m.transformFunc = handler
	return m.setTranformErr
}
