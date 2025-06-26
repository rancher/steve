package factory

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

//go:generate go run go.uber.org/mock/mockgen@latest --build_flags=--mod=mod -package factory -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Client
//go:generate go run go.uber.org/mock/mockgen@latest --build_flags=--mod=mod -package factory -destination ./transaction_mocks_tests.go -mock_names Client=MockTXClient github.com/rancher/steve/pkg/sqlcache/db/transaction Client
//go:generate go run go.uber.org/mock/mockgen@latest --build_flags=--mod=mod -package factory -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface
//go:generate go run go.uber.org/mock/mockgen@latest --build_flags=--mod=mod -package factory -destination ./k8s_cache_mocks_test.go k8s.io/client-go/tools/cache SharedIndexInformer

func TestNewCacheFactory(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "NewCacheFactory() with no errors returned, should return no errors", test: func(t *testing.T) {
		f, err := NewCacheFactory(CacheFactoryOptions{})
		assert.Nil(t, err)
		assert.NotNil(t, f.dbClient)
		assert.False(t, f.encryptAll)
	}})
	tests = append(tests, testCase{description: "NewCacheFactory() with no errors returned and EncryptAllEnvVar set to true, should return no errors and have encryptAll set to true", test: func(t *testing.T) {
		err := os.Setenv(EncryptAllEnvVar, "true")
		assert.Nil(t, err)
		f, err := NewCacheFactory(CacheFactoryOptions{})
		assert.Nil(t, err)
		assert.Nil(t, err)
		assert.NotNil(t, f.dbClient)
		assert.True(t, f.encryptAll)
	}})
	// cannot run as parallel because tests involve changing env var
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestCacheFor(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and stopCh not closed, should return no error and should call Informer.Run(). A subsequent call to CacheFor() should return same informer", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true).AnyTimes()
		sii.EXPECT().Run(gomock.Any()).MinTimes(1)
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			// this function ensures that stopCh is open for the duration of this test but if part of a longer process it will be closed eventually
			time.Sleep(5 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		// this sleep is critical to the test. It ensure there has been enough time for expected function like Run to be invoked in their go routines.
		time.Sleep(1 * time.Second)
		c2, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, c, c2)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning false, and stopCh not closed, should call Run() and return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}

		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(false).AnyTimes()
		sii.EXPECT().Run(gomock.Any())
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		expectedI := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return expectedI, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(1 * time.Second)
			close(f.stopCh)
		}()
		var err error
		_, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.NotNil(t, err)
		time.Sleep(2 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and stopCh closed, should not call Run() more than once and not return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}

		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true).AnyTimes()
		// may or may not call run initially
		sii.EXPECT().Run(gomock.Any()).MaxTimes(1)
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		close(f.stopCh)
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned and encryptAll set to true, should return no error and pass shouldEncrypt as true to newInformer func", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			encryptAll:  true,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})

	tests = append(tests, testCase{description: "CacheFor() should encrypt v1 Secrets", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Secret",
		}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			encryptAll:  false,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() should encrypt management.cattle.io tokens", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{
			Group:   "management.cattle.io",
			Version: "v3",
			Kind:    "Token",
		}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			encryptAll:  false,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})

	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, stopCh not closed, and transform func should return no error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1)
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		transformFunc := func(input interface{}) (interface{}, error) {
			return "someoutput", nil
		}
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			// we can't test func == func, so instead we check if the output was as expected
			input := "someinput"
			ouput, err := transform(input)
			assert.Nil(t, err)
			outputStr, ok := ouput.(string)
			assert.True(t, ok, "ouput from transform was expected to be a string")
			assert.Equal(t, "someoutput", outputStr)

			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			// this function ensures that stopCh is open for the duration of this test but if part of a longer process it will be closed eventually
			time.Sleep(5 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, transformFunc, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with default max events count", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 10, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			defaultMaximumEventsCount: 10,
			dbClient:                  dbClient,
			stopCh:                    make(chan struct{}),
			newInformer:               testNewInformer,
			encryptAll:                true,
			informers:                 map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		// CacheFor(ctx context.Context, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool, watchable bool)
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with per GVK maximum events count", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{
			Group:   "management.cattle.io",
			Version: "v3",
			Kind:    "Token",
		}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt, namespaced bool, watchable bool, maxEventsCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 10, maxEventsCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			defaultMaximumEventsCount: 5,
			perGVKMaximumEventsCount: map[schema.GroupVersionKind]int{
				expectedGVK: 10,
			},
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			encryptAll:  true,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, false, true)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}