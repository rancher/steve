package factory

import (
	"context"
	"fmt"
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

//go:generate go tool -modfile ../../../../gotools/mockgen/go.mod mockgen --build_flags=--mod=mod -package factory -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Client,TxClient
//go:generate go tool -modfile ../../../../gotools/mockgen/go.mod mockgen --build_flags=--mod=mod -package factory -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface
//go:generate go tool -modfile ../../../../gotools/mockgen/go.mod mockgen --build_flags=--mod=mod -package factory -destination ./k8s_cache_mocks_test.go k8s.io/client-go/tools/cache SharedIndexInformer
//go:generate go tool -modfile ../../../../gotools/mockgen/go.mod mockgen --build_flags=--mod=mod -package factory -destination ./sql_informer_mocks_test.go github.com/rancher/steve/pkg/sqlcache/informer ByOptionsLister

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

	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and ctx not canceled, should return no error and should call Informer.Run(). A subsequent call to CacheFor() should return same informer", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true).AnyTimes()
		sii.EXPECT().Run(gomock.Any()).MinTimes(1)
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			// this function ensures that ctx is open for the duration of this test but if part of a longer process it will be closed eventually
			time.Sleep(5 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		// this sleep is critical to the test. It ensure there has been enough time for expected function like Run to be invoked in their go routines.
		time.Sleep(1 * time.Second)
		c2, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, c, c2)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning false, and ctx not canceled, should call Run() and return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}

		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(false).AnyTimes()
		sii.EXPECT().Run(gomock.Any())
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		expectedI := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return expectedI, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			time.Sleep(1 * time.Second)
			f.Stop(expectedGVK)
		}()
		_, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.NotNil(t, err)
		time.Sleep(2 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning false, request is canceled", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}

		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(false).AnyTimes()
		sii.EXPECT().Run(gomock.Any())
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		expectedI := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return expectedI, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
			defer cancel()
			_, err := f.CacheFor(ctx, fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
			errCh <- err
		}()

		select {
		case err := <-errCh:
			assert.NotNil(t, err)
		case <-time.After(2 * time.Second):
			assert.Fail(t, "CacheFor never exited")
		}
		time.Sleep(2 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and ctx is canceled, should not call Run() more than once and not return an error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}

		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true).AnyTimes()
		// may or may not call run initially
		sii.EXPECT().Run(gomock.Any()).MaxTimes(1)
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())
		f.Stop(expectedGVK)

		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned and encryptAll set to true, should return no error and pass shouldEncrypt as true to newInformer func", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			encryptAll:  true,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			time.Sleep(10 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})

	tests = append(tests, testCase{description: "CacheFor() should encrypt v1 Secrets", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Secret",
		}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			encryptAll:  false,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			time.Sleep(10 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() should encrypt management.cattle.io tokens", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{
			Group:   "management.cattle.io",
			Version: "v3",
			Kind:    "Token",
		}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			encryptAll:  false,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			time.Sleep(10 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})

	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, ctx not canceled, and transform func should return no error", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
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
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
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
			assert.Equal(t, 0, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			// this function ensures that ctx is not canceled for the duration of this test but if part of a longer process it will be closed eventually
			time.Sleep(5 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, transformFunc, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with default max events count", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}
		bloi := NewMockByOptionsLister(gomock.NewController(t))
		bloi.EXPECT().DropAll(gomock.Any()).AnyTimes()
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		sii.EXPECT().SetWatchErrorHandler(gomock.Any())
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
			ByOptionsLister:     bloi,
		}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			assert.Equal(t, 10, gcKeepCount)
			assert.Nil(t, externalUpdateInfo)
			return i, nil
		}
		f := &CacheFactory{
			gcKeepCount: 10,
			dbClient:    dbClient,
			newInformer: testNewInformer,
			encryptAll:  true,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())

		go func() {
			time.Sleep(10 * time.Second)
			f.Stop(expectedGVK)
		}()
		c, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Nil(t, err)
		assert.Equal(t, i, c.ByOptionsLister)
		assert.Equal(t, expectedGVK, c.gvk)
		assert.NotNil(t, c.ctx)
		time.Sleep(1 * time.Second)
	}})
	// Test for panic from https://github.com/rancher/rancher/issues/52124
	tests = append(tests, testCase{description: "CacheFor() able to stop cache with a nil gi.informer", test: func(t *testing.T) {
		dbClient := NewMockClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		field := &informer.JSONPathField{Path: []string{"something"}}
		fields := map[string]informer.IndexedField{field.ColumnName(): field}
		typeGuidance := map[string]string{}
		expectedGVK := schema.GroupVersionKind{}
		testNewInformer := func(ctx context.Context, client dynamic.ResourceInterface, fields map[string]informer.IndexedField, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, typeGuidance map[string]string, namespaced bool, watchable bool, gcKeepCount int) (*informer.Informer, error) {
			return nil, fmt.Errorf("fake error")
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			newInformer: testNewInformer,
			encryptAll:  true,
			informers:   map[schema.GroupVersionKind]*guardedInformer{},
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())
		_, err := f.CacheFor(context.Background(), fields, nil, nil, nil, dynamicClient, expectedGVK, typeGuidance, false, true)
		assert.Error(t, err)
		err = f.Stop(expectedGVK)
		assert.NoError(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
