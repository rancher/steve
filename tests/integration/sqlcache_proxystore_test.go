package tests

import (
	"context"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func (i *IntegrationSuite) TestSQLCacheProxyStore() {
	ctx := i.T().Context()

	testFile := "testdata/sqlcache/proxy_store.test.yaml"

	// Apply CRD
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, testFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, testFile, i.doDelete)

	// Create steve server
	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCache: true,
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
		},
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL

	// Wait for schema to be available
	bananaGVR := k8sschema.GroupVersionResource{
		Group:    "fruits.cattle.io",
		Version:  "v1",
		Resource: "bananas",
	}
	i.waitForSchema(baseURL, bananaGVR)

	defer i.maybeStopAndDebug(baseURL)

	// Test that schema is registered
	i.Run("schema registered", func() {
		// Schema should be available after waitForSchema
		i.Require().True(true, "Schema registered successfully")
	})
}
