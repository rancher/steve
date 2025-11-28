package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

// TestProxyStore tests that when a CRD's schema changes, existing watch connections
// are properly closed and new watches can be established.
func (i *IntegrationSuite) TestProxyStore() {
	ctx := i.T().Context()

	// Create a Steve server with SQL cache enabled
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

	orangeGVK := schema.GroupVersionKind{
		Group:   "fruits.cattle.io",
		Version: "v1",
		Kind:    "Orange",
	}
	orangeGVR := schema.GroupVersionResource{
		Group:    "fruits.cattle.io",
		Version:  "v1",
		Resource: "oranges",
	}

	// Create the Orange CRD dynamically
	orangeCRD := &apiextensionsv1.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apiextensions.k8s.io/v1",
			Kind:       "CustomResourceDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "oranges.fruits.cattle.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Scope: "Cluster",
			Group: orangeGVK.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     orangeGVK.Kind,
				ListKind: "OrangeList",
				Plural:   "oranges",
				Singular: "orange",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name: orangeGVK.Version,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"bar": {
									Type: "string",
								},
								"foo": {
									Type: "string",
								},
							},
						},
					},
					Served:  true,
					Storage: true,
				},
			},
		},
	}

	// Create the CRD using the unstructured client
	crdUnstructured := &unstructured.Unstructured{}
	crdBytes, err := json.Marshal(orangeCRD)
	i.Require().NoError(err)
	err = json.Unmarshal(crdBytes, &crdUnstructured.Object)
	i.Require().NoError(err)

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	_, err = i.client.Resource(crdGVR).Create(ctx, crdUnstructured, metav1.CreateOptions{})
	i.Require().NoError(err)

	defer func() {
		err := i.client.Resource(crdGVR).Delete(ctx, orangeCRD.Name, metav1.DeleteOptions{})
		i.Require().NoError(err)
	}()

	// Wait for the schema to be available
	i.waitForSchema(baseURL, orangeGVR)

	// Start watching the Orange resources via SSE
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	watchURL := baseURL + "/v1/fruits.cattle.io.oranges"
	watchReq, err := http.NewRequestWithContext(watchCtx, "GET", watchURL, nil)
	i.Require().NoError(err)
	watchReq.Header.Set("Accept", "text/event-stream")

	watchResp, err := http.DefaultClient.Do(watchReq)
	i.Require().NoError(err)
	i.Require().Equal(http.StatusOK, watchResp.StatusCode)

	// Channel to signal when watch is closed
	watchClosed := make(chan struct{})
	go func() {
		defer close(watchClosed)
		reader := bufio.NewReader(watchResp.Body)
		for {
			_, err := reader.ReadBytes('\n')
			if err != nil {
				// Watch connection closed (expected when CRD is modified)
				return
			}
		}
	}()

	// Patch the CRD to add an additional printer column
	patch := []byte(`{
		"spec": {
			"versions": [{
				"name": "v1",
				"schema": {
					"openAPIV3Schema": {
						"type": "object",
						"properties": {
							"bar": {"type": "string"},
							"foo": {"type": "string"}
						}
					}
				},
				"served": true,
				"storage": true,
				"additionalPrinterColumns": [{
					"name": "Foo",
					"type": "string",
					"jsonPath": ".foo"
				}]
			}]
		}
	}`)

	_, err = i.client.Resource(crdGVR).Patch(ctx, orangeCRD.Name, k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	i.Require().NoError(err)

	// Wait for the watch to be closed (with timeout)
	select {
	case <-watchClosed:
		// Watch was properly closed
	case <-time.After(10 * time.Second):
		watchCancel() // Cancel to clean up the watch goroutine
		i.Require().Fail("expected watch to be closed after CRD modification")
	}

	// Close the previous watch response body
	watchResp.Body.Close()

	// Wait for schema to be updated with new columns
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		i.discoveryMapper.Reset()
		url := baseURL + "/v1/schemaDefinitions/fruits.cattle.io.oranges"
		resp, err := http.Get(url)
		require.NoError(c, err)
		defer resp.Body.Close()
		require.Equal(c, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(c, err)

		// Check that the schema contains the new column
		var schemaResp map[string]interface{}
		err = json.Unmarshal(body, &schemaResp)
		require.NoError(c, err)
	}, 15*time.Second, 500*time.Millisecond)

	// Verify that a new watch can be started after the schema reset
	newWatchCtx, newWatchCancel := context.WithCancel(ctx)
	defer newWatchCancel()

	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		newWatchReq, err := http.NewRequestWithContext(newWatchCtx, "GET", watchURL, nil)
		require.NoError(c, err)
		newWatchReq.Header.Set("Accept", "text/event-stream")

		newWatchResp, err := http.DefaultClient.Do(newWatchReq)
		require.NoError(c, err)
		require.Equal(c, http.StatusOK, newWatchResp.StatusCode)

		// Close immediately, we just wanted to verify the watch can be established
		newWatchResp.Body.Close()
	}, 15*time.Second, 500*time.Millisecond)

	defer i.maybeStopAndDebug(baseURL)
}
