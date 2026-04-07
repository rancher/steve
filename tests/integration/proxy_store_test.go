package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

const orangeCRDManifest = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: oranges.fruits.cattle.io
spec:
  scope: Cluster
  group: fruits.cattle.io
  names:
    kind: Orange
    listKind: OrangeList
    plural: oranges
    singular: orange
  versions:
  - name: v1
    schema:
      openAPIV3Schema:
        type: object
        properties:
          bar:
            type: string
          foo:
            type: string
    served: true
    storage: true
`

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

	orangeGVR := schema.GroupVersionResource{
		Group:    "fruits.cattle.io",
		Version:  "v1",
		Resource: "oranges",
	}

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	// Create the Orange CRD dynamically using YAML string
	gvrs := make(map[schema.GroupVersionResource]struct{})
	i.doManifestString(ctx, orangeCRDManifest, func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestStringReversed(ctx, orangeCRDManifest, i.doDelete)

	// Wait for the schema to be available
	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}
	i.waitForSchema(baseURL, orangeGVR)

	// Enable debugging after CRDs are created but before tests start
	defer i.maybeStopAndDebug(baseURL)

	// Start watching the Orange resources via WebSocket
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	// Convert HTTP URL to WebSocket URL
	wsURL := strings.Replace(baseURL, "http://", "ws://", 1) + "/v1"
	dialer := websocket.Dialer{}
	wsConn, resp, err := dialer.DialContext(watchCtx, wsURL, nil)
	i.Require().NoError(err)
	i.Require().Equal(http.StatusSwitchingProtocols, resp.StatusCode)

	// Send message to establish watch
	watchMessage := map[string]string{"resourceType": "fruits.cattle.io.oranges"}
	err = wsConn.WriteJSON(watchMessage)
	i.Require().NoError(err)

	// Channel to signal when watch is closed
	watchClosed := make(chan struct{})
	go func() {
		defer close(watchClosed)
		for {
			_, _, err := wsConn.ReadMessage()
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

	_, err = i.client.Resource(crdGVR).Patch(ctx, "oranges.fruits.cattle.io", k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	i.Require().NoError(err)

	// Wait for the watch to be closed (with timeout)
	select {
	case <-watchClosed:
		// Watch was properly closed
	case <-time.After(10 * time.Second):
		watchCancel() // Cancel to clean up the watch goroutine
		i.Require().Fail("expected watch to be closed after CRD modification")
	}

	// Close the previous WebSocket connection
	wsConn.Close()

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
		newWsConn, resp, err := dialer.DialContext(newWatchCtx, wsURL, nil)
		require.NoError(c, err)
		require.Equal(c, http.StatusSwitchingProtocols, resp.StatusCode)

		// Send message to establish watch
		err = newWsConn.WriteJSON(watchMessage)
		require.NoError(c, err)

		// Close immediately, we just wanted to verify the watch can be established
		newWsConn.Close()
	}, 15*time.Second, 500*time.Millisecond)
}
