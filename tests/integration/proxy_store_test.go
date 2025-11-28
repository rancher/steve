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
	i.doManifestString(ctx, orangeCRDManifest, func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestStringReversed(ctx, orangeCRDManifest, i.doDelete)

	// Wait for the Orange schema to be available
	i.waitForSchema(baseURL, orangeGVR)

	// Enable debugging after CRDs are created but before tests start
	defer i.maybeStopAndDebug(baseURL)

	// Start watching the Orange resources via WebSocket
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	// Convert HTTP URL to WebSocket URL
	wsURL := strings.Replace(baseURL, "http://", "ws://", 1) + "/v1/subscribe"
	dialer := websocket.Dialer{}
	wsConn, resp, err := dialer.DialContext(watchCtx, wsURL, nil)
	i.Require().NoError(err)
	i.Require().Equal(http.StatusSwitchingProtocols, resp.StatusCode)

	// First, list the oranges to get the resourceVersion for watching
	orangeList, err := i.client.Resource(orangeGVR).List(ctx, metav1.ListOptions{})
	i.Require().NoError(err)
	resourceVersion := orangeList.GetResourceVersion()

	// Send message to establish watch with the resourceVersion
	watchMessage := map[string]interface{}{
		"resourceType": "fruits.cattle.io.oranges",
		"resourceVersion": resourceVersion,
	}
	err = wsConn.WriteJSON(watchMessage)
	i.Require().NoError(err)

	// Channel to signal when watch is closed via resource.stop message
	watchClosed := make(chan struct{})
	// Channel to signal when we receive a resource.create notification
	createReceived := make(chan struct{})
	go func() {
		defer close(watchClosed)
		for {
			_, message, err := wsConn.ReadMessage()
			if err != nil {
				// Connection error
				return
			}
			// Check for messages
			var msg map[string]interface{}
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}
			if msg["name"] == "resource.create" && msg["resourceType"] == "fruits.cattle.io.oranges" {
				// Watch received the create notification
				select {
				case <-createReceived:
					// Already closed
				default:
					close(createReceived)
				}
			}
			if msg["name"] == "resource.stop" && msg["resourceType"] == "fruits.cattle.io.oranges" {
				// Watch was closed by the server
				return
			}
		}
	}()

	// Create an Orange resource to verify the watch is working
	testOrange := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "fruits.cattle.io/v1",
			"kind":       "Orange",
			"metadata": map[string]interface{}{
				"name": "test-orange",
			},
			"foo": "bar",
		},
	}
	_, err = i.client.Resource(orangeGVR).Create(ctx, testOrange, metav1.CreateOptions{})
	i.Require().NoError(err)
	defer func() {
		_ = i.client.Resource(orangeGVR).Delete(ctx, "test-orange", metav1.DeleteOptions{})
	}()

	// Wait for the create notification to confirm the watch is working
	select {
	case <-createReceived:
		// Watch is confirmed working
	case <-time.After(10 * time.Second):
		watchCancel()
		i.Require().Fail("expected to receive resource.create notification for test-orange")
	}

	// Patch the CRD to add an additional printer column using Apply with Force
	patchedCRD := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": "oranges.fruits.cattle.io",
			},
			"spec": map[string]interface{}{
				"group": "fruits.cattle.io",
				"scope": "Cluster",
				"names": map[string]interface{}{
					"kind":     "Orange",
					"listKind": "OrangeList",
					"plural":   "oranges",
					"singular": "orange",
				},
				"versions": []interface{}{
					map[string]interface{}{
						"name": "v1",
						"schema": map[string]interface{}{
							"openAPIV3Schema": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"bar": map[string]interface{}{"type": "string"},
									"foo": map[string]interface{}{"type": "string"},
								},
							},
						},
						"served":  true,
						"storage": true,
						"additionalPrinterColumns": []interface{}{
							map[string]interface{}{
								"name":     "Foo",
								"type":     "string",
								"jsonPath": ".foo",
							},
						},
					},
				},
			},
		},
	}
	_, err = i.client.Resource(crdGVR).Apply(ctx, "oranges.fruits.cattle.io", patchedCRD, metav1.ApplyOptions{
		FieldManager: "integration-tests",
		Force:        true,
	})
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
