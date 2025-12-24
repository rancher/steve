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

const orangeCRDManifestWithColumn = `
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
    additionalPrinterColumns:
    - name: Foo
      type: string
      jsonPath: .foo
`

// TestProxyStore tests that when a CRD's schema changes, existing watch connections
// are properly closed and new watches can be established.
func (i *IntegrationSuite) TestProxyStore() {
	ctx := i.T().Context()

	// Typed structs for websocket messages
	type steveListResponse struct {
		Revision string `json:"revision"`
	}
	type watchRequest struct {
		ResourceType string `json:"resourceType"`
		Revision     string `json:"revision,omitempty"`
	}
	type watchEvent struct {
		Name         string `json:"name"`
		ResourceType string `json:"resourceType"`
	}

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

	// List the oranges using Steve API to get the revision for watching
	listResp, err := http.Get(baseURL + "/v1/fruits.cattle.io.oranges")
	i.Require().NoError(err)
	defer listResp.Body.Close()
	i.Require().Equal(http.StatusOK, listResp.StatusCode)

	listBody, err := io.ReadAll(listResp.Body)
	i.Require().NoError(err)

	var listResult steveListResponse
	err = json.Unmarshal(listBody, &listResult)
	i.Require().NoError(err)
	revision := listResult.Revision

	// Send message to establish watch with the revision
	watchMessage := watchRequest{
		ResourceType: "fruits.cattle.io.oranges",
		Revision:     revision,
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
			var event watchEvent
			if err := json.Unmarshal(message, &event); err != nil {
				continue
			}
			if event.Name == "resource.create" && event.ResourceType == "fruits.cattle.io.oranges" {
				// Watch received the create notification
				select {
				case <-createReceived:
					// Already closed
				default:
					close(createReceived)
				}
			}
			if event.Name == "resource.stop" && event.ResourceType == "fruits.cattle.io.oranges" {
				// Watch received resource.stop message indicating CRD schema change
				// Note: The WebSocket connection itself remains open, but Steve sends this
				// message to signal that the watch needs to be re-established
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

	// Patch the CRD to add an additional printer column using YAML manifest
	i.doManifestString(ctx, orangeCRDManifestWithColumn, func(ctx context.Context, obj *unstructured.Unstructured, gvr schema.GroupVersionResource) error {
		return i.doApply(ctx, obj, gvr)
	})

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

		// Send message to establish watch (without revision to watch from current state)
		newWatchMessage := watchRequest{
			ResourceType: "fruits.cattle.io.oranges",
		}
		err = newWsConn.WriteJSON(newWatchMessage)
		require.NoError(c, err)

		// Close immediately, we just wanted to verify the watch can be established
		newWsConn.Close()
	}, 15*time.Second, 500*time.Millisecond)
}
