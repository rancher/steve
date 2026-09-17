package tests

import (
	"context"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	carrotGVR = k8sschema.GroupVersionResource{
		Group:    "vegetables.cattle.io",
		Version:  "v1",
		Resource: "carrots",
	}
	carrotSchemaID = "vegetables.cattle.io.carrot"
)

// countSubscribeEvent is specific to COUNT events which have a different Data structure
type countSubscribeEvent struct {
	Name string                 `json:"name"`
	Data map[string]interface{} `json:"data"`
}

// TestCountWatchReceivesNewCRDResources is a regression test for the COUNT
// websocket bug where new CRDs installed after Steve starts don't trigger
// websocket updates to COUNT subscribers.
//
// Bug scenario:
// 1. Dashboard loads and establishes websocket watch on /v1/count
// 2. COUNT's Watch() builds static gvkToSchema map with existing schemas
// 3. New CRD is installed (e.g., rancher-backup operator's ResourceSet)
// 4. Resources of new type are created
// 5. COUNT's onChange callback ignores events for unknown GVK (not in static map)
// 6. Websocket clients never receive update about new resource type
// 7. Dashboard shows stale COUNT until hard refresh (fresh HTTP GET)
//
// This test proves the bug by:
// - Establishing COUNT websocket subscription with initial schemas
// - Dynamically installing a new CRD (carrots.vegetables.cattle.io)
// - Creating carrot resources
// - Asserting websocket receives COUNT update with new resource type
func (i *IntegrationSuite) TestCountWatchReceivesNewCRDResources() {
	ctx, cancel := context.WithTimeout(i.T().Context(), 60*time.Second)
	defer cancel()

	// Track cleanup items to ensure they're deleted even if test fails
	var cleanupCRD, cleanupCarrot1, cleanupCarrot2 bool

	// Start Steve server with authentication middleware
	authMiddleware := auth.ToMiddleware(auth.AuthenticatorFunc(auth.AlwaysAdmin))
	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
			UseTempDir:  true,
		},
		AuthMiddleware: authMiddleware,
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer func() {
		httpServer.Close()
		// Give server time to fully shut down
		time.Sleep(100 * time.Millisecond)
	}()

	baseURL := httpServer.URL

	// Wait for existing schemas to be available (bananas from SetupSuite)
	bananaGVR := k8sschema.GroupVersionResource{
		Group:    "fruits.cattle.io",
		Version:  "v1",
		Resource: "bananas",
	}
	i.waitForSchema(baseURL, bananaGVR)
	defer i.maybeStopAndDebug(baseURL)

	// Step 1: Verify initial COUNT state (has bananas, no carrots)
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		// Bananas should exist in initial COUNT
		count, ok := i.getCountFor(c, baseURL, "", "fruits.cattle.io.banana")
		assert.True(c, ok, "bananas should exist in initial COUNT")
		assert.GreaterOrEqual(c, count, 0, "banana count should be non-negative")

		// Carrots should NOT exist yet (CRD not installed)
		_, ok = i.getCountFor(c, baseURL, "", carrotSchemaID)
		assert.False(c, ok, "carrots should NOT exist in initial COUNT (CRD not installed yet)")
	}, 30*time.Second, 500*time.Millisecond, "initial COUNT state didn't converge")

	// Step 2: Establish WebSocket subscription to COUNT
	// Subscribe as admin (empty username) to see all resources
	conn := i.dialSubscribe(baseURL, "", "count")
	defer conn.Close()

	events := make(chan countSubscribeEvent, 16)
	go func() {
		for {
			var evt countSubscribeEvent
			if err := conn.ReadJSON(&evt); err != nil {
				close(events)
				return
			}
			events <- evt
		}
	}()

	// Wait for resource.start to confirm subscription is live
	// Use a separate loop since waitForSubscribeEvent expects subscribeEvent, not countSubscribeEvent
	func() {
		deadline := time.After(10 * time.Second)
		for {
			select {
			case evt, ok := <-events:
				if !ok {
					i.FailNow("events channel closed before resource.start")
					return
				}
				if evt.Name == "resource.start" {
					return
				}
			case <-deadline:
				i.FailNow("timeout waiting for resource.start")
				return
			}
		}
	}()

	// Step 3: Install carrot CRD dynamically (simulates rancher-backup operator installation)
	crdGVR := k8sschema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	carrotCRD := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": "carrots.vegetables.cattle.io",
			},
			"spec": map[string]interface{}{
				"group": "vegetables.cattle.io",
				"names": map[string]interface{}{
					"kind":     "Carrot",
					"listKind": "CarrotList",
					"plural":   "carrots",
					"singular": "carrot",
				},
				"scope": "Cluster",
				"versions": []interface{}{
					map[string]interface{}{
						"name": "v1",
						"schema": map[string]interface{}{
							"openAPIV3Schema": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"color": map[string]interface{}{
										"type": "string",
									},
									"length": map[string]interface{}{
										"type": "integer",
									},
								},
							},
						},
						"served":  true,
						"storage": true,
					},
				},
			},
		},
	}

	err = i.doApply(ctx, carrotCRD, crdGVR)
	i.Require().NoError(err, "failed to install carrot CRD")
	cleanupCRD = true

	// Clean up CRD on test exit - runs even if test fails
	defer func() {
		if cleanupCRD {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer deleteCancel()
			err := i.doDelete(deleteCtx, carrotCRD, crdGVR)
			if err != nil {
				i.T().Logf("Warning: failed to cleanup carrot CRD: %v", err)
			}
		}
	}()

	// Wait for schema to become available
	i.waitForSchema(baseURL, carrotGVR)

	// Step 4: Create carrot resources to trigger COUNT update
	carrot1 := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "vegetables.cattle.io/v1",
			"kind":       "Carrot",
			"metadata": map[string]interface{}{
				"name": "carrot-test-1",
			},
			"color":  "orange",
			"length": 15,
		},
	}

	carrot2 := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "vegetables.cattle.io/v1",
			"kind":       "Carrot",
			"metadata": map[string]interface{}{
				"name": "carrot-test-2",
			},
			"color":  "purple",
			"length": 12,
		},
	}

	err = i.doApply(ctx, carrot1, carrotGVR)
	i.Require().NoError(err, "failed to create carrot-test-1")
	cleanupCarrot1 = true
	defer func() {
		if cleanupCarrot1 {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer deleteCancel()
			err := i.doDelete(deleteCtx, carrot1, carrotGVR)
			if err != nil {
				i.T().Logf("Warning: failed to cleanup carrot-test-1: %v", err)
			}
		}
	}()

	err = i.doApply(ctx, carrot2, carrotGVR)
	i.Require().NoError(err, "failed to create carrot-test-2")
	cleanupCarrot2 = true
	defer func() {
		if cleanupCarrot2 {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer deleteCancel()
			err := i.doDelete(deleteCtx, carrot2, carrotGVR)
			if err != nil {
				i.T().Logf("Warning: failed to cleanup carrot-test-2: %v", err)
			}
		}
	}()

	// Give ClusterCache a moment to process the Add events
	time.Sleep(500 * time.Millisecond)

	// Step 5: Wait for COUNT websocket update (THIS IS THE FAILING ASSERTION)
	//
	// BEFORE FIX: This will timeout because COUNT's Watch() method has a static
	// gvkToSchema map built at watch creation time. The onChange callback
	// silently ignores events for the carrot GVK (lines 153-156 in counts.go).
	//
	// AFTER FIX: This should receive a resource.change event within ~1-2 seconds
	// containing the updated COUNT with vegetables.cattle.io.carrot entry.
	var countChangeEvent countSubscribeEvent
	found := false
	deadline := time.After(10 * time.Second)

collectEvents:
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				break collectEvents
			}
			if evt.Name != "resource.change" {
				continue
			}

			// Check if this event contains carrot count
			hasCarrotCount, err := i.eventContainsSchemaID(evt, carrotSchemaID)
			if err != nil {
			}
			if hasCarrotCount {
				countChangeEvent = evt
				found = true
				break collectEvents
			}

		case <-deadline:
			break collectEvents
		}
	}

	// Primary assertion: websocket should have sent COUNT update with carrot
	i.Require().True(found,
		"COUNT websocket should broadcast update when new CRD resources are created.\n"+
			"Expected: resource.change event with %q in counts\n"+
			"Actual: No such event received within 10 seconds\n"+
			"This proves the bug: COUNT's Watch() has static gvkToSchema map that ignores unknown GVKs",
		carrotSchemaID)

	// Verify the event has correct structure and carrot count
	if found {
		count := i.extractCountFromEvent(countChangeEvent, carrotSchemaID)
		i.Require().Equal(2, count,
			"COUNT websocket event should show 2 carrot resources")
	}

	// Step 6: Verify HTTP GET still works (proves bug is websocket-specific, not COUNT calculation)
	//
	// This should PASS even before the fix because HTTP GET calls getCount()
	// which dynamically reads current schemas via schemasToWatch() - it doesn't
	// use the static gvkToSchema map.
	count, ok := i.getCountFor(i.T(), baseURL, "", carrotSchemaID)
	i.Require().True(ok,
		"HTTP GET to /v1/count should include newly installed CRD.\n"+
			"This proves the bug is specific to websocket watch, not COUNT calculation itself.")
	i.Require().Equal(2, count,
		"HTTP GET should show 2 carrot resources")

	// Cleanup: Close websocket and cancel context to ensure no state leaks to other tests
	conn.Close()
	cancel()
	// Give cleanup goroutines time to finish
	time.Sleep(100 * time.Millisecond)
}

// eventContainsSchemaID checks if a COUNT change event contains the given schema ID
func (i *IntegrationSuite) eventContainsSchemaID(evt countSubscribeEvent, schemaID string) (bool, error) {
	// evt.Data is already a map[string]interface{} for countSubscribeEvent
	counts, ok := evt.Data["counts"].(map[string]interface{})
	if !ok {
		return false, nil
	}

	_, hasSchema := counts[schemaID]
	return hasSchema, nil
}

// extractCountFromEvent extracts the count value for a schema from a COUNT change event
func (i *IntegrationSuite) extractCountFromEvent(evt countSubscribeEvent, schemaID string) int {
	// evt.Data is already a map[string]interface{} for countSubscribeEvent
	counts, ok := evt.Data["counts"].(map[string]interface{})
	if !ok {
		return 0
	}

	schemaCount, ok := counts[schemaID].(map[string]interface{})
	i.Require().True(ok, "event should contain schema %q", schemaID)

	summary, ok := schemaCount["summary"].(map[string]interface{})
	if !ok {
		return 0
	}

	count, ok := summary["count"].(float64)
	if !ok {
		return 0
	}

	return int(count)
}
