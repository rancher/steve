package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type StateResponse struct {
	Metadata struct {
		Name  string `json:"name"`
		State struct {
			Name string `json:"name"`
		} `json:"state"`
	} `json:"metadata"`
}

type ListStateResponse struct {
	Data []StateResponse `json:"data"`
}

func (i *IntegrationSuite) TestStateUpdate() {
	ctx := i.T().Context()
	// Initialize Steve server with SQL Cache enabled
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
	gvr := schema.GroupVersionResource{
		Group:    "management.cattle.io",
		Version:  "v3",
		Resource: "clusters",
	}

	clusterName := "state-test-cluster"
	schemaID := "management.cattle.io.cluster"

	// 1. Create a Cluster with initial state "provisioning"
	cluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "management.cattle.io/v3",
			"kind":       "Cluster",
			"metadata": map[string]interface{}{
				"name": clusterName,
			},
			"spec": map[string]interface{}{},
			"status": map[string]interface{}{
				"summary": map[string]interface{}{
					"state": "provisioning",
				},
			},
		},
	}

	i.doApply(ctx, cluster, gvr)
	defer i.doDelete(ctx, cluster, gvr)

	i.waitForSchema(baseURL, gvr)

	defer i.maybeStopAndDebug(baseURL)

	// 2. List cluster by filtering with status "provisioning"
	// This exercises the SQL cache: if the transformer didn't put the state in the DB,
	// or if the index didn't pick it up, this filter would return nothing.
	listUrlProvisioning := fmt.Sprintf("%s/v1/%s?filter=metadata.state.name=provisioning", baseURL, schemaID)

	i.Require().Eventually(func() bool {
		return checkListContainsCluster(i.T(), listUrlProvisioning, clusterName, "provisioning")
	}, 10*time.Second, 500*time.Millisecond, "Should find cluster when filtering by initial state 'provisioning'")

	// 3. Update the Cluster to state "active"
	cluster.Object["status"].(map[string]interface{})["summary"].(map[string]interface{})["state"] = "active"
	err = i.doApply(ctx, cluster, gvr)
	i.Require().NoError(err)

	// 4. List cluster by filtering with the new status "active"
	// This is the critical test: it verifies that the SQL cache was updated with the new state.
	// If 'metadata.state.name' was immutable (the bug), the DB would still have "provisioning",
	// so filtering by "active" would return nothing.
	listUrlActive := fmt.Sprintf("%s/v1/%s?filter=metadata.state.name=active", baseURL, schemaID)

	i.Require().Eventually(func() bool {
		return checkListContainsCluster(i.T(), listUrlActive, clusterName, "active")
	}, 10*time.Second, 500*time.Millisecond, "Should find cluster when filtering by updated state 'active'")
}

func checkListContainsCluster(t assert.TestingT, url, clusterName, expectedState string) bool {
	resp, err := http.Get(url)
	if !assert.NoError(t, err) {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var parsed ListStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); !assert.NoError(t, err) {
		return false
	}

	for _, item := range parsed.Data {
		if item.Metadata.Name == clusterName {
			if item.Metadata.State.Name != expectedState {
				return false
			}
			return true
		}
	}
	return false
}
