package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TestNamespaceFormatter tests that namespace metadata is properly stripped for users without GET access.
func (i *IntegrationSuite) TestNamespaceFormatter() {
	ctx := i.T().Context()

	manifestsFile := filepath.Join(testdataListDir, "namespace-formatter.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	impersonateOrAdmin := func(req *http.Request) (user.Info, bool, error) {
		info, ok, err := auth.Impersonation(req)
		if ok || err != nil {
			return info, ok, err
		}
		return auth.AlwaysAdmin(req)
	}
	authMiddleware := auth.ToMiddleware(auth.AuthenticatorFunc(impersonateOrAdmin))

	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
		},
		AuthMiddleware: authMiddleware,
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	i.Run("user with namespace GET sees full metadata", func() {
		ns := i.getNamespaceByUser(ctx, baseURL, "ns-formatter-test", "user-ns-get")

		metadata := ns["metadata"].(map[string]interface{})
		labels := metadata["labels"].(map[string]interface{})
		annotations := metadata["annotations"].(map[string]interface{})

		// Should have all labels
		assert.Equal(i.T(), "production", labels["env"])
		assert.Equal(i.T(), "true", labels["fleet.cattle.io/managed"])

		// Should have all annotations
		assert.Equal(i.T(), "sensitive-info", annotations["description"])
		assert.Equal(i.T(), "true", annotations["management.cattle.io/system-namespace"])
		assert.Equal(i.T(), "local:p-test", annotations["field.cattle.io/projectId"])
	})

	// This is the real issue scenario: user has pod access in namespace but NO namespace permissions
	// Steve grants synthetic namespace access for UI dropdown, but should strip sensitive metadata
	i.Run("user with pod access but no namespace GET sees stripped metadata", func() {
		ns := i.getNamespaceByUser(ctx, baseURL, "ns-formatter-test", "user-pod-only")

		metadata := ns["metadata"].(map[string]interface{})
		labels := metadata["labels"].(map[string]interface{})
		annotations := metadata["annotations"].(map[string]interface{})

		// Should have name
		assert.Equal(i.T(), "ns-formatter-test", metadata["name"])

		// Should only have allowed labels
		assert.Equal(i.T(), "true", labels["fleet.cattle.io/managed"])
		assert.NotContains(i.T(), labels, "env", "sensitive label should be stripped")

		// Should only have allowed annotations
		assert.Equal(i.T(), "true", annotations["management.cattle.io/system-namespace"])
		assert.Equal(i.T(), "local:p-test", annotations["field.cattle.io/projectId"])
		assert.NotContains(i.T(), annotations, "description", "sensitive annotation should be stripped")

		// Should have empty spec/status (not nil)
		assert.NotNil(i.T(), ns["spec"], "spec should exist")
		assert.NotNil(i.T(), ns["status"], "status should exist")
	})
}

func (i *IntegrationSuite) getNamespaceByUser(ctx context.Context, baseURL, name, user string) map[string]interface{} {
	// Use LIST endpoint and find our namespace
	url := fmt.Sprintf("%s/v1/namespaces", baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	i.Require().NoError(err)

	if user != "" {
		req.Header.Set("Impersonate-User", user)
	}

	resp, err := http.DefaultClient.Do(req)
	i.Require().NoError(err)
	defer resp.Body.Close()

	i.Require().Equal(http.StatusOK, resp.StatusCode)

	var result struct {
		Data []map[string]interface{} `json:"data"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	i.Require().NoError(err)

	for _, ns := range result.Data {
		if ns["id"] == name {
			return ns
		}
	}
	i.Require().Fail("namespace not found: " + name)
	return nil
}
