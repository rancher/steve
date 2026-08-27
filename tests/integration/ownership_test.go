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
	"github.com/rancher/steve/pkg/resources/ownership"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

var testdataOwnershipDir = filepath.Join("testdata", "ownership")

var bananaGVK = k8sschema.GroupVersionKind{Group: "fruits.cattle.io", Version: "v1", Kind: "Banana"}

const (
	bananaListURLPath = "fruits.cattle.io.bananas"
	bananaSchemaID    = "fruits.cattle.io.banana"
)

// TestOwnershipCountMatchesList is a regression test for
// rancher/rancher#56849: /v1/count used to leak the true global count of
// resources served by rancher's extension apiserver (ext.cattle.io Tokens,
// Kubeconfigs) to non-admin users, because the per-user ownership filter
// that the regular list endpoint applies (see
// pkg/stores/sqlproxy/proxy_store.go) was never applied to /v1/count (see
// pkg/resources/counts/counts.go).
//
// This test doesn't stand up a real extension apiserver. Instead it
// registers an ownership.Filter for the Banana CRD's GVK -- functionally
// identical to what rancher/rancher registers for Tokens/Kubeconfigs -- and
// asserts that for every user (admin and non-admin), both /v1/count and
// the regular list endpoint report the exact expected number of visible
// bananas. Asserting the exact expected counts (rather than only that the
// two endpoints agree with each other) ensures the test would fail if the
// ownership filter were dropped from both call sites at once.
func (i *IntegrationSuite) TestOwnershipCountMatchesList() {
	ctx := i.T().Context()

	// Register the ownership filter for the duration of this test only, so
	// it can't leak into other tests running against the same schema.
	ownership.Register(bananaGVK, ownership.NewUserIDLabelFilter("cattle.io/user-id"))
	defer ownership.Unregister(bananaGVK)

	manifestsFile := filepath.Join(testdataOwnershipDir, "common.manifests.yaml")
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
			UseTempDir:  true,
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

	totalBananas := 4

	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		count, ok := i.getCountFor(c, baseURL, "", bananaSchemaID)
		require.True(c, ok, "no %q entry in /v1/count response yet", bananaSchemaID)
		require.Equal(c, totalBananas, count, "admin count hasn't converged to the expected total yet")
	}, 90*time.Second, 1*time.Second)

	tests := []struct {
		description string
		username    string // "" = no impersonation header = AlwaysAdmin fallback
		expected    int
	}{
		{description: "admin sees every banana regardless of owner", username: "", expected: totalBananas},
		{description: "user-alice sees only her own bananas", username: "user-alice", expected: 2},
		{description: "user-bob sees only his own bananas", username: "user-bob", expected: 1},
		{description: "user-carol owns no bananas and sees none", username: "user-carol", expected: 0},
	}
	for _, test := range tests {
		i.Run(test.description, func() {
			listCount := i.countBananasViaList(ctx, baseURL, test.username)
			countEndpointCount := i.countBananasViaCountEndpoint(ctx, baseURL, test.username)

			assert.Equal(i.T(), test.expected, listCount,
				"user %q: /v1/%s returned an unexpected number of bananas", test.username, bananaListURLPath)
			assert.Equal(i.T(), test.expected, countEndpointCount,
				"user %q: /v1/count returned an unexpected banana count", test.username)
		})
	}
}

func (i *IntegrationSuite) countBananasViaList(ctx context.Context, baseURL, username string) int {
	url := fmt.Sprintf("%s/v1/%s", baseURL, bananaListURLPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	i.Require().NoError(err)
	if username != "" {
		req.Header.Set("Impersonate-User", username)
	}

	resp, err := http.DefaultClient.Do(req)
	i.Require().NoError(err)
	defer resp.Body.Close()
	i.Require().Equal(http.StatusOK, resp.StatusCode)

	var result struct {
		Data []map[string]interface{} `json:"data"`
	}
	i.Require().NoError(json.NewDecoder(resp.Body).Decode(&result))
	return len(result.Data)
}

type countResponse struct {
	Data []struct {
		Counts map[string]struct {
			Summary struct {
				Count int `json:"count"`
			} `json:"summary"`
		} `json:"counts"`
	} `json:"data"`
}

func (i *IntegrationSuite) getCountFor(c require.TestingT, baseURL, username, schemaID string) (int, bool) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/count", nil)
	require.NoError(c, err)
	if username != "" {
		req.Header.Set("Impersonate-User", username)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(c, err)
	defer resp.Body.Close()
	require.Equal(c, http.StatusOK, resp.StatusCode)

	var result countResponse
	require.NoError(c, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(c, result.Data, 1)

	itemCount, ok := result.Data[0].Counts[schemaID]
	return itemCount.Summary.Count, ok
}

func (i *IntegrationSuite) countBananasViaCountEndpoint(ctx context.Context, baseURL, username string) int {
	count, ok := i.getCountFor(i.T(), baseURL, username, bananaSchemaID)
	i.Require().True(ok, "no %q entry in /v1/count response", bananaSchemaID)
	return count
}
