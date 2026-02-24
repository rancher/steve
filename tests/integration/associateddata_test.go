package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"time"

	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

var testdataAssociatedDataDir = filepath.Join("testdata", "associateddata")

// SimplifiedSS is used for direct comparison in tests
type SimplifiedSS struct {
	Name string   `json:"name"`
	Pods []string `json:"pods"`
}

func (i *IntegrationSuite) TestAssociatedData() {
	ctx := i.T().Context()

	manifestsFile := filepath.Join(testdataAssociatedDataDir, "associateddata.manifests.yaml")
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
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
		SQLCache: true,
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
		},
		AuthMiddleware: authMiddleware,
	})
	i.Require().NoError(err)

	steveServer := httptest.NewServer(steveHandler)
	defer steveServer.Close()

	// Wait for pods to be ready and cache to sync
	i.T().Log("Waiting for pods to be available in steve cache...")
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		// Query with filter to ensure we hit the steve cache
		url := fmt.Sprintf("%s/v1/pods?filter=metadata.namespace=associated-data-test", steveServer.URL)
		resp, err := http.DefaultClient.Get(url)
		if !assert.NoError(c, err) {
			return
		}
		defer resp.Body.Close()

		if !assert.Equal(c, http.StatusOK, resp.StatusCode) {
			return
		}

		var podList struct {
			Data []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"data"`
		}
		err = json.NewDecoder(resp.Body).Decode(&podList)
		if !assert.NoError(c, err) {
			return
		}

		foundPods := make(map[string]bool)
		for _, pod := range podList.Data {
			foundPods[pod.Metadata.Name] = true
		}

		expectedPods := []string{"nginx-a-0", "nginx-b-0", "nginx-b-1", "nginx-c-0"}
		for _, name := range expectedPods {
			assert.True(c, foundPods[name], "pod %s not found in steve cache yet", name)
		}
	}, 1*time.Minute, 1*time.Second)

	// Helper to extract relevant data from Steve response
	extract := func(respBody []byte) []SimplifiedSS {
		var raw struct {
			Data []struct {
				Metadata struct {
					Name           string `json:"name"`
					AssociatedData []struct {
						GVK  k8sschema.GroupVersionKind `json:"gvk"`
						Data []struct {
							ChildName string `json:"childName"`
						} `json:"data"`
					} `json:"associatedData"`
				} `json:"metadata"`
			} `json:"data"`
		}
		err := json.Unmarshal(respBody, &raw)
		i.Require().NoError(err)

		var results []SimplifiedSS
		for _, item := range raw.Data {
			ss := SimplifiedSS{Name: item.Metadata.Name, Pods: []string{}}
			for _, ad := range item.Metadata.AssociatedData {
				if ad.GVK.Kind == "Pod" {
					for _, pod := range ad.Data {
						ss.Pods = append(ss.Pods, pod.ChildName)
					}
				}
			}
			sort.Strings(ss.Pods)
			results = append(results, ss)
		}
		sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
		return results
	}

	// 1. Admin Test: Should see all 3 StatefulSets and all pods
	i.Run("AdminAccess", func() {
		url := fmt.Sprintf("%s/v1/apps.statefulsets?includeAssociatedData=true&filter=metadata.namespace=associated-data-test", steveServer.URL)
		resp, err := http.DefaultClient.Get(url)
		i.Require().NoError(err)
		defer resp.Body.Close()

		expected := []SimplifiedSS{
			{Name: "nginx-a", Pods: []string{"nginx-a-0"}},
			{Name: "nginx-b", Pods: []string{"nginx-b-0", "nginx-b-1"}},
			{Name: "nginx-c", Pods: []string{"nginx-c-0"}},
		}

		body, err := io.ReadAll(resp.Body)
		i.Require().NoError(err)
		i.Assert().Equal(expected, extract(body))
	})

	// 2. Restricted User Test: Should see 2 StatefulSets and pods nginx-a-0 and nginx-b-0
	i.Run("RestrictedUserAccess", func() {
		url := fmt.Sprintf("%s/v1/apps.statefulsets?includeAssociatedData=true&filter=metadata.namespace=associated-data-test", steveServer.URL)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Impersonate-User", "restricted-user")
		resp, err := http.DefaultClient.Do(req)
		i.Require().NoError(err)
		defer resp.Body.Close()

		expected := []SimplifiedSS{
			{Name: "nginx-a", Pods: []string{"nginx-a-0"}},
			{Name: "nginx-b", Pods: []string{"nginx-b-0"}},
		}

		body, err := io.ReadAll(resp.Body)
		i.Require().NoError(err)
		i.Assert().Equal(expected, extract(body))
	})
}
