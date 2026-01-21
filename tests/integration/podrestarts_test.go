package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

var (
	testdataPodRestartsDir = filepath.Join("testdata", "podrestarts")
)

func (i *IntegrationSuite) TestPodRestarts() {
	ctx := i.T().Context()

	// Apply manifests once
	manifestsFile := filepath.Join(testdataPodRestartsDir, "podrestarts.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	// Cleanup manifests after all tests complete
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	// Wait for pods to be ready
	time.Sleep(10 * time.Second)

	// Run SQL mode only - these tests are specifically for SQL cache with JSONB support
	i.runPodRestartsTest(ctx, true, gvrs)
}

func (i *IntegrationSuite) runPodRestartsTest(ctx context.Context, sqlCache bool, gvrs map[k8sschema.GroupVersionResource]struct{}) {
	// Custom authenticator
	impersonateOrAdmin := func(req *http.Request) (user.Info, bool, error) {
		info, ok, err := auth.Impersonation(req)
		if ok || err != nil {
			return info, ok, err
		}
		return auth.AlwaysAdmin(req)
	}
	authMiddleware := auth.ToMiddleware(auth.AuthenticatorFunc(impersonateOrAdmin))

	var steveHandler http.Handler
	var err error
	if sqlCache {
		steveHandler, err = server.New(ctx, i.restCfg, &server.Options{
			SQLCache: true,
			SQLCacheFactoryOptions: factory.CacheFactoryOptions{
				GCInterval:  15 * time.Minute,
				GCKeepCount: 1000,
			},
			AuthMiddleware: authMiddleware,
		})
	} else {
		steveHandler, err = server.New(ctx, i.restCfg, &server.Options{
			SQLCache:       false,
			AuthMiddleware: authMiddleware,
		})
	}
	i.Require().NoError(err)

	steveServer := httptest.NewServer(steveHandler)
	defer steveServer.Close()

	// Wait for cache to be populated
	if sqlCache {
		time.Sleep(10 * time.Second)
	}

	// Load and run test scenarios
	matches, err := filepath.Glob(filepath.Join(testdataPodRestartsDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, testFile := range matches {
		name := filepath.Base(testFile)
		name = strings.TrimSuffix(name, ".test.yaml")

		i.Run(name, func() {
			var config ListTestConfig
			data, err := os.ReadFile(testFile)
			i.Require().NoError(err)
			err = yaml.Unmarshal(data, &config)
			i.Require().NoError(err)

			for idx, test := range config.Tests {
				i.Run(fmt.Sprintf("#%02d", idx), func() {
					url := buildURLRaw(steveServer.URL, config.SchemaID, test.Namespace, test.Query)
					i.T().Logf("Testing: %s", url)

					req, err := http.NewRequest("GET", url, nil)
					i.Require().NoError(err)

					if test.User != "" {
						req.Header.Set("Impersonate-User", test.User)
					}

					resp, err := http.DefaultClient.Do(req)
					i.Require().NoError(err)
					defer resp.Body.Close()

					// Just check that the request succeeds (status 200)
					i.Assert().Equal(http.StatusOK, resp.StatusCode, "request should succeed")
				})
			}
		})
	}
}
