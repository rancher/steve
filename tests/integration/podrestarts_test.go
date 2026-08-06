package tests

import (
	"context"
	"encoding/json"
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
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

var testdataPodRestartsDir = filepath.Join("testdata", "podrestarts")

func (i *IntegrationSuite) TestPodRestarts() {
	ctx := i.T().Context()

	manifestsFile := filepath.Join(testdataPodRestartsDir, "podrestarts.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	// Wait for pods to stabilize - restarting-pod needs time to restart once
	i.T().Log("Waiting for pods to stabilize (8 seconds)...")
	time.Sleep(8 * time.Second)

	// Run SQL mode only - these tests are specifically for SQL cache with multi-value field support
	i.runPodRestartsTest(ctx, gvrs)
}

func (i *IntegrationSuite) runPodRestartsTest(ctx context.Context, gvrs map[k8sschema.GroupVersionResource]struct{}) {
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
	steveHandler, err = server.New(ctx, i.restCfg, &server.Options{
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

	// Wait for the pod schema (and any other applied types) to be registered before
	// hitting the API - a fixed sleep isn't enough on a slow/cold cluster.
	for gvr := range gvrs {
		i.waitForSchema(steveServer.URL, gvr)
	}

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

			for _, test := range config.Tests {
				i.Run(fmt.Sprintf("%s", test.Description), func() {
					url := buildURLRaw(steveServer.URL, config.SchemaID, test.Namespace, test.Query)
					i.T().Logf("Testing: %s", url)

					// The SQL cache indexer processes newly-applied pods asynchronously,
					// so even after the schema is registered the rows may not be indexed
					// yet. Retry until the expected data shows up.
					i.Require().EventuallyWithT(func(c *assert.CollectT) {
						req, err := http.NewRequest("GET", url, nil)
						if !assert.NoError(c, err) {
							return
						}

						if test.User != "" {
							req.Header.Set("Impersonate-User", test.User)
						}

						resp, err := http.DefaultClient.Do(req)
						if !assert.NoError(c, err) {
							return
						}
						defer resp.Body.Close()

						if !assert.Equal(c, http.StatusOK, resp.StatusCode, "request should succeed") {
							return
						}

						type Response struct {
							Data []struct {
								Metadata struct {
									Name string `json:"name"`
								} `json:"metadata"`
							} `json:"data"`
						}

						var parsed Response
						if !assert.NoError(c, json.NewDecoder(resp.Body).Decode(&parsed)) {
							return
						}

						var actualNames []string
						for _, pod := range parsed.Data {
							actualNames = append(actualNames, pod.Metadata.Name)
						}

						if test.ExpectContains {
							expectedNames := make([]string, 0, len(test.Expect))
							for _, expected := range test.Expect {
								expectedNames = append(expectedNames, expected["name"])
							}

							for _, expectedName := range expectedNames {
								assert.Contains(c, actualNames, expectedName, "expected pod %q to be in response", expectedName)
							}
						} else {
							expectedNames := make([]string, 0, len(test.Expect))
							for _, expected := range test.Expect {
								expectedNames = append(expectedNames, expected["name"])
							}

							assert.Equal(c, expectedNames, actualNames, "pod list order mismatch")
						}
					}, 5*time.Second, 100*time.Millisecond)
				})
			}
		})
	}
}
