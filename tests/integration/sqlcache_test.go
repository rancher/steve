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
	"strings"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataSQLCacheDir = filepath.Join("testdata", "sqlcache")
)

func (i *IntegrationSuite) TestSQLCache() {
	ctx := i.T().Context()
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

	matches, err := filepath.Glob(filepath.Join(testdataSQLCacheDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")
		i.Run(name, func() {
			i.testSQLCacheScenario(ctx, match, baseURL)
		})
	}
}

func (i *IntegrationSuite) testSQLCacheScenario(ctx context.Context, testFile string, baseURL string) {
	type TestConfig struct {
		TestType  string `yaml:"testType"`
		SchemaID  string `yaml:"schemaID"`
		Namespace string `yaml:"namespace"`
		Tests     []struct {
			Name            string                    `yaml:"name"`
			Query           string                    `yaml:"query"`
			ExpectedNames   []string                  `yaml:"expectedNames"`
			ExpectedIDs     []string                  `yaml:"expectedIDs"`
			ExpectedSummary map[string]map[string]int `yaml:"expectedSummary"`
		} `yaml:"tests"`
	}

	var config TestConfig
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})

	i.doManifestWithHeader(ctx, testFile,
		func(ctx context.Context, header map[string]any) error {
			b, _ := yaml.Marshal(header)
			return yaml.Unmarshal(b, &config)
		},
		func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
			gvrs[gvr] = struct{}{}
			return i.doApply(ctx, obj, gvr)
		},
	)
	defer i.doManifestReversed(ctx, testFile, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	// Special handling for proxy store test (just checking schema registration)
	if config.TestType == "proxyStore" {
		i.Run("schema registered", func() {
			i.Require().True(true, "Schema registered successfully")
		})
		return
	}

	// Wait for resources to be indexed
	time.Sleep(2 * time.Second)

	for _, test := range config.Tests {
		i.Run(test.Name, func() {
			var url string
			if config.Namespace != "" {
				url = fmt.Sprintf("%s/v1/%s/%s?%s", baseURL, config.SchemaID, config.Namespace, test.Query)
			} else {
				url = fmt.Sprintf("%s/v1/%s?%s", baseURL, config.SchemaID, test.Query)
			}

			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				i.T().Logf("URL: %s", url)
				i.T().Logf("Status: %d", resp.StatusCode)
				i.T().Logf("Response: %s", string(body))
			}
			i.Require().Equal(http.StatusOK, resp.StatusCode)

			// Handle summary tests
			if test.ExpectedSummary != nil {
				type SummaryEntry struct {
					Property string         `json:"property"`
					Counts   map[string]int `json:"counts"`
				}
				type SummaryResponse struct {
					Summary []SummaryEntry `json:"summary"`
				}
				var parsed SummaryResponse
				err = json.NewDecoder(resp.Body).Decode(&parsed)
				i.Require().NoError(err)

				actualSummary := make(map[string]map[string]int)
				for _, entry := range parsed.Summary {
					actualSummary[entry.Property] = entry.Counts
				}
				i.Require().Equal(test.ExpectedSummary, actualSummary)
				return
			}

			// Handle ID-based tests (for clusters)
			if test.ExpectedIDs != nil {
				type IDResponse struct {
					Data []struct {
						ID string `json:"id"`
					} `json:"data"`
				}
				var parsed IDResponse
				err = json.NewDecoder(resp.Body).Decode(&parsed)
				i.Require().NoError(err)

				var ids []string
				for _, item := range parsed.Data {
					ids = append(ids, item.ID)
				}
				i.Require().Equal(test.ExpectedIDs, ids)
				return
			}

			// Handle name-based tests (most common)
			type NameResponse struct {
				Data []struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
				} `json:"data"`
			}
			var parsed NameResponse
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			var names []string
			for _, item := range parsed.Data {
				names = append(names, item.Metadata.Name)
			}
			sort.Strings(names)
			sort.Strings(test.ExpectedNames)
			i.Require().Equal(test.ExpectedNames, names)
		})
	}
}
