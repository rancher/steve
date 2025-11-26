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

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataFilteringDir = filepath.Join("testdata", "filtering")
)

func (i *IntegrationSuite) TestListFiltering() {
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

	matches, err := filepath.Glob(filepath.Join(testdataFilteringDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")
		i.Run(name, func() {
			i.testFilterScenario(ctx, name, baseURL)
		})
	}
}

func (i *IntegrationSuite) testFilterScenario(ctx context.Context, scenario string, baseURL string) {
	scenarioManifests := filepath.Join(testdataFilteringDir, scenario+".manifests.yaml")
	scenarioTests := filepath.Join(testdataFilteringDir, scenario+".test.yaml")

	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, scenarioManifests, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, scenarioManifests, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	testFile, err := os.Open(scenarioTests)
	i.Require().NoError(err)

	type FilterTestConfig struct {
		SchemaID string `yaml:"schemaID"`
		Tests    []struct {
			Query    string   `yaml:"query"`
			Expected []string `yaml:"expected"`
		} `yaml:"tests"`
	}
	var filterTestConfig FilterTestConfig
	err = yaml.NewDecoder(testFile).Decode(&filterTestConfig)
	i.Require().NoError(err)

	for _, test := range filterTestConfig.Tests {
		i.Run(test.Query, func() {
			url := fmt.Sprintf("%s/v1/%s?%s", baseURL, filterTestConfig.SchemaID, test.Query)
			fmt.Println(url)
			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			i.Require().Equal(http.StatusOK, resp.StatusCode)

			type Response struct {
				Data []struct {
					ID string `json:"id"`
				} `json:"data"`
			}

			var parsed Response
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			gotIDs := make(map[string]struct{})
			for _, data := range parsed.Data {
				gotIDs[data.ID] = struct{}{}
			}

			expectedIDs := make(map[string]struct{})
			for _, data := range test.Expected {
				expectedIDs[data] = struct{}{}
			}

			i.Require().Equal(expectedIDs, gotIDs)
		})
	}
}
