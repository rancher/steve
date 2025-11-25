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
	testdataSortingDir = filepath.Join("testdata", "sorting")
)

func (i *IntegrationSuite) TestListSorting() {
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

	matches, err := filepath.Glob(filepath.Join(testdataSortingDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")
		i.Run(name, func() {
			i.testSortScenario(ctx, name, baseURL)
		})
	}
}

func (i *IntegrationSuite) testSortScenario(ctx context.Context, scenario string, baseURL string) {
	scenarioManifests := filepath.Join(testdataSortingDir, scenario+".manifests.yaml")
	scenarioTests := filepath.Join(testdataSortingDir, scenario+".test.yaml")

	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, scenarioManifests, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, scenarioManifests, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, fmt.Sprintf("%s.%s", gvr.Group, gvr.Resource))
	}

	defer i.maybeStopAndDebug(baseURL)

	testFile, err := os.Open(scenarioTests)
	i.Require().NoError(err)

	type SortTestConfig struct {
		SchemaID string `yaml:"schemaID"`
		Tests    []struct {
			Query    string   `yaml:"query"`
			Expected []string `yaml:"expected"`
		} `yaml:"tests"`
	}
	var sortTestConfig SortTestConfig
	err = yaml.NewDecoder(testFile).Decode(&sortTestConfig)
	i.Require().NoError(err)

	for _, test := range sortTestConfig.Tests {
		i.Run(test.Query, func() {
			url := fmt.Sprintf("%s/v1/%s?%s", baseURL, sortTestConfig.SchemaID, test.Query)
			fmt.Println(url)
			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			type Response struct {
				Data []struct {
					ID string `json:"id"`
				} `json:"data"`
			}
			var parsed Response
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			var ids []string
			for _, data := range parsed.Data {
				ids = append(ids, data.ID)
			}
			i.Require().Equal(test.Expected, ids)
		})
	}
}
