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
		i.waitForSchema(baseURL, gvr)
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

			i.Require().Equal(http.StatusOK, resp.StatusCode)

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

// Verify metadata.state.name and metadata.fields[2] are in sync.

func (i *IntegrationSuite) TestListSortPodsByStateName() {
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

	// Grab one file to make this look like other tests
	matches, err := filepath.Glob(filepath.Join(testdataSortingDir, "*.test.yaml"))
	i.Require().NoError(err)
	i.Require().Less(0, len(matches))
	// We don't actually process any yaml files here, but this helps wait for the server to be ready.
	scenario := filepath.Base(matches[0])
	scenario = strings.TrimSuffix(scenario, ".test.yaml")
	scenarioManifests := filepath.Join(testdataSortingDir, scenario+".manifests.yaml")

	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, scenarioManifests, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	i.Run("sorting pods", func() {
		url := fmt.Sprintf("%s/v1/pods?sort=metadata.state.name,metadata.name", baseURL)
		fmt.Println(url)
		resp, err := http.Get(url)
		i.Require().NoError(err)
		defer resp.Body.Close()

		i.Require().Equal(http.StatusOK, resp.StatusCode)

		type Response struct {
			Data []struct {
				Metadata struct {
					Name string `json:"name"`
					Fields []any `json:"fields"`
					State struct {
						Name string `json:"name"`
					} `json:"state"`
				} `json:"metadata"`
			} `json:"data"`
		}
		var parsed Response
		err = json.NewDecoder(resp.Body).Decode(&parsed)
		i.Require().NoError(err)

		if len(parsed.Data) == 0 {
			fmt.Printf("Warning: TestListSortPodsByStateName: retrieved 0 pods, expected more\n")
			return
		}

		lastStateName := ""
		lastName := ""
		for _, data := range parsed.Data {
			mdName := data.Metadata.Name
			stateName := data.Metadata.State.Name
			i.Require().GreaterOrEqual(len(data.Metadata.Fields), 2)
			i.Require().NotEqual("", mdName)
			i.Require().NotEqual("", stateName)
			mdStateName, ok := data.Metadata.Fields[2].(string)
			i.Require().True(ok)

			// Verify metadata.state.name and metadata.fields[2] differ only by case of the first letter
			i.Require().Equal(len(stateName), len(mdStateName))
			if len(stateName) > 1 {
				i.Require().Equal(stateName[1:len(stateName)], mdStateName[1:len(mdStateName)])
			}
			// md.stateName is camelCase
			// md.fields[2] is UpperCamelCase
			firstStateName := stateName[0:1]
			firstField2Name := mdStateName[0:1]
			i.Require().Equal(strings.ToUpper(firstStateName), firstField2Name)
			i.Require().Equal(strings.ToLower(firstField2Name), firstStateName)

			// Verify metadata.state.name and metadata.name are sorted ascending
			if lastStateName == stateName {
				// verify the second -- allow for equal names just in case there are
				// two pods with same name in different namespaces.
				i.Require().LessOrEqual(lastName, mdName)
			} else {
				i.Require().Less(lastStateName, stateName)
				lastStateName = stateName
			}
			lastName = mdName
		}
	})
}
