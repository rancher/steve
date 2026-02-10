package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataColumnsDir = filepath.Join("testdata", "columns")
)

func (i *IntegrationSuite) TestListColumns() {
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

	matches, err := filepath.Glob(filepath.Join(testdataColumnsDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")
		i.Run(name, func() {
			i.testColumnsScenario(ctx, name, baseURL)
		})
	}
	depPodName := "check deployments for associated pod info"
	i.Run(depPodName, func() {
		i.testDeploymentPodAssociations(ctx, depPodName, baseURL)
	})
}

func (i *IntegrationSuite) testColumnsScenario(ctx context.Context, scenario string, baseURL string) {
	scenarioTests := filepath.Join(testdataColumnsDir, scenario+".test.yaml")

	type ColumnTestConfig struct {
		Tests []struct {
			SchemaID string  `yaml:"schemaID"`
			Query    string  `yaml:"query"`
			Expected [][]any `yaml:"expected"`
		} `json:"tests"`
	}
	var columnTestConfig ColumnTestConfig

	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifestWithHeader(ctx, scenarioTests,
		func(ctx context.Context, header map[string]any) error {
			return mapToObj(header, &columnTestConfig)
		},
		func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
			gvrs[gvr] = struct{}{}
			return i.doApply(ctx, obj, gvr)
		},
	)
	defer i.doManifestReversed(ctx, scenarioTests, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	for _, test := range columnTestConfig.Tests {
		i.Run(test.SchemaID, func() {
			url := fmt.Sprintf("%s/v1/%s", baseURL, test.SchemaID)
			if test.Query != "" {
				url = fmt.Sprintf("%s?%s", url, test.Query)
			}
			fmt.Println(url)
			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			i.Require().Equal(http.StatusOK, resp.StatusCode)

			type Response struct {
				Data []struct {
					Metadata struct {
						Fields []any `json:"fields"`
					} `json:"metadata"`
				} `json:"data"`
			}
			var parsed Response
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			i.Require().Len(parsed.Data, len(test.Expected))

			var table [][]any
			for _, row := range parsed.Data {
				table = append(table, row.Metadata.Fields)
			}

			for row := range test.Expected {
				i.Require().Len(table[row], len(test.Expected[row]))
				for fieldIndex := range test.Expected[row] {
					field := test.Expected[row][fieldIndex]
					switch field {
					case "$duration":
						_, err := rescommon.ParseHumanReadableDuration(fmt.Sprintf("%v", table[row][fieldIndex]))
						i.Require().NoError(err, "expected duration (row:%d, col:%d) but got: %s", row, fieldIndex, table[row][fieldIndex])
						test.Expected[row][fieldIndex] = fmt.Sprintf("%v", table[row][fieldIndex])
					case "$timestamp":
						_, err := time.Parse(time.RFC3339, fmt.Sprintf("%v", table[row][fieldIndex]))
						i.Require().NoError(err, "expected duration (row:%d, col:%d) but got: %s", row, fieldIndex, table[row][fieldIndex])
						test.Expected[row][fieldIndex] = fmt.Sprintf("%v", table[row][fieldIndex])
					case "$skip":
						// Sometimes you just don't care what the value is
						test.Expected[row][fieldIndex] = table[row][fieldIndex]
					}
				}
			}

			i.Require().Equal(test.Expected, table)
		})
	}
}

func (i *IntegrationSuite) testDeploymentPodAssociations(ctx context.Context, scenario string, baseURL string) {
	var gvr k8sschema.GroupVersionResource
	gvr = k8sschema.GroupVersionResource{
		Group: "apps",
		Version: "v1",
		Resource: "deployments",
	}
	thingy := fmt.Sprintf("%s.%s", gvr.Group, gvr.Resource)
	i.waitForSchema(baseURL, gvr)
	defer i.maybeStopAndDebug(baseURL)
	url := fmt.Sprintf("%s/v1/%s", baseURL, thingy)
	fmt.Println(url)
	resp, err := http.Get(url)
	i.Require().NoError(err)
	defer resp.Body.Close()

	i.Require().Equal(http.StatusOK, resp.StatusCode)

	type Response struct {
		Data []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
				Name string `json:"name"`
				AssociatedData []any `json:"associatedData,omitempty"`
			} `json:"metadata"`
		} `json:"data"`
	}
	var parsed Response
	err = json.NewDecoder(resp.Body).Decode(&parsed)
	i.Require().NoError(err)

	// Require at least one non-empty associations block

	i.Require().GreaterOrEqualf(len(parsed.Data), 1, "no data")
	numAssociations := 0
	for _, row := range parsed.Data {
		md := row.Metadata
		if len(md.AssociatedData) > 0 {
			numAssociations += 1
		}
	}
	i.Require().GreaterOrEqualf(numAssociations, 1, "no deployment-pod associations found")
}


// Yeah.. Heh.. 🤷
func mapToObj(theMap map[string]any, obj any) error {
	b, err := yaml.Marshal(theMap)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, obj)
}
