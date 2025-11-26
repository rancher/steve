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
}

func (i *IntegrationSuite) testColumnsScenario(ctx context.Context, scenario string, baseURL string) {
	scenarioTests := filepath.Join(testdataColumnsDir, scenario+".test.yaml")

	type ColumnTestConfig struct {
		Tests []struct {
			SchemaID string  `yaml:"schemaID"`
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
					}
				}
			}

			i.Require().Equal(test.Expected, table)
		})
	}
}

// Yeah.. Heh.. 🤷
func mapToObj(theMap map[string]any, obj any) error {
	b, err := yaml.Marshal(theMap)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, obj)
}
