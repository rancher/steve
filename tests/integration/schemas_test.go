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

	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataSchemasDir = filepath.Join("testdata", "schemas")
)

func (i *IntegrationSuite) TestSchemas() {
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

	matches, err := filepath.Glob(filepath.Join(testdataSchemasDir, "*.test.json"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.json")
		i.Run(name, func() {
			i.testSchemasScenario(ctx, name, baseURL)
		})
	}
}

func (i *IntegrationSuite) testSchemasScenario(ctx context.Context, scenario string, baseURL string) {
	manifestsFile := filepath.Join(testdataSchemasDir, scenario+".manifests.yaml")
	expectedFile := filepath.Join(testdataSchemasDir, scenario+".test.json")

	var gvrs []k8sschema.GroupVersionResource
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs = append(gvrs, gvr)
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	for _, gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	type Schema struct {
		ID         string `json:"id"`
		Attributes struct {
			Columns []common.ColumnDefinition `json:"columns"`
		} `json:"attributes"`
	}
	type Schemas struct {
		Data []Schema `json:"data"`
	}

	expectedBytes, err := os.ReadFile(expectedFile)
	i.Require().NoError(err)

	var expected Schemas
	err = json.Unmarshal(expectedBytes, &expected)
	i.Require().NoError(err)

	url := fmt.Sprintf("%s/v1/schemas", baseURL)
	resp, err := http.Get(url)
	i.Require().NoError(err)
	defer resp.Body.Close()
	i.Require().Equal(http.StatusOK, resp.StatusCode)

	var parsedResponse Schemas
	err = json.NewDecoder(resp.Body).Decode(&parsedResponse)
	i.Require().NoError(err)

	parsedMap := make(map[string]Schema)
	for _, schema := range parsedResponse.Data {
		parsedMap[schema.ID] = schema
	}

	for _, expectedSchema := range expected.Data {
		i.Run(expectedSchema.ID, func() {
			parsedSchema, ok := parsedMap[expectedSchema.ID]
			i.Require().True(ok, "%s not found in schemas, did you make a typo?", expectedSchema.ID)
			i.Require().Equal(expectedSchema, parsedSchema)
		})
	}
}
