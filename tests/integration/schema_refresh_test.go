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
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataSchemaRefreshDir = filepath.Join("testdata", "schema_refresh")
)

type SchemaRefreshScenario struct {
	Name            string                 `yaml:"name"`
	SchemaID        string                 `yaml:"schemaID"`
	CRDName         string                 `yaml:"crdName"`
	Resource        map[string]interface{} `yaml:"resource"`
	ExpectedInitial [][]any                `yaml:"expectedInitial"`
	ExpectedUpdated [][]any                `yaml:"expectedUpdated"`
	InitialCRD      map[string]interface{} `yaml:"initialCRD"`
	UpdatedCRD      map[string]interface{} `yaml:"updatedCRD"`
}

type SchemaRefreshTestConfig struct {
	Scenarios []SchemaRefreshScenario `yaml:"scenarios"`
}

type FieldsResponse struct {
	Data []struct {
		Metadata struct {
			Name   string `json:"name"`
			Fields []any  `json:"fields"`
		} `json:"metadata"`
	} `json:"data"`
}

func (i *IntegrationSuite) TestSchemaRefresh() {
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

	// Find all scenario files
	matches, err := filepath.Glob(filepath.Join(testdataSchemaRefreshDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		scenarioFile := filepath.Base(match)
		scenarioFile = strings.TrimSuffix(scenarioFile, ".test.yaml")
		
		// Load test configuration
		configFile, err := os.Open(match)
		i.Require().NoError(err)
		defer configFile.Close()

		var testConfig SchemaRefreshTestConfig
		err = yaml.NewDecoder(configFile).Decode(&testConfig)
		i.Require().NoError(err)

		// Run each scenario in the file
		for _, scenario := range testConfig.Scenarios {
			i.Run(scenario.Name, func() {
				i.testSchemaRefreshScenario(ctx, scenario, baseURL)
			})
		}
	}
}

func (i *IntegrationSuite) testSchemaRefreshScenario(ctx context.Context, scenario SchemaRefreshScenario, baseURL string) {
	// 1. Convert initialCRD and updatedCRD maps to unstructured objects
	initialCRD := &unstructured.Unstructured{Object: scenario.InitialCRD}
	updatedCRD := &unstructured.Unstructured{Object: scenario.UpdatedCRD}

	// 2. Define GVRs
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	testGVR := i.parseGVRFromSchemaID(scenario.SchemaID)

	// 3. Apply initial CRD
	err := i.doApply(ctx, initialCRD, crdGVR)
	i.Require().NoError(err)
	defer i.doDelete(ctx, initialCRD, crdGVR)

	// Wait for schema to be available
	i.waitForSchema(baseURL, testGVR)

	// 4. Create test resource
	resource := &unstructured.Unstructured{Object: scenario.Resource}
	err = i.doApply(ctx, resource, testGVR)
	i.Require().NoError(err)
	defer i.doDelete(ctx, resource, testGVR)

	// 5. Verify initial metadata.fields
	resourceName := scenario.Resource["metadata"].(map[string]interface{})["name"].(string)
	url := fmt.Sprintf("%s/v1/%s?filter=metadata.name=%s&filter=metadata.namespace=default", baseURL, scenario.SchemaID, resourceName)

	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		fields := i.getResourceFields(c, url)
		i.verifyFieldsWithCollectT(c, fields, scenario.ExpectedInitial)
	}, 10*time.Second, 500*time.Millisecond, "Initial schema should be available")

	// 6. Update CRD
	// Must get current CRD for resourceVersion
	currentCRD, err := i.client.Resource(crdGVR).Get(ctx, scenario.CRDName, metav1.GetOptions{})
	i.Require().NoError(err)

	// Preserve resourceVersion when updating
	updatedCRD.SetResourceVersion(currentCRD.GetResourceVersion())

	_, err = i.client.Resource(crdGVR).Update(ctx, updatedCRD, metav1.UpdateOptions{})
	i.Require().NoError(err)

	// 7. Wait for schema refresh and verify updated metadata.fields
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		fields := i.getResourceFields(c, url)
		i.verifyFieldsWithCollectT(c, fields, scenario.ExpectedUpdated)
	}, 30*time.Second, 1*time.Second, "Schema should refresh with updated columns")
}

func (i *IntegrationSuite) parseGVRFromSchemaID(schemaID string) schema.GroupVersionResource {
	parts := strings.Split(schemaID, ".")
	i.Require().GreaterOrEqual(len(parts), 3, "schemaID should have at least 3 parts")

	// Last part is plural resource name (e.g., "zerotosomes")
	plural := parts[len(parts)-1]

	// Everything except last part is the group
	group := strings.Join(parts[:len(parts)-1], ".")

	return schema.GroupVersionResource{
		Group:    group,
		Version:  "v1",
		Resource: plural,
	}
}

func (i *IntegrationSuite) getResourceFields(t assert.TestingT, url string) []any {
	resp, err := http.Get(url)
	if !assert.NoError(t, err) {
		return nil
	}
	defer resp.Body.Close()

	if !assert.Equal(t, http.StatusOK, resp.StatusCode) {
		return nil
	}

	var parsed FieldsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); !assert.NoError(t, err) {
		return nil
	}

	if !assert.NotEmpty(t, parsed.Data) {
		return nil
	}

	return parsed.Data[0].Metadata.Fields
}

func (i *IntegrationSuite) verifyFieldsWithCollectT(c *assert.CollectT, actualFields []any, expectedRows [][]any) {
	if !assert.Len(c, expectedRows, 1, "Expected single row") {
		return
	}
	expected := expectedRows[0]

	if !assert.Len(c, actualFields, len(expected), "Field count mismatch") {
		return
	}

	for idx, expectedField := range expected {
		switch expectedField {
		case "$timestamp":
			_, err := time.Parse(time.RFC3339, fmt.Sprintf("%v", actualFields[idx]))
			assert.NoError(c, err, "Expected timestamp at index %d", idx)
		case "$skip":
			continue
		default:
			assert.Equal(c, expectedField, actualFields[idx], "Field mismatch at index %d", idx)
		}
	}
}
