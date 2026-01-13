package tests

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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

var (
	testdataListDir = filepath.Join("testdata", "list")
	jsonOutputDir      = filepath.Join("testdata", "list", "json")
)

// ListTestConfig defines the structure for list test YAML files
type ListTestConfig struct {
	SchemaID string `yaml:"schemaID"`
	Tests    []struct {
		Description    string              `yaml:"description"`
		User           string              `yaml:"user"`
		Namespace      string              `yaml:"namespace"`
		Query          string              `yaml:"query"`
		Expect         []map[string]string `yaml:"expect"`
		ExpectExcludes bool                `yaml:"expectExcludes"`
		ExpectContains bool                `yaml:"expectContains"`
		SQLOnly        bool                `yaml:"sqlOnly"`    // Skip in non-SQL mode
		NonSQLOnly     bool                `yaml:"nonSQLOnly"` // Skip in SQL mode
	} `yaml:"tests"`
}

func (i *IntegrationSuite) TestList() {
	i.runListTest(true)  // SQL mode
}

func (i *IntegrationSuite) TestListNonSQL() {
	i.runListTest(false) // Non-SQL mode
}

func (i *IntegrationSuite) runListTest(sqlCache bool) {
	ctx := i.T().Context()

	// Custom authenticator: use impersonation if header present, otherwise admin
	impersonateOrAdmin := func(req *http.Request) (user.Info, bool, error) {
		info, ok, err := auth.Impersonation(req)
		if ok || err != nil {
			return info, ok, err
		}
		// No impersonation header, fall back to admin
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

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL

	// Apply common manifests (shared across all scenarios)
	commonManifestsFile := filepath.Join(testdataListDir, "common.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, commonManifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	// Only cleanup in non-SQL mode (runs second, after SQL mode)
	if !sqlCache {
		defer i.doManifestReversed(ctx, commonManifestsFile, i.doDelete)
	}

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	// Set up JSON output directory and CSV writer
	var csvWriter *csv.Writer
	var csvFile *os.File
	if os.Getenv("SAVE_JSON_RESPONSES") == "true" {
		var err error
		csvWriter, csvFile, err = setupJSONOutput()
		if err == nil && csvFile != nil {
			defer csvFile.Close()
			defer csvWriter.Flush()
		}
	}

	// Find all test YAML files
	matches, err := filepath.Glob(filepath.Join(testdataListDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")

		config := i.readListTestConfig(match)

		i.Run(name, func() {
			// Apply scenario-specific manifests if they exist
			scenarioManifestsFile := filepath.Join(testdataListDir, name+".manifests.yaml")
			if _, err := os.Stat(scenarioManifestsFile); err == nil {
				i.doManifest(ctx, scenarioManifestsFile, i.doApply)
				defer i.doManifestReversed(ctx, scenarioManifestsFile, i.doDelete)
			}

			i.testListScenario(ctx, config, baseURL, sqlCache, csvWriter)
		})
	}
}

func (i *IntegrationSuite) readListTestConfig(testFile string) ListTestConfig {
	file, err := os.Open(testFile)
	i.Require().NoError(err)
	defer file.Close()

	var config ListTestConfig
	err = yaml.NewDecoder(file).Decode(&config)
	i.Require().NoError(err)
	return config
}

func (i *IntegrationSuite) testListScenario(ctx context.Context, config ListTestConfig, baseURL string, sqlCache bool, csvWriter *csv.Writer) {
	// Track continue token and revision across tests in this scenario
	var lastContinueToken string
	var lastRevision string

	for _, test := range config.Tests {
		// Skip SQL-only tests when running in non-SQL mode
		if !sqlCache && test.SQLOnly {
			continue
		}
		// Skip non-SQL-only tests when running in SQL mode
		if sqlCache && test.NonSQLOnly {
			continue
		}

		i.Run(test.Description, func() {
			query := test.Query

			// Replace nondeterministic placeholders with actual values from previous responses
			if strings.Contains(query, "nondeterministictoken") {
				query = strings.Replace(query, "nondeterministictoken", lastContinueToken, 1)
			}
			if strings.Contains(query, "nondeterministicint") {
				query = strings.Replace(query, "nondeterministicint", lastRevision, 1)
			}

			// Convert project names to project IDs for projectsornamespaces parameter
			query = convertProjectsOrNamespaces(query)

			// Convert labelSelector/fieldSelector to filter format for SQL mode
			if sqlCache {
				query = convertQueryForSQLCache(query)
			}
			url := buildURLRaw(baseURL, config.SchemaID, test.Namespace, query)
			fmt.Println(url)

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			i.Require().NoError(err)

			// User impersonation via headers
			if test.User != "" {
				req.Header.Set("Impersonate-User", test.User)
			}

			resp, err := http.DefaultClient.Do(req)
			i.Require().NoError(err)
			defer resp.Body.Close()

			i.Require().Equal(http.StatusOK, resp.StatusCode)

			// Read full response body for JSON saving
			bodyBytes, err := io.ReadAll(resp.Body)
			i.Require().NoError(err)

			type Response struct {
				Data     []responseItem `json:"data"`
				Continue string         `json:"continue"`
				Revision string         `json:"revision"`
			}
			var parsed Response
			err = json.Unmarshal(bodyBytes, &parsed)
			i.Require().NoError(err)

			// Store continue token and revision for subsequent tests
			if parsed.Continue != "" {
				lastContinueToken = parsed.Continue
			}
			if parsed.Revision != "" {
				lastRevision = parsed.Revision
			}

			// Save JSON response if csvWriter is provided
			if csvWriter != nil {
				jsonResp, err := formatJSONResponse(bodyBytes)
				if err == nil {
					jsonFileName := getJSONFileName(test.User, test.Namespace, test.Query)
					jsonFilePath := filepath.Join(jsonOutputDir, jsonFileName)
					_ = writeJSONResponse(csvWriter, test.User, url, jsonFilePath, jsonResp)
				}
			}

			if test.ExpectContains {
				i.assertListContains(test.Expect, parsed.Data)
			} else if test.ExpectExcludes {
				i.assertListExcludes(test.Expect, parsed.Data)
			} else {
				i.assertListIsEqual(test.Expect, parsed.Data)
			}
		})
	}
}

func buildURLRaw(baseURL, schemaID, namespace, query string) string {
	var url string
	if namespace != "" {
		url = fmt.Sprintf("%s/v1/%s/%s", baseURL, schemaID, namespace)
	} else {
		url = fmt.Sprintf("%s/v1/%s", baseURL, schemaID)
	}
	if query != "" {
		url += "?" + query
	}
	return url
}

// convertQueryForSQLCache converts labelSelector and fieldSelector params to filter format
// This mirrors the logic in steve_api_test.go#L2661-L2681
func convertQueryForSQLCache(query string) string {
	parts := strings.Split(query, "&")
	changed := false

	for j, part := range parts {
		if strings.HasPrefix(part, "labelSelector=") {
			// labelSelector=test-label=2 -> filter=metadata.labels[test-label]=2
			rest := strings.TrimPrefix(part, "labelSelector=")
			eqIdx := strings.Index(rest, "=")
			if eqIdx > 0 {
				labelName := rest[:eqIdx]
				labelValue := rest[eqIdx+1:]
				parts[j] = fmt.Sprintf("filter=metadata.labels[%s]=%s", labelName, labelValue)
				changed = true
			}
		} else if strings.HasPrefix(part, "fieldSelector=") {
			// fieldSelector=metadata.namespace=test-ns-1 -> filter=metadata.namespace=test-ns-1
			// fieldSelector=metadata.name=test1 -> filter=metadata.name=test1
			rest := strings.TrimPrefix(part, "fieldSelector=")
			eqIdx := strings.Index(rest, "=")
			if eqIdx > 0 {
				field := rest[:eqIdx]
				value := rest[eqIdx+1:]
				parts[j] = fmt.Sprintf("filter=%s=%s", field, value)
				changed = true
			}
		}
	}

	if changed {
		return strings.Join(parts, "&")
	}
	return query
}

// convertProjectsOrNamespaces - no conversion needed since we use test-prj-X directly
// This mirrors the logic in steve_api_test.go#L2732-L2751
func convertProjectsOrNamespaces(query string) string {
	// No conversion needed - test-prj-X is used directly as the label value
	return query
}

type responseItem struct {
	ID       string `json:"id"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
}

func (r responseItem) getName() string {
	return r.Metadata.Name
}

func (r responseItem) getNamespace() string {
	return r.Metadata.Namespace
}

func (i *IntegrationSuite) assertListIsEqual(expected []map[string]string, received []responseItem) {
	assert.Equal(i.T(), len(expected), len(received), "list length mismatch")

	includeNamespace := false
	if len(expected) > 0 {
		_, includeNamespace = expected[0]["namespace"]
	}

	receivedSubset := make([]map[string]string, len(received))
	for idx, r := range received {
		vals := map[string]string{"name": r.getName()}
		if includeNamespace {
			vals["namespace"] = r.getNamespace()
		}
		receivedSubset[idx] = vals
	}

	// Build expected subset matching received format
	expectedSubset := make([]map[string]string, len(expected))
	for idx, e := range expected {
		vals := map[string]string{"name": e["name"]}
		if includeNamespace {
			vals["namespace"] = e["namespace"]
		}
		expectedSubset[idx] = vals
	}

	assert.Equal(i.T(), expectedSubset, receivedSubset, "list contents do not match")
}

func (i *IntegrationSuite) assertListContains(expected []map[string]string, received []responseItem) {
	assert.GreaterOrEqual(i.T(), len(received), len(expected), "received list too short")

	for _, e := range expected {
		found := false
		for _, r := range received {
			nameMatch := r.getName() == e["name"]
			nsMatch := e["namespace"] == "" || r.getNamespace() == e["namespace"]
			if nameMatch && nsMatch {
				found = true
				break
			}
		}
		assert.True(i.T(), found, "expected item not found: %v", e)
	}
}

func (i *IntegrationSuite) assertListExcludes(expected []map[string]string, received []responseItem) {
	for _, e := range expected {
		found := false
		for _, r := range received {
			nameMatch := r.getName() == e["name"]
			nsMatch := e["namespace"] == "" || r.getNamespace() == e["namespace"]
			if nameMatch && nsMatch {
				found = true
				break
			}
		}
		assert.False(i.T(), found, "unexpected item found: %v", e)
	}
}

// JSON output helper functions

func setupJSONOutput() (*csv.Writer, *os.File, error) {
	// Create JSON output directory
	err := os.MkdirAll(jsonOutputDir, 0755)
	if err != nil {
		return nil, nil, err
	}

	// Create CSV index file
	csvPath := filepath.Join(testdataListDir, "output.csv")
	csvFile, err := os.OpenFile(csvPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, err
	}

	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	csvWriter.Write([]string{"user", "url", "response"})

	return csvWriter, csvFile, nil
}

func getJSONFileName(user, ns, query string) string {
	if user == "" {
		user = "none"
	}
	if ns == "" {
		ns = "none"
	}
	if query == "" {
		query = "none"
	} else {
		query = strings.ReplaceAll(query, "/", "%2F")
		query = strings.ReplaceAll(query, "?", "%3F")
		query = strings.ReplaceAll(query, "&", "%26")
	}
	return user + "_" + ns + "_" + query + ".json"
}

func formatJSONResponse(bodyBytes []byte) ([]byte, error) {
	var mapResp map[string]interface{}
	err := json.Unmarshal(bodyBytes, &mapResp)
	if err != nil {
		return nil, err
	}

	// Normalize non-deterministic fields for comparison
	mapResp["revision"] = "100"
	if _, ok := mapResp["continue"]; ok {
		mapResp["continue"] = "CONTINUE_TOKEN"
	}

	// Normalize data items
	data, ok := mapResp["data"].([]interface{})
	if ok {
		for i := range data {
			item, ok := data[i].(map[string]interface{})
			if !ok {
				continue
			}
			metadata, ok := item["metadata"].(map[string]interface{})
			if !ok {
				continue
			}
			// Remove non-deterministic fields
			delete(metadata, "creationTimestamp")
			delete(metadata, "managedFields")
			delete(metadata, "uid")
			delete(metadata, "resourceVersion")
		}
		mapResp["data"] = data
	}

	return json.MarshalIndent(mapResp, "", "  ")
}

func writeJSONResponse(csvWriter *csv.Writer, user, url, path string, resp []byte) error {
	jsonFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	_, err = jsonFile.Write(resp)
	if err != nil {
		return err
	}

	csvWriter.Write([]string{user, url, fmt.Sprintf("[%s](%s)", filepath.Base(path), path)})
	return nil
}
