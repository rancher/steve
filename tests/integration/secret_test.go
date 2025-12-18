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

var (
	testdataSecretsDir = filepath.Join("testdata", "secrets")
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
	} `yaml:"tests"`
}

func (i *IntegrationSuite) TestSecrets() {
	i.runSecretsTest(true)  // SQL mode
}

func (i *IntegrationSuite) TestSecretsNonSQL() {
	i.runSecretsTest(false) // Non-SQL mode
}

func (i *IntegrationSuite) runSecretsTest(sqlCache bool) {
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

	// Apply secrets manifests
	manifestsFile := filepath.Join(testdataSecretsDir, "secrets.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	// Only cleanup in non-SQL mode (runs second, after SQL mode)
	if !sqlCache {
		defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)
	}

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	// Find all test YAML files
	matches, err := filepath.Glob(filepath.Join(testdataSecretsDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")

		// Skip pagination tests for SQL mode (they use limit/continue which is non-SQL only)
		if sqlCache && name == "pagination" {
			continue
		}
		// Skip non-pagination tests for non-SQL mode
		if !sqlCache && name != "pagination" {
			continue
		}

		i.Run(name, func() {
			i.testSecretScenario(ctx, match, baseURL, sqlCache)
		})
	}
}

func (i *IntegrationSuite) testSecretScenario(ctx context.Context, testFile string, baseURL string, sqlCache bool) {
	file, err := os.Open(testFile)
	i.Require().NoError(err)
	defer file.Close()

	var config ListTestConfig
	err = yaml.NewDecoder(file).Decode(&config)
	i.Require().NoError(err)

	// Track continue token and revision across tests in this scenario
	var lastContinueToken string
	var lastRevision string

	for _, test := range config.Tests {
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

			type Response struct {
				Data     []responseItem `json:"data"`
				Continue string         `json:"continue"`
				Revision string         `json:"revision"`
			}
			var parsed Response
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			// Store continue token and revision for subsequent tests
			if parsed.Continue != "" {
				lastContinueToken = parsed.Continue
			}
			if parsed.Revision != "" {
				lastRevision = parsed.Revision
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
