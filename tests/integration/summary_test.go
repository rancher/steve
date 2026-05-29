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
	testdataSummaryDir   = filepath.Join("testdata", "summary")
	jsonSummaryOutputDir = filepath.Join(testdataSummaryDir, "json")
)

type summaryTotalType struct {
	Total     int            `yaml:"total"`
	Namespace map[string]int `json:"namespace,omitempty" yaml:"namespace,omitempty"`
}

type summaryExpectType struct {
	Counts   map[string]summaryTotalType `yaml:"counts"`
	Property string                      `yaml:"property"`
}

type expectedDataAndSummaryType struct {
	Data    []int               `yaml:"data"` // do *not* omit
	Summary []summaryExpectType `yaml:"summary"`
}

// SummaryTestConfig defines the structure for summary test YAML files
type SummaryTestConfig struct {
	SchemaID string `yaml:"schemaID"`
	Tests    []struct {
		Description string                     `yaml:"description"`
		User        string                     `yaml:"user"`
		Namespace   string                     `yaml:"namespace"`
		Query       string                     `yaml:"query"`
		Expect      expectedDataAndSummaryType `yaml:"expect"`
	} `yaml:"tests"`
}

func (i *IntegrationSuite) TestSummary() {
	ctx := i.T().Context()

	// Apply common manifests once for both test modes
	commonManifestsFile := filepath.Join(testdataSummaryDir, "common.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, commonManifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	// Cleanup common manifests after all tests complete
	defer i.doManifestReversed(ctx, commonManifestsFile, i.doDelete)

	// Run SQL mode first, then non-SQL mode sequentially
	i.Run("SQL", func() {
		i.runSummaryTest(ctx, true, gvrs)
	})
	i.Run("NonSQL", func() {
		i.runSummaryTest(ctx, false, gvrs)
	})
}

func (i *IntegrationSuite) runSummaryTest(ctx context.Context, sqlCache bool, gvrs map[k8sschema.GroupVersionResource]struct{}) {

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

	// Wait for schemas to be ready (gvrs already applied in TestSummary)
	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	// Set up JSON output directory and CSV writer
	var csvWriter *csv.Writer
	var csvFile *os.File
	if os.Getenv("SAVE_JSON_RESPONSES") == "true" {
		var err error
		csvWriter, csvFile, err = setupJSONSummaryOutput()
		if err == nil && csvFile != nil {
			defer csvFile.Close()
			defer csvWriter.Flush()
		}
	}

	// Find all test YAML files
	matches, err := filepath.Glob(filepath.Join(testdataSummaryDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")

		config := i.readSummaryTestConfig(match)

		i.Run(name, func() {
			// Apply scenario-specific manifests if they exist
			scenarioManifestsFile := filepath.Join(testdataSummaryDir, name+".manifests.yaml")
			if _, err := os.Stat(scenarioManifestsFile); err == nil {
				i.doManifest(ctx, scenarioManifestsFile, i.doApply)
				defer i.doManifestReversed(ctx, scenarioManifestsFile, i.doDelete)
			}

			i.testSummaryScenario(ctx, config, baseURL, sqlCache, csvWriter)
		})
	}
}

func (i *IntegrationSuite) readSummaryTestConfig(testFile string) SummaryTestConfig {
	file, err := os.Open(testFile)
	i.Require().NoError(err)
	defer file.Close()

	var config SummaryTestConfig
	err = yaml.NewDecoder(file).Decode(&config)
	i.Require().NoError(err)
	return config
}

func (i *IntegrationSuite) testSummaryScenario(ctx context.Context, config SummaryTestConfig, baseURL string, sqlCache bool, csvWriter *csv.Writer) {
	// Track continue token and revision across tests in this scenario
	var lastContinueToken string
	var lastRevision string
	if !sqlCache {
		i.T().Skip("Skipping non-sql tests")
	}

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
			//TODO: Delete this:
			//fmt.Fprintf(os.Stderr, "body:\n[\n%s\n]\n", string(bodyBytes))

			type Response struct {
				Data     []responseItem      `json:"data"`
				Summary  []summaryExpectType `json:"summary"`
				Continue string              `json:"continue"`
				Revision string              `json:"revision"`
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
					jsonFilePath := filepath.Join(jsonSummaryOutputDir, jsonFileName)
					_ = writeJSONResponse(csvWriter, test.User, url, jsonFilePath, jsonResp)
				}
			}
			// We don't care about matching the data in summary-testing, just verify the array size
			assert.Equal(i.T(), len(test.Expect.Data), len(parsed.Data))
			i.assertSummaryIsEqual(test.Expect.Summary, parsed.Summary)
		})
	}
}

func (i *IntegrationSuite) assertSummaryIsEqual(expected []summaryExpectType, received []summaryExpectType) {

	//fmt.Fprintf(os.Stderr, "expected: %v\n\n, received: %v\n\n", expected, received)
	assert.Equal(i.T(), expected, received)
	// assert.Equal(i.T(), len(expected) + 100, len(received), "summary length mismatch")
}

// JSON output helper functions

func setupJSONSummaryOutput() (*csv.Writer, *os.File, error) {
	// Create JSON output directory
	err := os.MkdirAll(jsonSummaryOutputDir, 0755)
	if err != nil {
		return nil, nil, err
	}

	// Create CSV index file
	csvPath := filepath.Join(testdataSummaryDir, "output.csv")
	csvFile, err := os.OpenFile(csvPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, err
	}

	csvWriter := csv.NewWriter(bufio.NewWriter(csvFile))
	csvWriter.Write([]string{"user", "url", "response"})

	return csvWriter, csvFile, nil
}
