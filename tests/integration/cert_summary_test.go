package tests

import (
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
	"k8s.io/apiserver/pkg/authentication/user"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataWranglerCertDir = filepath.Join("testdata", "wrangler_certs")
)

// WranglerCertTestConfig defines the structure for list test YAML files
type WranglerCertTestConfig struct {
	SchemaID string `yaml:"schemaID"`
	Tests    []struct {
		Description    string              `yaml:"description"`
		User           string              `yaml:"user"`
		Query          string              `yaml:"query"`
		Expected       struct {
			State struct {
				Error bool `yaml:"error"`
				Name string `yaml:"name"`
				Message string `yaml:"message"`
				Transitioning bool `yaml:"transitioning"`
			}  `yaml:"state"`
		}  `yaml:"expected"`
	} `yaml:"tests"`
}

func (i *IntegrationSuite) TestWranglerCerts() {
	ctx := i.T().Context()

	manifestsFile := filepath.Join(testdataWranglerCertDir, "certificates.manifests.yaml")
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		fmt.Fprintf(os.Stderr, "QQQ: TestWranglerCerts: -i.doApply\n")
		i.normalizeTheObject(obj)
		return nil
		// return i.doApplyWithoutStatusUpdate(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	// Wait for pods to stabilize - restarting-pod needs time to restart once
	timeToWait := 5
	i.T().Log(fmt.Sprintf("Waiting for certs to stabilize (%d seconds)...", timeToWait))
	time.Sleep(time.Duration(timeToWait) * time.Second)

	
	i.runCertStateTest(ctx)
}


func (i *IntegrationSuite) runCertStateTest(ctx context.Context) {

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

	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
			UseTempDir:  true,
		},
		AuthMiddleware: authMiddleware,
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL
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
	matches, err := filepath.Glob(filepath.Join(testdataWranglerCertDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")

		config := i.readWranglerCertTestConfig(match)

		i.Run(name, func() {
			// Apply scenario-specific manifests if they exist
			scenarioManifestsFile := filepath.Join(testdataWranglerCertDir, name+".manifests.yaml")
			if _, err := os.Stat(scenarioManifestsFile); err == nil {
				i.doManifest(ctx, scenarioManifestsFile, i.doApply)
				defer i.doManifestReversed(ctx, scenarioManifestsFile, i.doDelete)
			}

			i.testWranglerCertScenario(ctx, config, baseURL, csvWriter)
		})
	}
}

func (i *IntegrationSuite) readWranglerCertTestConfig(testFile string) WranglerCertTestConfig {
	file, err := os.Open(testFile)
	i.Require().NoError(err)
	defer file.Close()

	var config WranglerCertTestConfig
	err = yaml.NewDecoder(file).Decode(&config)
	i.Require().NoError(err)
	return config
}

func (i *IntegrationSuite) testWranglerCertScenario(ctx context.Context, config WranglerCertTestConfig, baseURL string, csvWriter *csv.Writer) {
	for _, test := range config.Tests {
		i.Run(test.Description, func() {
			url := buildURLRaw(baseURL, config.SchemaID, "", test.Query)
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

			if http.StatusOK != resp.StatusCode {
				fmt.Fprintf(os.Stderr, "QQQ: status code %d on URL %s\n", resp.StatusCode, url)
			}
			i.Require().Equal(http.StatusOK, resp.StatusCode)

			// Read full response body for JSON saving
			bodyBytes, err := io.ReadAll(resp.Body)
			i.Require().NoError(err)

			type Response struct {
				Data     struct {
					Metadata struct {
						State struct {
							Error bool `json:"error"`
							Name string `json:"name"`
							Message string `json:"message"`
							Transitioning bool `json:"transitioning"`
						}  `json:"state"`
					}  `json:"metadata"`
				} `json:"data"`
			}
			var parsed Response
			err = json.Unmarshal(bodyBytes, &parsed)
			i.Require().NoError(err)

			// Save JSON response if csvWriter is provided
			if csvWriter != nil {
				jsonResp, err := formatJSONResponse(bodyBytes)
				if err == nil {
					jsonFileName := getJSONFileName(test.User, "default", test.Query)
					jsonFilePath := filepath.Join(jsonOutputDir, jsonFileName)
					_ = writeJSONResponse(csvWriter, test.User, url, jsonFilePath, jsonResp)
				}
			}

			assert.Equal(i.T(), test.Expected, parsed)
		})
	}
}

