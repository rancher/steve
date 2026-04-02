package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func (i *IntegrationSuite) TestSQLCacheSecrets() {
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
	testFile := filepath.Join("testdata", "sqlcache", "secrets.test.yaml")

	i.testSecretsScenario(ctx, testFile, baseURL)
}

func (i *IntegrationSuite) testSecretsScenario(ctx context.Context, testFile string, baseURL string) {
	type SecretTestConfig struct {
		SchemaID  string `yaml:"schemaID"`
		Namespace string `yaml:"namespace"`
		Tests     []struct {
			Name          string   `yaml:"name"`
			Query         string   `yaml:"query"`
			ExpectedNames []string `yaml:"expectedNames"`
		} `yaml:"tests"`
	}

	var config SecretTestConfig
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})

	i.doManifestWithHeader(ctx, testFile,
		func(ctx context.Context, header map[string]any) error {
			b, _ := yaml.Marshal(header)
			return yaml.Unmarshal(b, &config)
		},
		func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
			gvrs[gvr] = struct{}{}
			return i.doApply(ctx, obj, gvr)
		},
	)
	defer i.doManifestReversed(ctx, testFile, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	defer i.maybeStopAndDebug(baseURL)

	time.Sleep(2 * time.Second)

	for _, test := range config.Tests {
		i.Run(test.Name, func() {
			url := fmt.Sprintf("%s/v1/%s/%s?%s", baseURL, config.SchemaID, config.Namespace, test.Query)
			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			i.Require().Equal(http.StatusOK, resp.StatusCode)

			type Response struct {
				Data []struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
				} `json:"data"`
			}
			var parsed Response
			err = json.NewDecoder(resp.Body).Decode(&parsed)
			i.Require().NoError(err)

			var names []string
			for _, item := range parsed.Data {
				names = append(names, item.Metadata.Name)
			}
			i.Require().Equal(test.ExpectedNames, names)
		})
	}
}
