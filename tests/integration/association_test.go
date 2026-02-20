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
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	testdataDir = filepath.Join("testdata", "associations")
)

func (i *IntegrationSuite) TestListAssociations() {
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

	matches, err := filepath.Glob(filepath.Join(testdataDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := filepath.Base(match)
		name = strings.TrimSuffix(name, ".test.yaml")
		i.Run(name, func() {
			i.testAssociationsScenarios(ctx, name, baseURL)
		})
	}
}

type relationshipManifest struct {
	Metadata struct {
		Name          string           `json:"name"`
		Namespace     string           `json:"namespace"`
		Relationships []map[string]any `json:"relationships"`
	} `json:"metadata"`
}

type relationshipList []relationshipManifest

type stateManifest struct {
	Metadata struct {
		Name      string         `json:"name"`
		Namespace string         `json:"namespace"`
		State     map[string]any `json:"state"`
	} `json:"metadata"`
}

type stateList []stateManifest

func (i *IntegrationSuite) testAssociationsScenarios(ctx context.Context, scenario string, baseURL string) {
	logrus.SetLevel(logrus.DebugLevel)
	scenarioManifests := filepath.Join(testdataDir, scenario+".manifests.yaml")
	scenarioTests := filepath.Join(testdataDir, scenario+".test.yaml")

	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, scenarioManifests, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		err := i.doApply(ctx, obj, gvr)
		return err
	})
	defer i.doManifestReversed(ctx, scenarioManifests, i.doDelete)

	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}

	testFile, err := os.Open(scenarioTests)
	i.Require().NoError(err)

	type TestConfig struct {
		SchemaID string `yaml:"schemaID"`
		Tests    []struct {
			Query    string         `yaml:"query"`
			Expected map[string]any `yaml:"expected"`
		} `yaml:"tests"`
	}
	var config TestConfig
	err = yaml.NewDecoder(testFile).Decode(&config)
	if err != nil {
		err2 := fmt.Errorf("Error loading file %s: %s\n", scenarioTests, err)
		i.Require().NoError(err2)
	}

	for _, test := range config.Tests {
		i.Run(test.Query, func() {
			url := fmt.Sprintf("%s/v1/%s?%s", baseURL, config.SchemaID, test.Query)
			fmt.Println(url)
			resp, err := http.Get(url)
			i.Require().NoError(err)
			defer resp.Body.Close()

			i.Require().Equal(http.StatusOK, resp.StatusCode)

			type AssociatedDataStruct struct {
				GVK struct {
					Group   string `json:"group"`
					Version string `json:"version"`
					Kind    string `json:"kind"`
				} `json:"gvk"`
				Data []struct {
					ChildName string `json:"childName"`
					State     struct {
						Error         string `json:"error"`
						Message       string `json:"message"`
						Name          string `json:"name"`
						Transitioning any    `json:"transitioning"`
					} `json:"state"`
				} `json:"data"`
			}

			type Response struct {
				Data []struct {
					ID       string `json:"id"`
					Metadata struct {
						Namespace      string                 `json:"namespace"`
						Name           string                 `json:"name"`
						AssociatedData []AssociatedDataStruct `json:"associatedData,omitempty"`
					} `json:"metadata"`
				} `json:"data"`
			}

			var parsedResponse Response
			err = json.NewDecoder(resp.Body).Decode(&parsedResponse)
			i.Require().NoError(err)

			// Compare the skeletal structure of expected and received
			// Many of the produced values are random, like pods named "foo-TAG"
			// And we don't know what the various fields in the state packet will be.
			// But we can assert they should all be non-empty (as strings).

			expectedDataRaw, ok := test.Expected["data"]
			i.Require().True(ok)
			expectedData, ok := expectedDataRaw.([]any)
			i.Require().True(ok)
			i.Require().Equal(len(expectedData), len(parsedResponse.Data))
			for i1, exp1Raw := range expectedData {
				expDataItem, ok := exp1Raw.(map[string]any)
				i.Require().True(ok)
				got1 := parsedResponse.Data[i1]
				i.Require().True(ok)
				expIDRaw, ok := expDataItem["id"]
				i.Require().True(ok)
				expID, ok := expIDRaw.(string)
				i.Require().True(ok)
				i.Require().Equal(expID, got1.ID)

				got1Metadata := got1.Metadata
				expMetadataRaw, ok := expDataItem["metadata"]
				i.Require().True(ok)
				expMetadata, ok := expMetadataRaw.(map[string]any)
				i.Require().True(ok)
				expNameRaw, ok := expMetadata["name"]
				i.Require().True(ok)
				expName, ok := expNameRaw.(string)
				i.Require().True(ok)
				i.Require().Equal(expName, got1Metadata.Name)

				expNamespaceRaw, ok := expMetadata["namespace"]
				i.Require().True(ok)
				expNamespace, ok := expNamespaceRaw.(string)
				i.Require().True(ok)
				i.Require().Equal(expNamespace, got1Metadata.Namespace)

				expADRaw, ok := expMetadata["associatedData"]
				if strings.Contains(test.Query, "includeAssociatedData=true") {
					i.Require().True(ok)
				} else {
					i.Require().False(ok)
					continue
				}
				expAD, ok := expADRaw.([]any)
				i.Require().True(ok)
				i.Require().Equal(len(expAD), len(got1Metadata.AssociatedData))

				for i2, expAssocDataRaw := range expAD {
					expAssocData, ok := expAssocDataRaw.(map[string]any)
					i.Require().True(ok)
					gotAssocData := got1.Metadata.AssociatedData[i2]
					expGVKRaw, ok := expAssocData["gvk"]
					i.Require().True(ok)
					expGVK, ok := expGVKRaw.(map[string]any)
					i.Require().Equal(expGVK["group"], gotAssocData.GVK.Group)
					i.Require().Equal(expGVK["version"], gotAssocData.GVK.Version)
					i.Require().Equal(expGVK["kind"], gotAssocData.GVK.Kind)

					// Just verify that we have at least one associated state block
					i.Require().Less(0, len(gotAssocData.Data))
					expDataTemplatesRaw, ok := expAssocData["data"]
					i.Require().True(ok)
					expDataTemplates, ok := expDataTemplatesRaw.([]any)
					i.Require().True(ok)
					i.Require().Less(0, len(expDataTemplates))
					expDataTemplate, ok := (expDataTemplates[0]).(map[string]any)
					expChildNameBaseRaw, ok := expDataTemplate["childName"]
					i.Require().True(ok)
					expChildNameBase, ok := expChildNameBaseRaw.(string)
					i.Require().True(ok)

					for _, gotAssocData1 := range gotAssocData.Data {
						i.Require().True(strings.HasPrefix(gotAssocData1.ChildName, expChildNameBase))
						gotAssocDataState := gotAssocData1.State
						i.Require().Less(0, len(gotAssocDataState.Error))
						i.Require().Less(0, len(gotAssocDataState.Message))
						i.Require().Less(0, len(gotAssocDataState.Name))
						i.Require().Less(0, len(gotAssocDataState.Transitioning.(string)))
					}
				}
			}

		})
	}
}
