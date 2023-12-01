package definitions

import (
	"fmt"
	"testing"
	"time"

	openapi_v2 "github.com/google/gnostic/openapiv2"
	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	wschemas "github.com/rancher/wrangler/pkg/schemas"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
)

func TestByID(t *testing.T) {
	schemas := types.EmptyAPISchemas()
	schemas.MustAddSchema(types.APISchema{
		Schema: &wschemas.Schema{
			ID:                "management.cattle.io.globalrole",
			CollectionMethods: []string{"get"},
			ResourceMethods:   []string{"get"},
		},
	})
	tests := []struct {
		name          string
		schemaName    string
		needsRefresh  bool
		wantObject    *types.APIObject
		wantError     bool
		wantErrorCode *int
	}{
		{
			name:         "global role definition",
			schemaName:   "management.cattle.io.globalrole",
			needsRefresh: true,
			wantObject: &types.APIObject{
				ID:   "management.cattle.io.globalrole",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "io.cattle.management.v2.GlobalRole",
					Definitions: map[string]definition{
						"io.cattle.management.v2.GlobalRole": {
							ResourceFields: map[string]definitionField{
								"apiVersion": {
									Type:        "string",
									Description: "The APIVersion of this resource",
								},
								"kind": {
									Type:        "string",
									Description: "The kind",
								},
								"metadata": {
									Type:        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
									Description: "The metadata",
								},
								"spec": {
									Type:        "io.cattle.management.v2.GlobalRole.spec",
									Description: "The spec for the project",
								},
							},
							Type:        "io.cattle.management.v2.GlobalRole",
							Description: "A Global Role V2 provides Global Permissions in Rancher",
						},
						"io.cattle.management.v2.GlobalRole.spec": {
							ResourceFields: map[string]definitionField{
								"clusterName": {
									Type:        "string",
									Description: "The name of the cluster",
									Required:    true,
								},
								"displayName": {
									Type:        "string",
									Description: "The UI readable name",
									Required:    true,
								},
								"newField": {
									Type:        "string",
									Description: "A new field not present in v1",
								},
								"notRequired": {
									Type:        "boolean",
									Description: "Some field that isn't required",
								},
							},
							Type:        "io.cattle.management.v2.GlobalRole.spec",
							Description: "The spec for the project",
						},
						"io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
							ResourceFields: map[string]definitionField{
								"annotations": {
									Type:        "map",
									SubType:     "string",
									Description: "annotations of the resource",
								},
								"name": {
									Type:        "string",
									SubType:     "",
									Description: "name of the resource",
								},
							},
							Type:        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
							Description: "Object Metadata",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client, err := buildDefaultDiscovery()
			require.Nil(t, err)
			handler := SchemaDefinitionHandler{
				client: client,
			}
			if !test.needsRefresh {
				handler.lastRefresh = time.Now()
			}
			request := types.APIRequest{
				Schemas: schemas,
				Name:    test.schemaName,
			}
			response, err := handler.ByIDHandler(&request)
			if test.wantError {
				require.Error(t, err)
				if test.wantErrorCode != nil {
					require.True(t, apierror.IsAPIError(err))
					apiErr, _ := err.(*apierror.APIError)
					require.Equal(t, *test.wantErrorCode, apiErr.Code.Status)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, *test.wantObject, response)
			}
		})
	}
}

func buildDefaultDiscovery() (*fakeDiscovery, error) {
	document, err := openapi_v2.ParseDocument([]byte(openapi_raw))
	if err != nil {
		return nil, fmt.Errorf("unable to parse openapi document %w", err)
	}
	groups := []*metav1.APIGroup{
		{
			Name: "management.cattle.io",
			PreferredVersion: metav1.GroupVersionForDiscovery{
				Version: "v2",
			},
		},
	}
	resources := []*metav1.APIResourceList{
		{
			GroupVersion: schema.GroupVersion{
				Group:   "management.cattle.io",
				Version: "v2",
			}.String(),
			APIResources: []metav1.APIResource{
				{
					Group:   "management.cattle.io",
					Kind:    "GlobalRole",
					Version: "v2",
				},
			},
		},
		{
			GroupVersion: schema.GroupVersion{
				Group:   "management.cattle.io",
				Version: "v1",
			}.String(),
			APIResources: []metav1.APIResource{
				{
					Group:   "management.cattle.io",
					Kind:    "GlobalRole",
					Version: "v2",
				},
			},
		},
	}
	return &fakeDiscovery{
		Groups:    groups,
		Resources: resources,
		Document:  document,
	}, nil
}

type fakeDiscovery struct {
	Groups            []*metav1.APIGroup
	Resources         []*metav1.APIResourceList
	Document          *openapi_v2.Document
	GroupResourcesErr error
	DocumentErr       error
}

// ServerGroupsAndResources is the only method we actually need for the test - just returns what is on the struct
func (f *fakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return f.Groups, f.Resources, f.GroupResourcesErr
}

// The rest of these methods are just here to conform to discovery.DiscoveryInterface
func (f *fakeDiscovery) RESTClient() restclient.Interface            { return nil }
func (f *fakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) { return nil, nil }
func (f *fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}
func (f *fakeDiscovery) ServerVersion() (*version.Info, error) { return nil, nil }
func (f *fakeDiscovery) OpenAPISchema() (*openapi_v2.Document, error) {
	return f.Document, f.DocumentErr
}
func (f *fakeDiscovery) OpenAPIV3() openapi.Client                { return nil }
func (f *fakeDiscovery) WithLegacy() discovery.DiscoveryInterface { return f }
