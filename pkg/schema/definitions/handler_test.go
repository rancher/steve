package definitions

import (
	"fmt"
	"testing"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v3/pkg/generic/fake"
	wschemas "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/util/proto"
)

func TestRefresh(t *testing.T) {
	defaultDocument, err := openapi_v2.ParseDocument([]byte(openapi_raw))
	require.NoError(t, err)
	defaultModels, err := proto.NewOpenAPIData(defaultDocument)
	require.NoError(t, err)

	crds, err := getCRDs()
	require.NoError(t, err)

	userAttributesV2 := getJSONSchema(crds, "userattributes.management.cattle.io", "v2")
	require.NotNil(t, userAttributesV2)

	nullableV2 := getJSONSchema(crds, "nullable.management.cattle.io", "v2")
	require.NotNil(t, nullableV2)

	tests := []struct {
		name             string
		openapiError     error
		crdListError     error
		serverGroupsErr  error
		useBadOpenApiDoc bool
		nilGroups        bool
		wantModels       proto.Models
		wantGVKModels    map[string]gvkModel
		wantError        bool
	}{
		{
			name:       "success",
			wantModels: defaultModels,
			wantGVKModels: map[string]gvkModel{
				"configmap": {
					ModelName: "io.k8s.api.core.v1.ConfigMap",
					Schema:    defaultModels.LookupModel("io.k8s.api.core.v1.ConfigMap"),
				},
				"management.cattle.io.deprecatedresource": {
					ModelName: "io.cattle.management.v1.DeprecatedResource",
					Schema:    defaultModels.LookupModel("io.cattle.management.v1.DeprecatedResource"),
				},
				"management.cattle.io.globalrole": {
					ModelName: "io.cattle.management.v2.GlobalRole",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.GlobalRole"),
				},
				"management.cattle.io.newresource": {
					ModelName: "io.cattle.management.v2.NewResource",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.NewResource"),
				},
				"noversion.cattle.io.resource": {
					ModelName: "io.cattle.noversion.v1.Resource",
					Schema:    defaultModels.LookupModel("io.cattle.noversion.v1.Resource"),
				},
				"missinggroup.cattle.io.resource": {
					ModelName: "io.cattle.missinggroup.v1.Resource",
					Schema:    defaultModels.LookupModel("io.cattle.missinggroup.v1.Resource"),
				},
				"management.cattle.io.userattribute": {
					ModelName: "io.cattle.management.v2.UserAttribute",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.UserAttribute"),
					CRD:       userAttributesV2,
				},
				"management.cattle.io.nullable": {
					ModelName: "io.cattle.management.v2.Nullable",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.Nullable"),
					CRD:       nullableV2,
				},
				"management.cattle.io.schemaless": {
					ModelName: "io.cattle.management.v2.Schemaless",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.Schemaless"),
					CRD:       nil,
				},
			},
		},
		{
			name:         "error - openapi doc unavailable",
			openapiError: fmt.Errorf("server unavailable"),
			wantError:    true,
		},
		{
			name:         "error - crd cache list error",
			crdListError: fmt.Errorf("error from cache"),
			wantError:    true,
		},
		{
			name:             "error - unable to parse openapi doc",
			useBadOpenApiDoc: true,
			wantError:        true,
		},
		{
			name:            "error - unable to retrieve groups and resources",
			serverGroupsErr: fmt.Errorf("server not available"),
			wantError:       true,
		},
		{
			name:       "no groups or error from server",
			nilGroups:  true,
			wantModels: defaultModels,
			wantGVKModels: map[string]gvkModel{
				"configmap": {
					ModelName: "io.k8s.api.core.v1.ConfigMap",
					Schema:    defaultModels.LookupModel("io.k8s.api.core.v1.ConfigMap"),
				},
				"management.cattle.io.deprecatedresource": {
					ModelName: "io.cattle.management.v1.DeprecatedResource",
					Schema:    defaultModels.LookupModel("io.cattle.management.v1.DeprecatedResource"),
				},
				// GlobalRole is now v1 instead of v2
				"management.cattle.io.globalrole": {
					ModelName: "io.cattle.management.v1.GlobalRole",
					Schema:    defaultModels.LookupModel("io.cattle.management.v1.GlobalRole"),
				},
				"management.cattle.io.newresource": {
					ModelName: "io.cattle.management.v2.NewResource",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.NewResource"),
				},
				"noversion.cattle.io.resource": {
					ModelName: "io.cattle.noversion.v1.Resource",
					Schema:    defaultModels.LookupModel("io.cattle.noversion.v1.Resource"),
				},
				"missinggroup.cattle.io.resource": {
					ModelName: "io.cattle.missinggroup.v1.Resource",
					Schema:    defaultModels.LookupModel("io.cattle.missinggroup.v1.Resource"),
				},
				"management.cattle.io.userattribute": {
					ModelName: "io.cattle.management.v2.UserAttribute",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.UserAttribute"),
					CRD:       userAttributesV2,
				},
				"management.cattle.io.nullable": {
					ModelName: "io.cattle.management.v2.Nullable",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.Nullable"),
					CRD:       nullableV2,
				},
				"management.cattle.io.schemaless": {
					ModelName: "io.cattle.management.v2.Schemaless",
					Schema:    defaultModels.LookupModel("io.cattle.management.v2.Schemaless"),
					CRD:       nil,
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client, err := buildDefaultDiscovery()
			client.DocumentErr = test.openapiError
			client.GroupsErr = test.serverGroupsErr
			if test.useBadOpenApiDoc {
				schema := client.Document.Definitions.AdditionalProperties[0]
				schema.Value.Type = &openapi_v2.TypeItem{
					Value: []string{"multiple", "entries"},
				}
			}
			if test.nilGroups {
				client.Groups = nil
			}
			require.Nil(t, err)
			baseSchemas := types.EmptyAPISchemas()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			crds, err := getCRDs()
			require.NoError(t, err)
			crdCache := fake.NewMockNonNamespacedCacheInterface[*apiextv1.CustomResourceDefinition](ctrl)
			if test.crdListError != nil {
				crdCache.EXPECT().List(labels.Everything()).Return(nil, test.crdListError).AnyTimes()
			} else {
				crdCache.EXPECT().List(labels.Everything()).Return(crds, nil).AnyTimes()
			}

			handler := NewSchemaDefinitionHandler(baseSchemas, crdCache, client)
			err = handler.Refresh()
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			handler.lock.RLock()
			defer handler.lock.RUnlock()
			require.Equal(t, test.wantModels, handler.models)
			require.Equal(t, test.wantGVKModels, handler.gvkModels)
		})

	}
}

func Test_byID(t *testing.T) {
	discoveryClient, err := buildDefaultDiscovery()
	require.NoError(t, err)

	schemas := types.EmptyAPISchemas()
	addSchema := func(names ...string) {
		for _, name := range names {
			schemas.MustAddSchema(types.APISchema{
				Schema: &wschemas.Schema{
					ID:                name,
					CollectionMethods: []string{"get"},
					ResourceMethods:   []string{"get"},
				},
			})
		}
	}

	intPtr := func(input int) *int {
		return &input
	}
	builtinSchema := types.APISchema{
		Schema: &wschemas.Schema{
			ID:                "builtin",
			Description:       "some builtin type",
			CollectionMethods: []string{"get"},
			ResourceMethods:   []string{"get"},
			ResourceFields: map[string]wschemas.Field{
				"complex": {
					Type:        "map[string]",
					Description: "some complex field",
				},
				"complexArray": {
					Type:        "array[string]",
					Description: "some complex array field",
				},
				"complexRef": {
					Type:        "reference[complex]",
					Description: "some complex reference field",
				},
				"simple": {
					Type:        "string",
					Description: "some simple field",
					Required:    true,
				},
				"leftBracket": {
					Type:        "test[",
					Description: "some field with a open bracket but no close bracket",
				},
			},
		},
	}
	addSchema(
		"configmap",
		"management.cattle.io.globalrole",
		"management.cattle.io.missingfrommodel",
		"management.cattle.io.notakind",
		"management.cattle.io.nullable",
		"management.cattle.io.userattribute",
		"management.cattle.io.deprecatedresource",
	)
	baseSchemas := types.EmptyAPISchemas()
	baseSchemas.MustAddSchema(builtinSchema)
	schemas.MustAddSchema(builtinSchema)

	tests := []struct {
		name          string
		schemaName    string
		skipRefresh   bool
		wantObject    *types.APIObject
		wantError     bool
		wantErrorCode *int
	}{
		{
			// ConfigMaps is NOT a CRD but it is defined in OpenAPI V2
			name:       "configmap",
			schemaName: "configmap",
			wantObject: &types.APIObject{
				ID:   "configmap",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "io.k8s.api.core.v1.ConfigMap",
					Definitions: map[string]definition{
						"io.k8s.api.core.v1.ConfigMap": {
							Type:        "io.k8s.api.core.v1.ConfigMap",
							Description: "ConfigMap holds configuration data for pods to consume.",
							ResourceFields: map[string]definitionField{
								"apiVersion": {
									Type:        "string",
									Description: "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
								},
								"kind": {
									Type:        "string",
									Description: "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
								},
								"metadata": {
									Type:        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
									Description: "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata",
								},
								"binaryData": {
									Type:        "map",
									SubType:     "string",
									Description: "BinaryData contains the binary data. Each key must consist of alphanumeric characters, '-', '_' or '.'. BinaryData can contain byte sequences that are not in the UTF-8 range. The keys stored in BinaryData must not overlap with the ones in the Data field, this is enforced during validation process. Using this field will require 1.10+ apiserver and kubelet.",
								},
								"data": {
									Type:        "map",
									SubType:     "string",
									Description: "Data contains the configuration data. Each key must consist of alphanumeric characters, '-', '_' or '.'. Values with non-UTF-8 byte sequences must use the BinaryData field. The keys stored in Data must not overlap with the keys in the BinaryData field, this is enforced during validation process.",
								},
								"immutable": {
									Type:        "boolean",
									Description: "Immutable, if set to true, ensures that data stored in the ConfigMap cannot be updated (only object metadata can be modified). If not set to true, the field can be modified at any time. Defaulted to nil.",
								},
							},
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
		{
			// Nullable has fields that are not representable in OpenAPI V2
			// and requires the CRD information to be merged
			name:       "nullable",
			schemaName: "management.cattle.io.nullable",
			wantObject: &types.APIObject{
				ID:   "management.cattle.io.nullable",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "io.cattle.management.v2.Nullable",
					Definitions: map[string]definition{
						"io.cattle.management.v2.Nullable": {
							Type:        "io.cattle.management.v2.Nullable",
							Description: "",
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
									Type: "io.cattle.management.v2.Nullable.spec",
								},
							},
						},
						"io.cattle.management.v2.Nullable.spec": {
							Type:        "io.cattle.management.v2.Nullable.spec",
							Description: "",
							ResourceFields: map[string]definitionField{
								"rkeConfig": {
									Type: "io.cattle.management.v2.Nullable.spec.rkeConfig",
								},
							},
						},
						"io.cattle.management.v2.Nullable.spec.rkeConfig": {
							Type:        "io.cattle.management.v2.Nullable.spec.rkeConfig",
							Description: "",
							ResourceFields: map[string]definitionField{
								"additionalManifest": {
									Type: "string",
								},
							},
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
		{
			// UserAttribute is a CRD, but is not a Kind because the CRD misses some
			// fields for the object.
			// We still want it to be defined correctly and have default values applied (apiVersion, kind, metadata)
			name:       "user attribute",
			schemaName: "management.cattle.io.userattribute",
			wantObject: &types.APIObject{
				ID:   "management.cattle.io.userattribute",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "io.cattle.management.v2.UserAttribute",
					Definitions: map[string]definition{
						"io.cattle.management.v2.UserAttribute": {
							Type:        "io.cattle.management.v2.UserAttribute",
							Description: "",
							ResourceFields: map[string]definitionField{
								"apiVersion": {
									Type:        "string",
									Description: "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
								},
								"kind": {
									Type:        "string",
									Description: "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
								},
								"metadata": {
									Type:        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
									Description: "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata",
								},
							},
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
		{
			name:       "global role definition",
			schemaName: "management.cattle.io.globalrole",
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
									Type: "io.cattle.management.v2.GlobalRole.spec", Description: "The spec for the project",
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
		{
			// The preferred group for management.cattle.io is V2, but DeprecatedResource doesn't
			// exist in V2. Steve should be able to fallback to another version (V1).
			name:       "deprecated resource",
			schemaName: "management.cattle.io.deprecatedresource",
			wantObject: &types.APIObject{
				ID:   "management.cattle.io.deprecatedresource",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "io.cattle.management.v1.DeprecatedResource",
					Definitions: map[string]definition{
						"io.cattle.management.v1.DeprecatedResource": {
							Type:        "io.cattle.management.v1.DeprecatedResource",
							Description: "A resource that is not present in v2",
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
							},
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
		{
			name:       "baseSchema",
			schemaName: "builtin",
			wantObject: &types.APIObject{
				ID:   "builtin",
				Type: "schemaDefinition",
				Object: schemaDefinition{
					DefinitionType: "builtin",
					Definitions: map[string]definition{
						"builtin": {
							ResourceFields: map[string]definitionField{
								"complex": {
									Type:        "map",
									SubType:     "string",
									Description: "some complex field",
								},
								"complexArray": {
									Type:        "array",
									SubType:     "string",
									Description: "some complex array field",
								},
								"complexRef": {
									Type:        "reference",
									SubType:     "complex",
									Description: "some complex reference field",
								},
								"simple": {
									Type:        "string",
									Description: "some simple field",
									Required:    true,
								},
								"leftBracket": {
									Type:        "test[",
									Description: "some field with a open bracket but no close bracket",
								},
							},
							Type:        "builtin",
							Description: "some builtin type",
						},
					},
				},
			},
		},
		{
			name:          "not a kind",
			schemaName:    "management.cattle.io.notakind",
			wantError:     true,
			wantErrorCode: intPtr(503),
		},
		{
			name:          "missing definition",
			schemaName:    "management.cattle.io.cluster",
			wantError:     true,
			wantErrorCode: intPtr(404),
		},
		{
			name:          "not refreshed",
			schemaName:    "management.cattle.io.globalrole",
			skipRefresh:   true,
			wantError:     true,
			wantErrorCode: intPtr(503),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			crdCache := fake.NewMockNonNamespacedCacheInterface[*apiextv1.CustomResourceDefinition](ctrl)
			crds, err := getCRDs()
			require.NoError(t, err)

			crdCache.EXPECT().List(labels.Everything()).Return(crds, nil).AnyTimes()

			handler := NewSchemaDefinitionHandler(baseSchemas, crdCache, discoveryClient)
			if !test.skipRefresh {
				err = handler.Refresh()
				require.NoError(t, err)
			}
			request := types.APIRequest{
				Schemas: schemas,
				Name:    test.schemaName,
			}
			response, err := handler.byIDHandler(&request)
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
	groups := []metav1.APIGroup{
		// The core groups (eg: Pods, ConfigMaps, etc)
		{
			Name: "",
			PreferredVersion: metav1.GroupVersionForDiscovery{
				GroupVersion: "v1",
				Version:      "v1",
			},
			Versions: []metav1.GroupVersionForDiscovery{
				{
					GroupVersion: "v1",
					Version:      "v1",
				},
			},
		},
		{
			Name: "management.cattle.io",
			PreferredVersion: metav1.GroupVersionForDiscovery{
				GroupVersion: "management.cattle.io/v2",
				Version:      "v2",
			},
			Versions: []metav1.GroupVersionForDiscovery{
				{
					GroupVersion: "management.cattle.io/v1",
					Version:      "v1",
				},
				{
					GroupVersion: "management.cattle.io/v2",
					Version:      "v2",
				},
			},
		},
		{
			Name: "noversion.cattle.io",
			Versions: []metav1.GroupVersionForDiscovery{
				{
					GroupVersion: "noversion.cattle.io/v1",
					Version:      "v1",
				},
				{
					GroupVersion: "noversion.cattle.io/v2",
					Version:      "v2",
				},
			},
		},
	}
	return &fakeDiscovery{
		Groups: &metav1.APIGroupList{
			Groups: groups,
		},
		Document: document,
	}, nil
}

type fakeDiscovery struct {
	Groups      *metav1.APIGroupList
	Document    *openapi_v2.Document
	GroupsErr   error
	DocumentErr error
}

// ServerGroups is the only method that needs to be mocked
func (f *fakeDiscovery) ServerGroups() (*metav1.APIGroupList, error) {
	return f.Groups, f.GroupsErr
}

// The rest of these methods are just here to conform to discovery.DiscoveryInterface
func (f *fakeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return nil, nil, nil
}

func (f *fakeDiscovery) RESTClient() restclient.Interface { return nil }
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

func getJSONSchema(crds []*apiextv1.CustomResourceDefinition, name, version string) *apiextv1.JSONSchemaProps {
	for _, crd := range crds {
		if crd.GetName() != name {
			continue
		}

		for _, ver := range crd.Spec.Versions {
			if ver.Name != version {
				continue
			}

			return ver.Schema.OpenAPIV3Schema
		}
	}
	return nil
}
