package converter

import (
	"fmt"
	"testing"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/resources/apigroups"
	"github.com/rancher/steve/pkg/schema/table"
	"github.com/rancher/wrangler/v3/pkg/generic/fake"
	wranglerSchema "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/util/proto"
)

func TestToSchemas(t *testing.T) {
	createNamedSchema := func(name string, description string, gvk schema.GroupVersionKind) (*openapiv2.NamedSchema, error) {
		gvkExtensionMap := map[any]any{
			gvkExtensionGroup:   gvk.Group,
			gvkExtensionVersion: gvk.Version,
			gvkExtensionKind:    gvk.Kind,
		}
		gvkExtensionSlice := []any{gvkExtensionMap}
		extensionSliceYaml, err := yaml.Marshal(gvkExtensionSlice)
		if err != nil {
			return nil, fmt.Errorf("unable to create named schema for %s: %w", name, err)
		}
		return &openapiv2.NamedSchema{
			Name: name,
			Value: &openapiv2.Schema{
				Description: description,
				Type: &openapiv2.TypeItem{
					Value: []string{"object"},
				},
				Properties: &openapiv2.Properties{
					AdditionalProperties: []*openapiv2.NamedSchema{},
				},
				VendorExtension: []*openapiv2.NamedAny{
					{
						Name: gvkExtensionName,
						Value: &openapiv2.Any{
							Yaml: string(extensionSliceYaml),
						},
					},
				},
			},
		}, nil
	}
	apiGroupDescription := "APIGroup contains the name, the supported versions, and the preferred version of a group"
	gvkSchema, err := createNamedSchema("TestResources", "TestResources are test resource created for unit tests", schema.GroupVersionKind{
		Group:   "TestGroup",
		Version: "v1",
		Kind:    "TestResource",
	})
	require.NoError(t, err)
	apiGroupSchema, err := createNamedSchema("ApiGroups", apiGroupDescription, schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "APIGroup",
	})
	require.NoError(t, err)
	tests := []struct {
		name          string
		groups        []schema.GroupVersion
		resources     map[schema.GroupVersion][]metav1.APIResource
		crds          []v1.CustomResourceDefinition
		document      *openapiv2.Document
		discoveryErr  error
		documentErr   error
		crdErr        error
		wantError     bool
		desiredSchema map[string]*types.APISchema
	}{
		{
			name:   "crd listed in discovery, defined in crds",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			crds: []v1.CustomResourceDefinition{
				{
					Status: v1.CustomResourceDefinitionStatus{
						AcceptedNames: v1.CustomResourceDefinitionNames{
							Plural:   "testResources",
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
					Spec: v1.CustomResourceDefinitionSpec{
						Group: "TestGroup",
						Versions: []v1.CustomResourceDefinitionVersion{
							{
								Name: "v1",
								AdditionalPrinterColumns: []v1.CustomResourceColumnDefinition{
									{
										Name:     "TestColumn",
										JSONPath: "TestPath",
										Type:     "TestType",
										Format:   "TestFormat",
									},
								},
								Schema: &v1.CustomResourceValidation{
									OpenAPIV3Schema: &v1.JSONSchemaProps{
										Description: "Test Resource for unit tests",
										Required:    []string{"required"},
										Properties: map[string]v1.JSONSchemaProps{
											"required": {
												Description: "Required Property",
												Type:        "string",
											},
											"numberField": {
												Description: "NumberField - Not Required Property",
												Type:        "number",
											},
											"nullArrayField": {
												Description: "ArrayField with no type - Not Required Property",
												Type:        "array",
											},
											"nullObjectField": {
												Description: "ObjectField with no type - Not Required Property",
												Type:        "object",
											},
											"actions": {
												Description: "Reserved field - Not Required Property",
												Type:        "string",
											},
										},
									},
								},
							},
						},
						Names: v1.CustomResourceDefinitionNames{
							Plural:   "testResources",
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
							"columns": []table.Column{
								{
									Name:   "TestColumn",
									Field:  "TestPath",
									Type:   "TestType",
									Format: "TestFormat",
								},
							},
						},
						Description: "Test Resource for unit tests",
					},
				},
				"core.v1.apigroup": &apigroups.BaseSchema,
			},
		},
		{
			name:   "listed in discovery, not defined in crds",
			crds:   []v1.CustomResourceDefinition{},
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
				"core.v1.apigroup": &apigroups.BaseSchema,
			},
		},
		{
			name:      "defined in crds, but not in discovery",
			groups:    []schema.GroupVersion{},
			resources: map[schema.GroupVersion][]metav1.APIResource{},
			crds: []v1.CustomResourceDefinition{
				{
					Status: v1.CustomResourceDefinitionStatus{
						AcceptedNames: v1.CustomResourceDefinitionNames{
							Plural:   "testResources",
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
					Spec: v1.CustomResourceDefinitionSpec{
						Group: "TestGroup",
						Versions: []v1.CustomResourceDefinitionVersion{
							{
								Name: "v1",
								AdditionalPrinterColumns: []v1.CustomResourceColumnDefinition{
									{
										Name:     "TestColumn",
										JSONPath: "TestPath",
										Type:     "TestType",
										Format:   "TestFormat",
									},
								},
								Schema: &v1.CustomResourceValidation{
									OpenAPIV3Schema: &v1.JSONSchemaProps{
										Description: "Test Resource for unit tests",
										Required:    []string{"required"},
										Properties: map[string]v1.JSONSchemaProps{
											"required": {
												Description: "Required Property",
												Type:        "string",
											},
											"numberField": {
												Description: "NumberField - Not Required Property",
												Type:        "number",
											},
											"nullArrayField": {
												Description: "ArrayField with no type - Not Required Property",
												Type:        "array",
											},
											"nullObjectField": {
												Description: "ObjectField with no type - Not Required Property",
												Type:        "object",
											},
											"actions": {
												Description: "Reserved field - Not Required Property",
												Type:        "string",
											},
										},
									},
								},
							},
						},
						Names: v1.CustomResourceDefinitionNames{
							Plural:   "testResources",
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"core.v1.apigroup": &apigroups.BaseSchema,
			},
		},
		{
			name:          "discovery error",
			groups:        []schema.GroupVersion{},
			resources:     map[schema.GroupVersion][]metav1.APIResource{},
			discoveryErr:  fmt.Errorf("server is down, can't use discovery"),
			crds:          []v1.CustomResourceDefinition{},
			wantError:     true,
			desiredSchema: nil,
		},
		{
			name:   "crd error",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			crdErr:    fmt.Errorf("unable to use crd client, insufficient permissions"),
			crds:      []v1.CustomResourceDefinition{},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:         "testgroup.v1.testresource",
						PluralName: "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
				"core.v1.apigroup": &apigroups.BaseSchema,
			},
		},
		{
			name:   "adding descriptions",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			crdErr: nil,
			crds:   []v1.CustomResourceDefinition{},
			document: &openapiv2.Document{
				Definitions: &openapiv2.Definitions{
					AdditionalProperties: []*openapiv2.NamedSchema{gvkSchema, apiGroupSchema},
				},
			},
			wantError: false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID:          "testgroup.v1.testresource",
						Description: gvkSchema.Value.Description,
						PluralName:  "TestGroup.v1.testResources",
						Attributes: map[string]interface{}{
							"group":      "TestGroup",
							"version":    "v1",
							"kind":       "TestResource",
							"resource":   "testResources",
							"verbs":      []string{"get"},
							"namespaced": true,
						},
					},
				},
				"core.v1.apigroup": {
					Schema: &wranglerSchema.Schema{
						ID: "apigroup",
						Attributes: map[string]interface{}{
							"group":   "",
							"kind":    "APIGroup",
							"version": "v1",
						},
						Description: apiGroupDescription,
					},
				},
			},
		},
		{
			name:   "descriptions error",
			groups: []schema.GroupVersion{{Group: "TestGroup", Version: "v1"}},
			resources: map[schema.GroupVersion][]metav1.APIResource{
				{Group: "TestGroup", Version: "v1"}: {
					{

						Name:         "testResources",
						SingularName: "testResource",
						Kind:         "TestResource",
						Namespaced:   true,
						Verbs:        metav1.Verbs{"get"},
					},
				},
			},
			crdErr:      nil,
			crds:        []v1.CustomResourceDefinition{},
			document:    nil,
			documentErr: fmt.Errorf("can't get document"),
			wantError:   true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			testDiscovery := fakeDiscovery{}
			for _, gvr := range test.groups {
				gvr := gvr
				testDiscovery.AddGroup(gvr.Group, gvr.Version, false)
			}
			testDiscovery.Document = test.document
			testDiscovery.DocumentErr = test.documentErr
			for gvr, resourceSlice := range test.resources {
				for _, resource := range resourceSlice {
					resource := resource
					testDiscovery.AddResource(gvr.Group, gvr.Version, resource)
				}
			}
			testDiscovery.GroupResourcesErr = test.discoveryErr
			var crds *v1.CustomResourceDefinitionList
			if test.crds != nil {
				crds = &v1.CustomResourceDefinitionList{
					Items: test.crds,
				}
			}
			fakeClient := fake.NewMockNonNamespacedClientInterface[*v1.CustomResourceDefinition, *v1.CustomResourceDefinitionList](ctrl)
			fakeClient.EXPECT().List(gomock.Any()).Return(crds, test.crdErr).AnyTimes()

			schemas, err := ToSchemas(fakeClient, &testDiscovery)
			if test.wantError {
				assert.Error(t, err, "wanted error but didn't get one")
			} else {
				assert.NoError(t, err, "got an error but did not want one")
			}
			assert.Equal(t, test.desiredSchema, schemas, "did not get the desired schemas")
		})
	}

}

func TestGVKToVersionedSchemaID(t *testing.T) {
	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		want string
	}{
		{
			name: "basic gvk",
			gvk: schema.GroupVersionKind{
				Group:   "TestGroup",
				Version: "v1",
				Kind:    "TestKind",
			},
			want: "testgroup.v1.testkind",
		},
		{
			name: "core resource",
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "TestKind",
			},
			want: "core.v1.testkind",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, GVKToVersionedSchemaID(test.gvk))
		})
	}

}

func TestGVKToSchemaID(t *testing.T) {
	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		want string
	}{
		{
			name: "basic gvk",
			gvk: schema.GroupVersionKind{
				Group:   "TestGroup",
				Version: "v1",
				Kind:    "TestKind",
			},
			want: "testgroup.testkind",
		},
		{
			name: "core resource",
			gvk: schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "TestKind",
			},
			want: "testkind",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, GVKToSchemaID(test.gvk))
		})
	}
}

func TestGVRToPluralName(t *testing.T) {
	tests := []struct {
		name string
		gvr  schema.GroupVersionResource
		want string
	}{
		{
			name: "basic gvk",
			gvr: schema.GroupVersionResource{
				Group:    "TestGroup",
				Version:  "v1",
				Resource: "TestResources",
			},
			want: "TestGroup.TestResources",
		},
		{
			name: "core resource",
			gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "TestResources",
			},
			want: "TestResources",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, GVRToPluralName(test.gvr))
		})
	}
}

func TestGetGVKForKind(t *testing.T) {
	tests := []struct {
		name    string
		kind    *proto.Kind
		wantGVK *schema.GroupVersionKind
	}{
		{
			name: "basic kind",
			kind: &proto.Kind{
				BaseSchema: proto.BaseSchema{
					Extensions: map[string]any{
						gvkExtensionName: []any{
							"some other extension",
							map[any]any{
								gvkExtensionGroup:   "TestGroup",
								gvkExtensionVersion: "v1",
								gvkExtensionKind:    "TestKind",
							},
						},
					},
				},
			},
			wantGVK: &schema.GroupVersionKind{
				Group:   "TestGroup",
				Version: "v1",
				Kind:    "TestKind",
			},
		},
		{
			name: "kind missing gvkExtension",
			kind: &proto.Kind{
				BaseSchema: proto.BaseSchema{
					Extensions: map[string]any{},
				},
			},
			wantGVK: nil,
		},
		{
			name: "kind missing gvk map",
			kind: &proto.Kind{
				BaseSchema: proto.BaseSchema{
					Extensions: map[string]any{
						gvkExtensionName: []any{"some value"},
					},
				},
			},
			wantGVK: nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.wantGVK, GetGVKForKind(test.kind))
		})
	}

}
