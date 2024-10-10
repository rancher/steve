package converter

import (
	"fmt"
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema/table"
	"github.com/rancher/wrangler/v3/pkg/generic/fake"
	wranglerSchema "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestAddCustomResources(t *testing.T) {
	tests := []struct {
		name             string
		crds             []v1.CustomResourceDefinition
		preFilledSchemas []string // crds to pre-create schemas for
		crdError         error
		wantError        bool
		desiredSchema    map[string]*types.APISchema
	}{
		{
			name: "one crd - all different field types",
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
						Group: "testGroup",
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
											"stringField": {
												Description: "StringField - Not Required Property",
												Type:        "string",
											},
											"nullArrayField": {
												Description: "ArrayField with no type - Not Required Property",
												Type:        "array",
											},
											"objectArrayField": {
												Description: "ArrayField with an object type - Not Required Property",
												Type:        "array",
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Type: "object",
													},
												},
											},
											"objectArrayJSONField": {
												Description: "ArrayField with an object type defined in JSONSchemas - Not Required Property",
												Type:        "array",
												Items: &v1.JSONSchemaPropsOrArray{
													JSONSchemas: []v1.JSONSchemaProps{
														{
															Type: "object",
														},
													},
												},
											},
											"stringArrayField": {
												Description: "ArrayField with a string type - Not Required Property",
												Type:        "array",
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Type: "string",
													},
												},
											},
											"stringArrayJSONField": {
												Description: "ArrayField with a string type defined in JSONSchemas - Not Required Property",
												Type:        "array",
												Items: &v1.JSONSchemaPropsOrArray{
													JSONSchemas: []v1.JSONSchemaProps{
														{
															Type: "string",
														},
													},
												},
											},
											"stringArrayBothField": {
												Description: "ArrayField with a string type defined in both Schema and JSONSchemas - Not Required Property",
												Type:        "array",
												Items: &v1.JSONSchemaPropsOrArray{
													Schema: &v1.JSONSchemaProps{
														Type: "string",
													},
													JSONSchemas: []v1.JSONSchemaProps{
														{
															Type: "object",
														},
													},
												},
											},
											"nullObjectField": {
												Description: "ObjectField with no type - Not Required Property",
												Type:        "object",
											},
											"additionalPropertiesObjectField": {
												Description: "ObjectField with a type in additionalProperties - Not Required Property",
												Type:        "object",
												AdditionalProperties: &v1.JSONSchemaPropsOrBool{
													Schema: &v1.JSONSchemaProps{
														Type: "string",
													},
												},
											},
											"nestedObjectField": {
												Description: "ObjectField with an object type in additionalProperties - Not Required Property",
												Type:        "object",
												AdditionalProperties: &v1.JSONSchemaPropsOrBool{
													Schema: &v1.JSONSchemaProps{
														Type: "object",
													},
												},
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
			preFilledSchemas: []string{"testgroup.v1.testresource"},
			wantError:        false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID: "testgroup.v1.testresource",
						Attributes: map[string]interface{}{
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
			},
		},
		{
			name:          "crd list error - early break, no error",
			crds:          []v1.CustomResourceDefinition{},
			crdError:      fmt.Errorf("unable to list crds"),
			wantError:     false,
			desiredSchema: map[string]*types.APISchema{},
		},
		{
			name: "skip resource - no plural name",
			crds: []v1.CustomResourceDefinition{
				{
					Status: v1.CustomResourceDefinitionStatus{
						AcceptedNames: v1.CustomResourceDefinitionNames{
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
					Spec: v1.CustomResourceDefinitionSpec{
						Group: "testGroup",
						Versions: []v1.CustomResourceDefinitionVersion{
							{
								Schema: &v1.CustomResourceValidation{
									OpenAPIV3Schema: &v1.JSONSchemaProps{
										Description: "Test Resource for unit tests",
										Required:    []string{"required"},
										Properties: map[string]v1.JSONSchemaProps{
											"required": {
												Description: "Required Property",
												Type:        "string",
											},
										},
									},
								},
							},
						},
						Names: v1.CustomResourceDefinitionNames{
							Singular: "testResource",
							Kind:     "TestResource",
						},
					},
				},
			},
			preFilledSchemas: []string{"testgroup.v1.testresource"},
			wantError:        false,
			desiredSchema: map[string]*types.APISchema{
				"testgroup.v1.testresource": {
					Schema: &wranglerSchema.Schema{
						ID: "testgroup.v1.testresource",
					},
				},
			},
		},
		{
			name: "skip resource - no pre-defined schema",
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
						Group: "testGroup",
						Versions: []v1.CustomResourceDefinitionVersion{
							{
								Schema: &v1.CustomResourceValidation{
									OpenAPIV3Schema: &v1.JSONSchemaProps{
										Description: "Test Resource for unit tests",
										Required:    []string{"required"},
										Properties: map[string]v1.JSONSchemaProps{
											"required": {
												Description: "Required Property",
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
			wantError:     false,
			desiredSchema: map[string]*types.APISchema{},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			fakeClient := fake.NewMockNonNamespacedClientInterface[*v1.CustomResourceDefinition, *v1.CustomResourceDefinitionList](ctrl)
			var crds *v1.CustomResourceDefinitionList
			if test.crds != nil {
				crds = &v1.CustomResourceDefinitionList{
					Items: test.crds,
				}
			}
			fakeClient.EXPECT().List(gomock.Any()).Return(crds, test.crdError)
			schemas := map[string]*types.APISchema{}
			for i := range test.preFilledSchemas {
				schemas[test.preFilledSchemas[i]] = &types.APISchema{
					Schema: &wranglerSchema.Schema{
						ID: test.preFilledSchemas[i],
					},
				}
			}
			err := addCustomResources(fakeClient, schemas)
			if test.wantError {
				assert.Error(t, err, "expected an error but there was no error")
			} else {
				assert.NoError(t, err, "got an unexpected error")
			}
			assert.Equal(t, test.desiredSchema, schemas)
		})
	}
}
