package definitions

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

func TestRegister(t *testing.T) {
	schemas := types.EmptyAPISchemas()
	client := fakeDiscovery{}
	ctrl := gomock.NewController(t)
	crdController := fake.NewMockNonNamespacedControllerInterface[*apiextv1.CustomResourceDefinition, *apiextv1.CustomResourceDefinitionList](ctrl)
	apisvcController := fake.NewMockNonNamespacedControllerInterface[*apiregv1.APIService, *apiregv1.APIServiceList](ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	crdController.EXPECT().OnChange(ctx, handlerKey, gomock.Any())
	crdController.EXPECT().Cache().AnyTimes()
	apisvcController.EXPECT().OnChange(ctx, handlerKey, gomock.Any())
	Register(ctx, schemas, &client, crdController, apisvcController)
	registeredSchema := schemas.LookupSchema("schemaDefinition")
	require.NotNil(t, registeredSchema)
	require.Len(t, registeredSchema.ResourceMethods, 1)
	require.Equal(t, registeredSchema.ResourceMethods[0], "GET")
	require.NotNil(t, registeredSchema.ByIDHandler)
	// Register will spawn a background thread, so we want to stop that to not impact other tests
	cancel()
}

func Test_getDurationEnvVarOrDefault(t *testing.T) {
	os.Setenv("VALID", "1")
	os.Setenv("INVALID", "NOTANUMBER")
	tests := []struct {
		name         string
		envVar       string
		defaultValue int
		unit         time.Duration
		wantDuration time.Duration
	}{
		{
			name:         "not found, use default",
			envVar:       "NOT_FOUND",
			defaultValue: 12,
			unit:         time.Second,
			wantDuration: time.Second * 12,
		},
		{
			name:         "found but not an int",
			envVar:       "INVALID",
			defaultValue: 24,
			unit:         time.Minute,
			wantDuration: time.Minute * 24,
		},
		{
			name:         "found and valid int",
			envVar:       "VALID",
			defaultValue: 30,
			unit:         time.Hour,
			wantDuration: time.Hour * 1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := getDurationEnvVarOrDefault(test.envVar, test.defaultValue, test.unit)
			require.Equal(t, test.wantDuration, got)
		})
	}
}

func TestSchemaDefinitionMerge(t *testing.T) {
	tests := []struct {
		name     string
		schemas  [2]schemaDefinition
		wantErr  bool
		expected schemaDefinition
	}{
		{
			name: "merge top-level definitions",
			schemas: [2]schemaDefinition{
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
						},
					},
				},
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"bar": {
							Type:           "bar",
							Description:    "Bar",
							ResourceFields: map[string]definitionField{},
						},
					},
				},
			},
			expected: schemaDefinition{
				DefinitionType: "foo",
				Definitions: map[string]definition{
					"foo": {
						Type:        "foo",
						Description: "Foo",
					},
					"bar": {
						Type:           "bar",
						Description:    "Bar",
						ResourceFields: map[string]definitionField{},
					},
				},
			},
		},
		{
			name: "merge resource fields",
			schemas: [2]schemaDefinition{
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
							ResourceFields: map[string]definitionField{
								"old": {
									Type:        "string",
									Description: "foo.old",
								},
								"inBoth": {
									Type:        "string",
									Description: "foo.inBoth",
								},
							},
						},
					},
				},
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
							ResourceFields: map[string]definitionField{
								"new": {
									Type:        "string",
									Description: "foo.new",
								},
								"inBoth": {
									Type:        "array",
									SubType:     "number",
									Description: "foo.inBoth",
									Required:    true,
								},
							},
						},
					},
				},
			},
			expected: schemaDefinition{
				DefinitionType: "foo",
				Definitions: map[string]definition{
					"foo": {
						Type:        "foo",
						Description: "Foo",
						ResourceFields: map[string]definitionField{
							"new": {
								Type:        "string",
								Description: "foo.new",
							},
							"old": {
								Type:        "string",
								Description: "foo.old",
							},
							"inBoth": {
								Type:        "array",
								SubType:     "number",
								Description: "foo.inBoth",
								Required:    true,
							},
						},
					},
				},
			},
		},
		{
			name: "empty resource fields in old",
			schemas: [2]schemaDefinition{
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
						},
					},
				},
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
							ResourceFields: map[string]definitionField{
								"new": {
									Type:        "string",
									Description: "foo.new",
								},
							},
						},
					},
				},
			},
			expected: schemaDefinition{
				DefinitionType: "foo",
				Definitions: map[string]definition{
					"foo": {
						Type:        "foo",
						Description: "Foo",
						ResourceFields: map[string]definitionField{
							"new": {
								Type:        "string",
								Description: "foo.new",
							},
						},
					},
				},
			},
		},
		{
			name: "empty resource fields in new",
			schemas: [2]schemaDefinition{
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "string",
							Description: "Foo",
							ResourceFields: map[string]definitionField{
								"old": {
									Type:        "string",
									Description: "foo.old",
								},
							},
						},
					},
				},
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "string",
							Description: "Foo",
						},
					},
				},
			},
			expected: schemaDefinition{
				DefinitionType: "foo",
				Definitions: map[string]definition{
					"foo": {
						Type:        "string",
						Description: "Foo",
						ResourceFields: map[string]definitionField{
							"old": {
								Type:        "string",
								Description: "foo.old",
							},
						},
					},
				},
			},
		},
		{
			name: "empty definition type",
			schemas: [2]schemaDefinition{
				{
					DefinitionType: "foo",
					Definitions: map[string]definition{
						"foo": {
							Type:        "foo",
							Description: "Foo",
							ResourceFields: map[string]definitionField{
								"old": {
									Type:        "string",
									Description: "foo.old",
								},
							},
						},
					},
				},
				{
					DefinitionType: "",
					Definitions:    map[string]definition{},
				},
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.schemas[0].Merge(test.schemas[1])
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, test.schemas[0])
			}

		})
	}
}
