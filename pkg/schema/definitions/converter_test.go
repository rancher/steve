package definitions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestCRDToDefinition(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		// rawSchema is a JSON encoded OpenAPI V3 spec
		// We use JSON instead of Go types because it's closer to what
		// user are familiar with (JSON Schema in Go has some more fields
		// like JSONSchemaPropsOrArray
		rawSchema         []byte
		expectedSchemaDef schemaDefinition
		wantError         bool
	}{
		{
			name:      "primitives",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "aStringWithoutDescription": {
      "type": "string"
    },
    "aString": {
      "type": "string",
      "description": "description of aString"
    },
    "anInteger": {
      "type": "integer",
      "description": "description of anInteger"
    },
    "aNumber": {
      "type": "number",
      "description": "description of aNumber"
    },
    "aBoolean": {
      "type": "boolean",
      "description": "description of aBoolean"
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"aStringWithoutDescription": {
								Type: "string",
							},
							"aString": {
								Type:        "string",
								Description: "description of aString",
							},
							"anInteger": {
								Type:        "int",
								Description: "description of anInteger",
							},
							"aNumber": {
								Type:        "int",
								Description: "description of aNumber",
							},
							"aBoolean": {
								Type:        "boolean",
								Description: "description of aBoolean",
							},
						},
					},
				},
			},
		},
		{
			name:      "arrays",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "anArrayOfString": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "anArrayOfInteger": {
      "type": "array",
      "items": {
        "type": "integer"
      }
    },
    "anArrayOfNumber": {
      "type": "array",
      "items": {
        "type": "number"
      }
    },
    "anArrayOfBoolean": {
      "type": "array",
      "items": {
        "type": "boolean"
      }
    },
    "anArrayOfObject": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          }
        }
      }
    },
    "anArrayOfMap": {
      "type": "array",
      "items": {
        "type": "object"
      }
    },
    "anArrayWithListOfItems": {
      "type": "array",
      "items": [
	{
          "type": "string"
        },
	{
          "type": "number"
        }
      ]
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"anArrayOfString": {
								Type:    "array",
								SubType: "string",
							},
							"anArrayOfInteger": {
								Type:    "array",
								SubType: "int",
							},
							"anArrayOfNumber": {
								Type:    "array",
								SubType: "int",
							},
							"anArrayOfBoolean": {
								Type:    "array",
								SubType: "boolean",
							},
							"anArrayOfObject": {
								Type:    "array",
								SubType: "my.group.v1.Test.anArrayOfObject",
							},
							"anArrayOfMap": {
								Type:    "array",
								SubType: "map",
							},
							"anArrayWithListOfItems": {
								Type:    "array",
								SubType: "string",
							},
						},
					},
					"my.group.v1.Test.anArrayOfObject": {
						Type: "my.group.v1.Test.anArrayOfObject",
						ResourceFields: map[string]definitionField{
							"foo": {
								Type: "string",
							},
						},
					},
					// Currently not referenced in my.group.v1.Test due to lack of support for nested array
					// but will be useful once we get this in
					"my.group.v1.Test.anArrayOfMap": {
						Type:           "my.group.v1.Test.anArrayOfMap",
						ResourceFields: map[string]definitionField{},
					},
				},
			},
		},
		{
			name:      "nested objects",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "grandparent": {
      "type": "object",
      "properties": {
        "parent": {
          "type": "object",
          "properties": {
            "child": {
              "type": "string"
            }
          }
        }
      }
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"grandparent": {
								Type: "my.group.v1.Test.grandparent",
							},
						},
					},
					"my.group.v1.Test.grandparent": {
						Type: "my.group.v1.Test.grandparent",
						ResourceFields: map[string]definitionField{
							"parent": {
								Type: "my.group.v1.Test.grandparent.parent",
							},
						},
					},
					"my.group.v1.Test.grandparent.parent": {
						Type: "my.group.v1.Test.grandparent.parent",
						ResourceFields: map[string]definitionField{
							"child": {
								Type: "string",
							},
						},
					},
				},
			},
		},
		{
			name:      "nested arrays",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "anArrayOfArrayOfString": {
      "type": "array",
      "items": {
        "type": "array",
        "items": {
          "type": "string"
        }
      }
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"anArrayOfArrayOfString": {
								Type:    "array",
								SubType: "array",
							},
						},
					},
				},
			},
		},
		{
			name:      "maps in object",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "mapEmpty": {
      "type": "object"
    },
    "mapAdditionalPropertiesObject": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "properties": {
          "foo": {
            "type": "string"
          }
        }
      }
    },
    "mapAdditionalPropertiesPrimitive": {
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"mapEmpty": {
								Type:    "map",
								SubType: "my.group.v1.Test.mapEmpty",
							},
							"mapAdditionalPropertiesObject": {
								Type:    "map",
								SubType: "my.group.v1.Test.mapAdditionalPropertiesObject",
							},
							"mapAdditionalPropertiesPrimitive": {
								Type:    "map",
								SubType: "string",
							},
						},
					},
					"my.group.v1.Test.mapEmpty": {
						Type:           "my.group.v1.Test.mapEmpty",
						ResourceFields: map[string]definitionField{},
					},
					"my.group.v1.Test.mapAdditionalPropertiesObject": {
						Type: "my.group.v1.Test.mapAdditionalPropertiesObject",
						ResourceFields: map[string]definitionField{
							"foo": {
								Type: "string",
							},
						},
					},
				},
			},
		},
		{
			name:      "required fields",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "required": [
    "topLevelRequired"
  ],
  "properties": {
    "topLevelRequired": {
      "type": "string"
    },
    "child": {
      "type": "object",
      "required": [
        "fieldIsRequired"
      ],
      "properties": {
        "fieldIsRequired": {
          "type": "string"
        }
      }
    }
  }
}`),
			expectedSchemaDef: schemaDefinition{
				DefinitionType: "my.group.v1.Test",
				Definitions: map[string]definition{
					"my.group.v1.Test": {
						Type: "my.group.v1.Test",
						ResourceFields: map[string]definitionField{
							"topLevelRequired": {
								Type:     "string",
								Required: true,
							},
							"child": {
								Type: "my.group.v1.Test.child",
							},
						},
					},
					"my.group.v1.Test.child": {
						Type: "my.group.v1.Test.child",
						ResourceFields: map[string]definitionField{
							"fieldIsRequired": {
								Type:     "string",
								Required: true,
							},
						},
					},
				},
			},
		},
		{
			name:      "bad array",
			modelName: "my.group.v1.Test",
			rawSchema: []byte(`
{
  "type": "object",
  "properties": {
    "badArray": {
      "type": "array"
    }
  }
}`),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var schema apiextv1.JSONSchemaProps
			err := json.Unmarshal(test.rawSchema, &schema)
			require.NoError(t, err)

			schemaDef, err := crdToDefinition(&schema, test.modelName)

			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedSchemaDef, schemaDef)
			}
		})
	}

}
