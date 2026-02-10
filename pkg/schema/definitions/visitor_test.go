package definitions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/kube-openapi/pkg/util/proto"
)

var (
	protoPrimitive = proto.Primitive{
		BaseSchema: proto.BaseSchema{
			Description: "primitive value",
		},
		Type: "string",
	}
	protoPrimitiveInt = proto.Primitive{
		BaseSchema: proto.BaseSchema{
			Description: "primitive value - int",
		},
		Type: "integer",
	}
	protoPrimitiveNumber = proto.Primitive{
		BaseSchema: proto.BaseSchema{
			Description: "primitive value - number",
		},
		Type: "number",
	}
	protoArray = proto.Array{
		BaseSchema: proto.BaseSchema{
			Description: "testArray",
		},
		SubType: &protoPrimitive,
	}
	protoMap = proto.Map{
		BaseSchema: proto.BaseSchema{
			Description: "testMap",
		},
		SubType: &protoPrimitive,
	}
	protoKind = proto.Kind{
		BaseSchema: proto.BaseSchema{
			Description: "testKind",
			Path:        proto.NewPath("io.cattle.test"),
		},
		Fields: map[string]proto.Schema{
			"protoArray":     &protoArray,
			"protoPrimitive": &protoPrimitive,
			"protoMap":       &protoMap,
		},
		RequiredFields: []string{
			"protoArray",
			"protoPrimitive",
			"missing",
		},
	}
	protoRefNoSubSchema = openAPIV2Reference{
		BaseSchema: proto.BaseSchema{
			Description: "testRef - no subSchema",
		},
		reference: "some-other-type",
	}
	protoRef = openAPIV2Reference{
		BaseSchema: proto.BaseSchema{
			Description: "testRef",
		},
		reference: "testKind",
		subSchema: &protoKind,
	}
	protoArbitrary = proto.Arbitrary{
		BaseSchema: proto.BaseSchema{
			Description: "testArbitrary",
		},
	}
	protoNestedMap = proto.Map{
		BaseSchema: proto.BaseSchema{
			Description: "nestedMap",
		},
		SubType: &protoKind,
	}
	protoEmpty = proto.Kind{
		BaseSchema: proto.BaseSchema{
			Description: "emptySchema",
			Path:        proto.NewPath("io.cattle.empty"),
		},
	}
)

func TestSchemaFieldVisitor(t *testing.T) {
	protoKind.Fields["protoRef"] = &protoRef
	tests := []struct {
		name            string
		inputSchema     proto.Schema
		wantDefinitions map[string]definition
		wantField       definitionField
	}{
		{
			name:            "array",
			inputSchema:     &protoArray,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "array[string]",
				Description: protoArray.Description,
			},
		},
		{
			name:            "map",
			inputSchema:     &protoMap,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "map[string]",
				Description: protoMap.Description,
			},
		},
		{
			name:            "string primitive",
			inputSchema:     &protoPrimitive,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        protoPrimitive.Type,
				Description: protoPrimitive.Description,
			},
		},
		{
			name:            "integer primitive",
			inputSchema:     &protoPrimitiveInt,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "int",
				Description: protoPrimitiveInt.Description,
			},
		},
		{
			name:            "number primitive",
			inputSchema:     &protoPrimitiveNumber,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "int",
				Description: protoPrimitiveNumber.Description,
			},
		},
		{
			name:        "kind",
			inputSchema: &protoKind,
			wantDefinitions: map[string]definition{
				protoKind.Path.String(): {
					ResourceFields: map[string]definitionField{
						"protoArray": {
							Type:        "array[" + protoPrimitive.Type + "]",
							Description: protoArray.Description,
							Required:    true,
						},
						"protoMap": {
							Type:        "map[" + protoPrimitive.Type + "]",
							Description: protoMap.Description,
						},
						"protoPrimitive": {
							Type:        protoPrimitive.Type,
							Description: protoPrimitive.Description,
							Required:    true,
						},
						"protoRef": {
							Type:        protoKind.Path.String(),
							Description: protoRef.Description,
						},
					},
					Type:        protoKind.Path.String(),
					Description: protoKind.Description,
				},
			},
			wantField: definitionField{
				Description: protoKind.Description,
				Type:        protoKind.Path.String(),
			},
		},
		{
			name:            "reference no subschema",
			inputSchema:     &protoRefNoSubSchema,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        protoRefNoSubSchema.reference,
				Description: protoRefNoSubSchema.Description,
			},
		},
		{
			name:        "reference",
			inputSchema: &protoRef,
			wantDefinitions: map[string]definition{
				protoKind.Path.String(): {
					ResourceFields: map[string]definitionField{
						"protoArray": {
							Type:        "array[string]",
							Description: protoArray.Description,
							Required:    true,
						},
						"protoMap": {
							Type:        "map[string]",
							Description: protoMap.Description,
						},
						"protoPrimitive": {
							Type:        protoPrimitive.Type,
							Description: protoPrimitive.Description,
							Required:    true,
						},
						"protoRef": {
							Type:        protoKind.Path.String(),
							Description: protoRef.Description,
						},
					},
					Type:        protoKind.Path.String(),
					Description: protoKind.Description,
				},
			},
			wantField: definitionField{
				Type:        protoKind.Path.String(),
				Description: protoRef.Description,
			},
		},
		{
			name:            "abitrary schema",
			inputSchema:     &protoArbitrary,
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "string",
				Description: protoArbitrary.Description,
			},
		},
		{
			name:        "nested map with kind",
			inputSchema: &protoNestedMap,
			wantDefinitions: map[string]definition{
				protoKind.Path.String(): {
					ResourceFields: map[string]definitionField{
						"protoArray": {
							Type:        "array[string]",
							Description: protoArray.Description,
							Required:    true,
						},
						"protoMap": {
							Type:        "map[string]",
							Description: protoMap.Description,
						},
						"protoPrimitive": {
							Type:        protoPrimitive.Type,
							Description: protoPrimitive.Description,
							Required:    true,
						},
						"protoRef": {
							Type:        protoKind.Path.String(),
							Description: protoRef.Description,
						},
					},
					Type:        protoKind.Path.String(),
					Description: protoKind.Description,
				},
			},
			wantField: definitionField{
				Type:        "map[io.cattle.test]",
				Description: protoNestedMap.Description,
			},
		},
		{
			name: "multi-level nested maps and arrays",
			inputSchema: &proto.Map{
				BaseSchema: proto.BaseSchema{
					Description: "multi-level nested structure",
				},
				SubType: &proto.Array{
					BaseSchema: proto.BaseSchema{
						Description: "nested array",
					},
					SubType: &proto.Map{
						BaseSchema: proto.BaseSchema{
							Description: "deeply nested map",
						},
						SubType: &protoPrimitive,
					},
				},
			},
			wantDefinitions: map[string]definition{},
			wantField: definitionField{
				Type:        "map[array[map[string]]]",
				Description: "multi-level nested structure",
			},
		},
		{
			name:        "empty schema",
			inputSchema: &protoEmpty,
			wantDefinitions: map[string]definition{
				"io.cattle.empty": {
					ResourceFields: map[string]definitionField{},
					Type:           "io.cattle.empty",
					Description:    protoEmpty.Description,
				},
			},
			wantField: definitionField{
				Type:        "io.cattle.empty",
				Description: protoEmpty.Description,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definitions := map[string]definition{}
			visitor := schemaFieldVisitor{
				definitions: definitions,
			}
			test.inputSchema.Accept(&visitor)
			require.Equal(t, test.wantField, visitor.field)
			require.Equal(t, test.wantDefinitions, visitor.definitions)
		})
	}
}
