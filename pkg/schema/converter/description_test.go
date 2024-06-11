package converter

import (
	"fmt"
	"testing"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestAddDescription(t *testing.T) {
	gvkExtensionMap := map[any]any{
		gvkExtensionGroup:   "management.cattle.io",
		gvkExtensionVersion: "v3",
		gvkExtensionKind:    "GlobalRole",
	}
	gvkExtensionSlice := []any{gvkExtensionMap}
	extensionSliceYaml, err := yaml.Marshal(gvkExtensionSlice)
	require.NoError(t, err)
	gvkSchema := openapiv2.NamedSchema{
		Name: "GlobalRoles",
		Value: &openapiv2.Schema{
			Description: "GlobalRoles are Global permissions in Rancher",
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
	}
	noGVKSchema := openapiv2.NamedSchema{
		Name: "GlobalRoleSpec",
		Value: &openapiv2.Schema{
			Description: "The Spec of a GlobalRole",
			Type: &openapiv2.TypeItem{
				Value: []string{"object"},
			},
			Properties: &openapiv2.Properties{
				AdditionalProperties: []*openapiv2.NamedSchema{},
			},
		},
	}
	errorSchema := openapiv2.NamedSchema{
		Name: "InvalidResource",
		Value: &openapiv2.Schema{
			Description: "Resource that is invalid due to multiple types",
			Type: &openapiv2.TypeItem{
				Value: []string{"object", "map"},
			},
		},
	}
	tests := []struct {
		name            string
		documentSchemas []*openapiv2.NamedSchema
		clientErr       error
		inputSchemas    map[string]*types.APISchema
		wantSchemas     map[string]*types.APISchema
		wantErr         bool
	}{
		{
			name:            "basic gvk schema",
			documentSchemas: []*openapiv2.NamedSchema{&gvkSchema},
			inputSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.globalrole": {
					Schema: &schemas.Schema{},
				},
			},
			wantSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.globalrole": {
					Schema: &schemas.Schema{
						Description: gvkSchema.Value.Description,
					},
				},
			},
		},
		{
			name:            "kind has a gvk, but no schema",
			documentSchemas: []*openapiv2.NamedSchema{&gvkSchema},
			inputSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.otherschema": {
					Schema: &schemas.Schema{},
				},
			},
			wantSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.otherschema": {
					Schema: &schemas.Schema{},
				},
			},
		},
		{
			name:            "schema without gvk",
			documentSchemas: []*openapiv2.NamedSchema{&noGVKSchema},
			inputSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.globalrole": {
					Schema: &schemas.Schema{},
				},
			},
			wantSchemas: map[string]*types.APISchema{
				"management.cattle.io.v3.globalrole": {
					Schema: &schemas.Schema{},
				},
			},
		},
		{
			name:      "discovery error",
			clientErr: fmt.Errorf("server not available"),
			wantErr:   true,
		},
		{
			name:            "invalid models",
			documentSchemas: []*openapiv2.NamedSchema{&errorSchema},
			wantErr:         true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			document := openapiv2.Document{
				Definitions: &openapiv2.Definitions{
					AdditionalProperties: test.documentSchemas,
				},
			}
			fakeDiscovery := fakeDiscovery{
				Document:    &document,
				DocumentErr: test.clientErr,
			}
			gotErr := addDescription(&fakeDiscovery, test.inputSchemas)
			if test.wantErr {
				require.Error(t, gotErr)
			} else {
				require.NoError(t, gotErr)
			}
			// inputSchemas are modified in place
			require.Equal(t, test.wantSchemas, test.inputSchemas)
		})
	}
}
