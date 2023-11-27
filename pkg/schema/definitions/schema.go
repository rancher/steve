package definitions

import (
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v2/pkg/schemas"
	"k8s.io/client-go/discovery"
)

const (
	gvkExtensionName    = "x-kubernetes-group-version-kind"
	gvkExtensionGroup   = "group"
	gvkExtensionVersion = "version"
	gvkExtensionKind    = "kind"
	defaultDuration     = time.Second * 5
)

// Register registers the schemaDefinition schema.
func Register(baseSchema *types.APISchemas, client discovery.DiscoveryInterface) {
	handler := schemaDefinitionHandler{
		client:       client,
		refreshStale: defaultDuration,
	}
	baseSchema.MustAddSchema(types.APISchema{
		Schema: &schemas.Schema{
			ID:              "schemaDefinition",
			PluralName:      "schemaDefinitions",
			ResourceMethods: []string{"GET"},
		},
		ByIDHandler: handler.byIDHandler,
	})
}

type schemaDefinition struct {
	DefinitionType string                `json:"definitionType"`
	Definitions    map[string]definition `json:"definitions"`
}

type definition struct {
	ResourceFields map[string]definitionField `json:"resourceFields"`
	Type           string                     `json:"type"`
	Description    string                     `json:"description"`
}

type definitionField struct {
	Type        string `json:"type"`
	SubType     string `json:"subtype,omitempty"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}
