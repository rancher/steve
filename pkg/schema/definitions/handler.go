package definitions

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema/converter"
	wranglerDefinition "github.com/rancher/wrangler/v2/pkg/schemas/definition"
	"github.com/rancher/wrangler/v2/pkg/schemas/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
)

var (
	internalServerErrorCode = validation.ErrorCode{
		Status: http.StatusInternalServerError,
		Code:   "InternalServerError",
	}
	notRefreshedErrorCode = validation.ErrorCode{
		Status: http.StatusServiceUnavailable,
		Code:   "SchemasNotRefreshed",
	}
)

// SchemaDefinitionHandler is a byID handler for a specific schema, which provides field definitions for all schemas.
// Does not implement any method allowing a caller to list definitions for all schemas.
type SchemaDefinitionHandler struct {
	sync.RWMutex

	// baseSchema are the schemas (which may not represent a real CRD) added to the server
	baseSchema *types.APISchemas
	// client is the discovery client used to get the groups/resources/fields from kubernetes.
	client discovery.DiscoveryInterface
	// models are the cached models from the last response from kubernetes.
	models *proto.Models
	// schemaToModel is a map of the schema name to the model for that schema. Can be used to load the
	// top-level definition for a schema, which can then be processed by the schemaFieldVisitor.
	schemaToModel map[string]string
}

// Refresh writeLocks and updates the cache with new schemaDefinitions. Will result in a call to kubernetes to retrieve
// the openAPI schemas.
func (s *SchemaDefinitionHandler) Refresh() error {
	openapi, err := s.client.OpenAPISchema()
	if err != nil {
		return fmt.Errorf("unable to fetch openapi definition: %w", err)
	}
	models, err := proto.NewOpenAPIData(openapi)
	if err != nil {
		return fmt.Errorf("unable to parse openapi definition into models: %w", err)
	}
	groups, err := s.client.ServerGroups()
	if err != nil {
		return fmt.Errorf("unable to retrieve groups: %w", err)
	}
	s.Lock()
	defer s.Unlock()
	nameIndex := s.indexSchemaNames(models, groups)
	s.schemaToModel = nameIndex
	s.models = &models
	return nil
}

// byIDHandler is the Handler method for a request to get the schema definition for a specifc schema. Will use the
// cached models found during the last refresh as part of this process.
func (s *SchemaDefinitionHandler) byIDHandler(request *types.APIRequest) (types.APIObject, error) {
	// pseudo-access check, designed to make sure that users have access to the schema for the definition that they
	// are accessing.
	requestSchema := request.Schemas.LookupSchema(request.Name)
	if requestSchema == nil {
		return types.APIObject{}, apierror.NewAPIError(validation.NotFound, "no such schema")
	}

	// lock only in read-mode so that we don't read while refresh writes. Only use a read-lock - using a write lock
	// would make this endpoint only usable by one caller at a time
	s.RLock()
	defer s.RUnlock()

	if baseSchema := s.baseSchema.LookupSchema(requestSchema.ID); baseSchema != nil {
		// if this schema is a base schema it won't be in the model cache. In this case, and only this case, we process
		// the fields independently
		definitions := baseSchemaToDefinition(*requestSchema)
		return types.APIObject{
			ID:   request.Name,
			Type: "schemaDefinition",
			Object: schemaDefinition{
				DefinitionType: requestSchema.ID,
				Definitions:    definitions,
			},
		}, nil
	}

	if s.models == nil {
		return types.APIObject{}, apierror.NewAPIError(notRefreshedErrorCode, "schema definitions not yet refreshed")
	}
	models := *s.models
	modelName, ok := s.schemaToModel[requestSchema.ID]
	if !ok {
		return types.APIObject{}, apierror.NewAPIError(notRefreshedErrorCode, "no model found for schema, try again after refresh")
	}
	model := models.LookupModel(modelName)
	protoKind, ok := model.(*proto.Kind)
	if !ok {
		errorMsg := fmt.Sprintf("model for %s was type %T, not a proto.Kind", modelName, model)
		return types.APIObject{}, apierror.NewAPIError(internalServerErrorCode, errorMsg)
	}
	definitions := map[string]definition{}
	visitor := schemaFieldVisitor{
		definitions: definitions,
		models:      models,
	}
	protoKind.Accept(&visitor)

	return types.APIObject{
		ID:   request.Name,
		Type: "schemaDefinition",
		Object: schemaDefinition{
			DefinitionType: modelName,
			Definitions:    definitions,
		},
	}, nil
}

// indexSchemaNames returns a map of schemaID to the modelName for a given schema. Will use the preferred version of a
// resource if possible. Can return an error if unable to find groups.
func (s *SchemaDefinitionHandler) indexSchemaNames(models proto.Models, groups *metav1.APIGroupList) map[string]string {
	preferredResourceVersions := map[string]string{}
	if groups != nil {
		for _, group := range groups.Groups {
			preferredResourceVersions[group.Name] = group.PreferredVersion.Version
		}
	}
	schemaToModel := map[string]string{}
	for _, modelName := range models.ListModels() {
		protoKind, ok := models.LookupModel(modelName).(*proto.Kind)
		if !ok {
			// no need to process models that aren't kinds
			continue
		}
		gvk := converter.GetGVKForKind(protoKind)
		if gvk == nil {
			// not all kinds are for top-level resources, since these won't have a schema,
			// we can safely continue
			continue
		}
		schemaID := converter.GVKToSchemaID(*gvk)
		prefVersion := preferredResourceVersions[gvk.Group]
		_, ok = schemaToModel[schemaID]
		// we always add the preferred version to the map. However, if this isn't the preferred version the preferred group could
		// be missing this resource (e.x. v1alpha1 has a resource, it's removed in v1). In those cases, we add the model name
		// only if we don't already have an entry. This way we always choose the preferred, if possible, but still have 1 version
		// for everything
		if !ok || prefVersion == gvk.Version {
			schemaToModel[schemaID] = modelName
		}
	}
	return schemaToModel
}

// baseSchemaToDefinition converts a given schema to the definition map. This should only be used with baseSchemas, whose definitions
// are expected to be set by another application and may not be k8s resources.
func baseSchemaToDefinition(schema types.APISchema) map[string]definition {
	definitions := map[string]definition{}
	def := definition{
		Description:    schema.Description,
		Type:           schema.ID,
		ResourceFields: map[string]definitionField{},
	}
	for fieldName, field := range schema.ResourceFields {
		fieldType, subType := parseFieldType(field.Type)
		def.ResourceFields[fieldName] = definitionField{
			Type:        fieldType,
			SubType:     subType,
			Description: field.Description,
			Required:    field.Required,
		}
	}
	definitions[schema.ID] = def
	return definitions
}

// parseFieldType parses a schemas.Field's type to a type (first return) and subType (second return)
func parseFieldType(fieldType string) (string, string) {
	subType := wranglerDefinition.SubType(fieldType)
	if wranglerDefinition.IsMapType(fieldType) {
		return "map", subType
	}
	if wranglerDefinition.IsArrayType(fieldType) {
		return "array", subType
	}
	if wranglerDefinition.IsReferenceType(fieldType) {
		return "reference", subType
	}
	return fieldType, ""
}
