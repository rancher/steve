package definitions

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema/converter"
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
		prefVersion := preferredResourceVersions[gvk.Group]
		// if we don't have a known preferred version for this group or we are the preferred version
		// add this as the model name for the schema
		if prefVersion == "" || prefVersion == gvk.Version {
			schemaID := converter.GVKToSchemaID(*gvk)
			schemaToModel[schemaID] = modelName
		}
	}
	return schemaToModel
}
