package definitions

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema/converter"
	"github.com/rancher/wrangler/pkg/schemas/validation"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

type SchemaDefinitionHandler struct {
	sync.RWMutex

	lastRefresh   time.Time
	refreshStale  time.Duration
	client        discovery.DiscoveryInterface
	models        *proto.Models
	schemaToModel map[string]string
}

// ByIDHandler is the Handler method for a request to get the schema definition for a specifc schema. Will use the
// cached models found during the last refresh as part of this process
func (s *SchemaDefinitionHandler) ByIDHandler(request *types.APIRequest) (types.APIObject, error) {
	// pseudo-access check, designed to make sure that users have access to the schema for the definition that they
	// are accessing.
	requestSchema := request.Schemas.LookupSchema(request.Name)
	if requestSchema == nil {
		return types.APIObject{}, apierror.NewAPIError(validation.NotFound, "no such schema")
	}

	if s.needsRefresh() {
		err := s.refresh()
		if err != nil {
			logrus.Errorf("error refreshing schemas %s", err.Error())
			return types.APIObject{}, apierror.NewAPIError(internalServerErrorCode, "error refreshing schemas")
		}
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

// needsRefresh readLocks and checks if the cache needs to be refreshed
func (s *SchemaDefinitionHandler) needsRefresh() bool {
	s.RLock()
	defer s.RUnlock()
	if s.lastRefresh.IsZero() {
		return true
	}
	return s.lastRefresh.Add(s.refreshStale).Before(time.Now())
}

// refresh writeLocks and updates the cache with new schemaDefinitions. Will result in a call to OpenAPISchemas
func (s *SchemaDefinitionHandler) refresh() error {
	s.Lock()
	defer s.Unlock()
	openapi, err := s.client.OpenAPISchema()
	if err != nil {
		return fmt.Errorf("unable to fetch openapi definition: %w", err)
	}
	models, err := proto.NewOpenAPIData(openapi)
	if err != nil {
		return fmt.Errorf("unable to parse openapi definition into models: %w", err)
	}
	s.models = &models
	nameIndex, err := s.indexSchemaNames(models)
	if err != nil {
		return fmt.Errorf("unable to index schema name to model name: %w", err)
	}
	s.schemaToModel = nameIndex
	s.lastRefresh = time.Now()
	return nil
}

// indexSchemaNames returns a map of schemaID to the modelName for a given schema. Will use the preferred version of a resource if possible
func (s *SchemaDefinitionHandler) indexSchemaNames(models proto.Models) (map[string]string, error) {
	_, resourceLists, err := s.client.ServerGroupsAndResources()
	if gd, ok := err.(*discovery.ErrGroupDiscoveryFailed); ok {
		// this may occasionally fail to discover certain groups, but we still can refresh the others
		logrus.Errorf("in definition refresh failed to read API for groups %v", gd.Groups)
	} else if err != nil {
		return nil, fmt.Errorf("unable to retrieve groups and resources: %w", err)
	}
	preferredResourceVersions := map[schema.GroupKind]string{}
	for _, resourceList := range resourceLists {
		groupVersion, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil {
			logrus.Errorf("unable to parse group version %s: %s", resourceList.GroupVersion, err.Error())
			continue
		}
		if resourceList == nil {
			continue
		}
		for _, resource := range resourceList.APIResources {
			gk := schema.GroupKind{
				Group: groupVersion.Group,
				Kind:  resource.Kind,
			}
			// per the resource docs, if the resource.Version is empty, the preferred version for
			// this resource is the version of the APIResourceList it is in
			if resource.Version == "" || resource.Version == groupVersion.Version {
				preferredResourceVersions[gk] = groupVersion.Version
			}
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
		gk := schema.GroupKind{
			Group: gvk.Group,
			Kind:  gvk.Kind,
		}
		prefVersion, ok := preferredResourceVersions[gk]
		// if we don't have a known preferred version for this group or we are the prefered version
		// add this as the model name for the schema
		if !ok || prefVersion == gvk.Version {
			schemaID := converter.GVKToSchemaID(*gvk)
			schemaToModel[schemaID] = modelName
		}
	}
	return schemaToModel, err
}
