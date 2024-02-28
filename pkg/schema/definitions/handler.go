package definitions

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema/converter"
	"github.com/rancher/wrangler/v2/pkg/schemas/validation"
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

// schemaDefinitionHandler is a byID handler for a specific schema, which provides field definitions for all schemas.
// Does not implement any method allowing a caller to list definitions for all schemas.
type schemaDefinitionHandler struct {
	sync.RWMutex

	// lastRefresh is the last time that the handler retrieved models from kubernetes.
	lastRefresh time.Time
	// refreshStale is the duration between lastRefresh and the next refresh of models.
	refreshStale time.Duration
	// client is the discovery client used to get the groups/resources/fields from kubernetes.
	client discovery.DiscoveryInterface
	// models are the cached models from the last response from kubernetes.
	models *proto.Models
	// schemaToModel is a map of the schema name to the model for that schema. Can be used to load the
	// top-level definition for a schema, which can then be processed by the schemaFieldVisitor.
	schemaToModel map[string]string
}

// byIDHandler is the Handler method for a request to get the schema definition for a specifc schema. Will use the
// cached models found during the last refresh as part of this process.
func (s *schemaDefinitionHandler) byIDHandler(request *types.APIRequest) (types.APIObject, error) {
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

// needsRefresh readLocks and checks if the cache needs to be refreshed.
func (s *schemaDefinitionHandler) needsRefresh() bool {
	s.RLock()
	defer s.RUnlock()
	if s.lastRefresh.IsZero() {
		return true
	}
	return s.lastRefresh.Add(s.refreshStale).Before(time.Now())
}

// refresh writeLocks and updates the cache with new schemaDefinitions. Will result in a call to kubernetes to retrieve
// the openAPI schemas.
func (s *schemaDefinitionHandler) refresh() error {
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
	// indexSchemaNames may successfully refresh some definitions, but still return an error
	// in these cases, store what we could find, but still return up an error
	if nameIndex != nil {
		s.schemaToModel = nameIndex
		s.lastRefresh = time.Now()
	}
	if err != nil {
		return fmt.Errorf("unable to index schema name to model name: %w", err)
	}
	return nil
}

// indexSchemaNames returns a map of schemaID to the modelName for a given schema. Will use the preferred version of a
// resource if possible. May return a map and an error if it was able to index some schemas but not others.
func (s *schemaDefinitionHandler) indexSchemaNames(models proto.Models) (map[string]string, error) {
	_, resourceLists, err := s.client.ServerGroupsAndResources()
	// this may occasionally fail to discover certain groups, but we still can refresh the others in those cases
	if _, ok := err.(*discovery.ErrGroupDiscoveryFailed); err != nil && !ok {
		return nil, fmt.Errorf("unable to retrieve groups and resources: %w", err)
	}
	preferredResourceVersions := map[schema.GroupKind]string{}
	for _, resourceList := range resourceLists {
		if resourceList == nil {
			continue
		}
		groupVersion, gvErr := schema.ParseGroupVersion(resourceList.GroupVersion)
		// we may fail to parse the GV of one group, but can still parse out the others
		if gvErr != nil {
			err = errors.Join(err, fmt.Errorf("unable to parse group version %s: %w", resourceList.GroupVersion, gvErr))
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
		// if we don't have a known preferred version for this group or we are the preferred version
		// add this as the model name for the schema
		if !ok || prefVersion == gvk.Version {
			schemaID := converter.GVKToSchemaID(*gvk)
			schemaToModel[schemaID] = modelName
		}
	}
	return schemaToModel, err
}
