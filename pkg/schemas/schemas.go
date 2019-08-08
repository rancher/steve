package schemas

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	openapi_v2 "github.com/googleapis/gnostic/OpenAPIv2"
	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/naok/pkg/resources"
	"github.com/rancher/norman/pkg/store/proxy"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	apiextcontrollerv1beta1 "github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io/v1beta1"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/apiregistration.k8s.io/v1"
	"github.com/rancher/wrangler/pkg/merr"
	"github.com/sirupsen/logrus"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	apiv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-openapi/pkg/util/proto"
)

type SchemaFactory interface {
	Schemas(subjectKey string, access *accesscontrol.AccessSet, schemasFactory func() (*types.Schemas, error)) (*types.Schemas, error)
	ByGVR(gvr schema.GroupVersionResource) string
}

type handler struct {
	formatter   types.Formatter
	schemas     *schemas
	schemaStore types.Store
	client      discovery.DiscoveryInterface
}

type schemas struct {
	toSync int32

	sync.Mutex
	apiGroups   []*metav1.APIGroup
	gvkToName   map[schema.GroupVersionKind]string
	gvrToName   map[schema.GroupVersionResource]string
	openSchemas map[string]*types.Schema
	schemas     map[string]*types.Schema
	access      sync.Map
}

func (s *schemas) reset() {
	s.apiGroups = nil
	s.gvkToName = map[schema.GroupVersionKind]string{}
	s.gvrToName = map[schema.GroupVersionResource]string{}
	s.openSchemas = map[string]*types.Schema{}
	s.schemas = map[string]*types.Schema{}
	s.access.Range(func(key, value interface{}) bool {
		s.access.Delete(key)
		return true
	})
}

func (s *schemas) ByGVR(gvr schema.GroupVersionResource) string {
	return s.gvrToName[gvr]
}

func Register(ctx context.Context, clientGetter proxy.ClientGetter, discovery discovery.DiscoveryInterface, crd apiextcontrollerv1beta1.CustomResourceDefinitionController,
	apiService v1.APIServiceController) SchemaFactory {

	h := &handler{
		formatter:   resources.Formatter,
		client:      discovery,
		schemas:     &schemas{},
		schemaStore: proxy.NewProxyStore(clientGetter),
	}
	apiService.OnChange(ctx, "schema", h.OnChangeAPIService)
	crd.OnChange(ctx, "schema", h.OnChangeCRD)

	return h.schemas
}

func (h *handler) OnChangeCRD(key string, crd *v1beta1.CustomResourceDefinition) (*v1beta1.CustomResourceDefinition, error) {
	return crd, h.queueRefresh()
}

func (h *handler) OnChangeAPIService(key string, api *apiv1.APIService) (*apiv1.APIService, error) {
	return api, h.queueRefresh()
}

func schemaID(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return fmt.Sprintf("io.k8s.api.core.%s.%s", gvk.Version, gvk.Kind)
	}
	return fmt.Sprintf("io.k8s.api.%s.%s.%s", gvk.Group, gvk.Version, gvk.Kind)
}

func (h *handler) queueRefresh() error {
	atomic.StoreInt32(&h.schemas.toSync, 1)

	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := h.refreshAll(); err != nil {
			logrus.Errorf("failed to sync schemas: %v", err)
		}
	}()

	return nil
}

func (h *handler) refreshAll() error {
	h.schemas.Lock()
	defer h.schemas.Unlock()

	if !h.needToSync() {
		return nil
	}

	logrus.Info("Refreshing all schemas")

	groups, resourceLists, err := h.client.ServerGroupsAndResources()
	if err != nil {
		return err
	}

	openapi, err := h.client.OpenAPISchema()
	if err != nil {
		return err
	}

	h.schemas.reset()
	h.schemas.apiGroups = groups

	if err := populate(openapi, h.schemas); err != nil {
		return err
	}

	var errs []error
	for _, resourceList := range resourceLists {
		gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil {
			errs = append(errs, err)
		}

		if err := h.refresh(gv, resourceList); err != nil {
			errs = append(errs, err)
		}
	}

	return merr.NewErrors(errs...)
}

func (h *handler) needToSync() bool {
	old := atomic.SwapInt32(&h.schemas.toSync, 0)
	return old == 1
}

func (h *handler) refresh(gv schema.GroupVersion, resources *metav1.APIResourceList) error {
	for _, resource := range resources.APIResources {
		if strings.Contains(resource.Name, "/") {
			continue
		}

		gvk := schema.GroupVersionKind{
			Group:   gv.Group,
			Version: gv.Version,
			Kind:    resource.Kind,
		}
		gvr := gvk.GroupVersion().WithResource(resource.Name)

		logrus.Infof("APIVersion %s/%s Kind %s", gvk.Group, gvk.Version, gvk.Kind)

		schema := h.schemas.openSchemas[h.schemas.gvkToName[gvk]]
		if schema == nil {
			schema = &types.Schema{
				ID:      schemaID(gvk),
				Type:    "schema",
				Dynamic: true,
			}
			attributes.SetGVK(schema, gvk)
		}

		schema.PluralName = resource.Name
		attributes.SetAPIResource(schema, resource)
		schema.Store = h.schemaStore
		schema.Formatter = h.formatter

		h.schemas.schemas[schema.ID] = schema
		h.schemas.gvkToName[gvk] = schema.ID
		h.schemas.gvrToName[gvr] = schema.ID
	}

	return nil
}

func toField(schema proto.Schema) types.Field {
	f := types.Field{
		Description: schema.GetDescription(),
		Nullable:    true,
		Create:      true,
		Update:      true,
	}
	switch v := schema.(type) {
	case *proto.Array:
		f.Type = "array[" + toField(v.SubType).Type + "]"
	case *proto.Primitive:
		if v.Type == "number" {
			f.Type = "int"
		} else {
			f.Type = v.Type
		}
	case *proto.Map:
		f.Type = "map[" + toField(v.SubType).Type + "]"
	case *proto.Kind:
		parts := v.Path.Get()
		f.Type = parts[len(parts)-1]
	case proto.Reference:
		f.Type = v.SubSchema().GetPath().String()
	case *proto.Arbitrary:
	default:
		logrus.Errorf("unknown type: %v", schema)
		f.Type = "json"
	}

	return f
}

func modelToSchema(modelName string, k *proto.Kind, schemas *schemas) {
	s := types.Schema{
		ID:             modelName,
		Type:           "schema",
		ResourceFields: map[string]types.Field{},
		Attributes:     map[string]interface{}{},
		Description:    k.GetDescription(),
		Dynamic:        true,
	}

	for fieldName, schemaField := range k.Fields {
		s.ResourceFields[fieldName] = toField(schemaField)
	}

	for _, fieldName := range k.RequiredFields {
		if f, ok := s.ResourceFields[fieldName]; ok {
			f.Required = true
			s.ResourceFields[fieldName] = f
		}
	}

	if ms, ok := k.Extensions["x-kubernetes-group-version-kind"].([]interface{}); ok {
		for _, mv := range ms {
			if m, ok := mv.(map[interface{}]interface{}); ok {

				gvk := schema.GroupVersionKind{
					Group:   convert.ToString(m["group"]),
					Version: convert.ToString(m["version"]),
					Kind:    convert.ToString(m["kind"]),
				}

				attributes.SetGVK(&s, gvk)

				schemas.gvkToName[gvk] = s.ID
			}
		}
	}

	schemas.openSchemas[s.ID] = &s
}

func populate(openapi *openapi_v2.Document, schemas *schemas) error {
	models, err := proto.NewOpenAPIData(openapi)
	if err != nil {
		return err
	}

	for _, modelName := range models.ListModels() {
		model := models.LookupModel(modelName)
		if k, ok := model.(*proto.Kind); ok {
			modelToSchema(modelName, k, schemas)
		}
	}

	return nil
}
