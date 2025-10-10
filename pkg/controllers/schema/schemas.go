package schema

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/resources/common"
	schema2 "github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/schema/converter"
	apiextcontrollerv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiextensions.k8s.io/v1"
	v1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiregistration.k8s.io/v1"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	authorizationv1 "k8s.io/api/authorization/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	apiv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

var (
	listPool        = semaphore.NewWeighted(10)
	typeNameChanges = map[string]string{
		"extensions.v1beta1.ingress": "networking.k8s.io.v1beta1.ingress",
	}
)

const refreshDelay = 500 * time.Millisecond
const alreadySyncingError = "already syncing"

type SchemasHandlerFunc func(schemas *schema2.Collection) error

func (s SchemasHandlerFunc) OnSchemas(schemas *schema2.Collection) error {
	return s(schemas)
}

type handler struct {
	sync.Mutex

	ctx            context.Context
	refreshing     bool
	schemas        *schema2.Collection
	client         discovery.DiscoveryInterface
	cols           *common.DynamicColumns
	crdClient      apiextcontrollerv1.CustomResourceDefinitionClient
	ssar           authorizationv1client.SelfSubjectAccessReviewInterface
	requestChannel chan struct{}
	handler        SchemasHandlerFunc
}

func Register(ctx context.Context,
	cols *common.DynamicColumns,
	discovery discovery.DiscoveryInterface,
	crd apiextcontrollerv1.CustomResourceDefinitionController,
	apiService v1.APIServiceController,
	ssar authorizationv1client.SelfSubjectAccessReviewInterface,
	schemasHandler SchemasHandlerFunc,
	schemas *schema2.Collection) {

	h := &handler{
		ctx:            ctx,
		cols:           cols,
		client:         discovery,
		schemas:        schemas,
		handler:        schemasHandler,
		crdClient:      crd,
		ssar:           ssar,
		requestChannel: make(chan struct{}),
	}

	apiService.OnChange(ctx, "schema", h.OnChangeAPIService)
	crd.OnChange(ctx, "schema", h.OnChangeCRD)
	h.processRequests()
}

func (h *handler) OnChangeCRD(key string, crd *apiextv1.CustomResourceDefinition) (*apiextv1.CustomResourceDefinition, error) {
	h.queueRefresh()
	return crd, nil
}

func (h *handler) OnChangeAPIService(key string, api *apiv1.APIService) (*apiv1.APIService, error) {
	h.queueRefresh()
	return api, nil
}

func (h *handler) processRequests() {
	go func() {
		ticker := time.NewTicker(refreshDelay)
		// No need to collect the requests. We just need to know if we have a pending request to refresh
		haveRequests := false
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-h.requestChannel:
				haveRequests = true
			case <-ticker.C:
				if haveRequests {
					err := h.refreshAll(h.ctx)
					if err == nil {
						haveRequests = false
					} else {
						// Try on the next tick
						logrus.Errorf("processRequests: failed to sync schemas immediately: %v (will retry after a delay)", err)
					}
				}
			}
		}
	}()
}

// This is a weird kind of queue.
// The `refreshAll` function just calls a separate schema-tracker,
// which tracks all pending changes.
// We're really actually just adding a request to apply global changes to the queue, so we can cull out most change events
func (h *handler) queueRefresh() {
	go func() {
		h.requestChannel <- struct{}{}
	}()
}

func isListOrGetable(schema *types.APISchema) bool {
	for _, verb := range attributes.Verbs(schema) {
		switch verb {
		case "list":
			return true
		case "get":
			return true
		}
	}

	return false
}

func IsListWatchable(schema *types.APISchema) bool {
	var (
		canList  bool
		canWatch bool
	)

	for _, verb := range attributes.Verbs(schema) {
		switch verb {
		case "list":
			canList = true
		case "watch":
			canWatch = true
		}
	}

	return canList && canWatch
}

func (h *handler) getColumns(ctx context.Context, schemas map[string]*types.APISchema) error {
	eg := errgroup.Group{}

	for _, schema := range schemas {
		if !isListOrGetable(schema) {
			continue
		}

		if err := listPool.Acquire(ctx, 1); err != nil {
			return err
		}

		s := schema
		eg.Go(func() error {
			defer listPool.Release(1)
			return h.cols.SetColumns(ctx, s)
		})
	}

	return eg.Wait()
}

func (h *handler) refreshAll(ctx context.Context) error {
	h.Lock()
	defer h.Unlock()

	if h.refreshing {
		// Ignore this attempt - it means this function is still running.
		return errors.New(alreadySyncingError)
	}
	h.refreshing = true
	defer func() {
		h.refreshing = false
	}()

	schemas, err := converter.ToSchemas(h.crdClient, h.client)
	if err != nil {
		return err
	}

	filteredSchemas := map[string]*types.APISchema{}
	for _, schema := range schemas {
		if IsListWatchable(schema) {
			if preferredTypeExists(schema, schemas) {
				continue
			}
			if ok, err := h.allowed(ctx, schema); err != nil {
				return err
			} else if !ok {
				continue
			}
		}

		gvk := attributes.GVK(schema)
		if gvk.Kind != "" {
			gvr := attributes.GVR(schema)
			schema.ID = converter.GVKToSchemaID(gvk)
			schema.PluralName = converter.GVRToPluralName(gvr)
		}
		filteredSchemas[schema.ID] = schema
	}

	if err := h.getColumns(h.ctx, filteredSchemas); err != nil {
		return err
	}

	h.schemas.Reset(filteredSchemas)
	if h.handler != nil {
		return h.handler.OnSchemas(h.schemas)
	}

	return nil
}

func preferredTypeExists(schema *types.APISchema, schemas map[string]*types.APISchema) bool {
	if replacement, ok := typeNameChanges[schema.ID]; ok && schemas[replacement] != nil {
		return true
	}
	pg := attributes.PreferredGroup(schema)
	pv := attributes.PreferredVersion(schema)
	if pg == "" && pv == "" {
		return false
	}

	gvk := attributes.GVK(schema)
	if pg != "" {
		gvk.Group = pg
	}
	if pv != "" {
		gvk.Version = pv
	}

	_, ok := schemas[converter.GVKToVersionedSchemaID(gvk)]
	return ok
}

func (h *handler) allowed(ctx context.Context, schema *types.APISchema) (bool, error) {
	gvr := attributes.GVR(schema)
	ssar, err := h.ssar.Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Verb:     "list",
				Group:    gvr.Group,
				Version:  gvr.Version,
				Resource: gvr.Resource,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return ssar.Status.Allowed && !ssar.Status.Denied, nil
}
