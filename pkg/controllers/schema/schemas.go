package schema

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	schema2 "github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/naok/pkg/resources/schema/converter"
	apiextcontrollerv1beta1 "github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io/v1beta1"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/apiregistration.k8s.io/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/client-go/discovery"
	apiv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

type SchemasHandler interface {
	OnSchemas(schemas *schema2.Collection) error
}

type handler struct {
	sync.Mutex

	toSync  int32
	schemas *schema2.Collection
	client  discovery.DiscoveryInterface
	crd     apiextcontrollerv1beta1.CustomResourceDefinitionClient
	handler SchemasHandler
}

func Register(ctx context.Context,
	discovery discovery.DiscoveryInterface,
	crd apiextcontrollerv1beta1.CustomResourceDefinitionController,
	apiService v1.APIServiceController,
	schemasHandler SchemasHandler,
	schemas *schema2.Collection) (init func() error) {

	h := &handler{
		client:  discovery,
		schemas: schemas,
		handler: schemasHandler,
		crd:     crd,
	}

	apiService.OnChange(ctx, "schema", h.OnChangeAPIService)
	crd.OnChange(ctx, "schema", h.OnChangeCRD)

	return func() error {
		h.queueRefresh()
		return h.refreshAll()
	}
}

func (h *handler) OnChangeCRD(key string, crd *v1beta1.CustomResourceDefinition) (*v1beta1.CustomResourceDefinition, error) {
	h.queueRefresh()
	return crd, nil
}

func (h *handler) OnChangeAPIService(key string, api *apiv1.APIService) (*apiv1.APIService, error) {
	h.queueRefresh()
	return api, nil
}

func (h *handler) queueRefresh() {
	atomic.StoreInt32(&h.toSync, 1)

	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := h.refreshAll(); err != nil {
			logrus.Errorf("failed to sync schemas: %v", err)
			atomic.StoreInt32(&h.toSync, 1)
		}
	}()
}

func (h *handler) refreshAll() error {
	h.Lock()
	defer h.Unlock()

	if !h.needToSync() {
		return nil
	}

	logrus.Info("Refreshing all schemas")
	schemas, err := converter.ToSchemas(h.crd, h.client)
	if err != nil {
		return err
	}

	h.schemas.Reset(schemas)
	if h.handler != nil {
		return h.handler.OnSchemas(h.schemas)
	}

	return nil
}

func (h *handler) needToSync() bool {
	old := atomic.SwapInt32(&h.toSync, 0)
	return old == 1
}
