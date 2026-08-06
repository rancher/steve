package client

import (
	"github.com/rancher/apiserver/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ClientGetter is a dynamic kubernetes client factory. It is implemented by [Factory].
type ClientGetter interface {
	IsImpersonating() bool
	K8sInterface(ctx *types.APIRequest) (kubernetes.Interface, error)
	AdminK8sInterface() (kubernetes.Interface, error)
	Client(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	DynamicClient(ctx *types.APIRequest, warningHandler rest.WarningHandler) (dynamic.Interface, error)
	AdminClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableAdminClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableAdminClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
}

var _ ClientGetter = (*Factory)(nil)
