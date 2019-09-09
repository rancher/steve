package resources

import (
	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/clustercache"
	"github.com/rancher/naok/pkg/resources/apigroups"
	"github.com/rancher/naok/pkg/resources/common"
	"github.com/rancher/naok/pkg/resources/core"
	"github.com/rancher/naok/pkg/resources/counts"
	"github.com/rancher/naok/pkg/resources/helmrelease"
	"github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/norman/pkg/store/apiroot"
	"github.com/rancher/norman/pkg/store/proxy"
	"github.com/rancher/norman/pkg/subscribe"
	"github.com/rancher/norman/pkg/types"
	corev1controller "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	"k8s.io/client-go/kubernetes"
)

func SchemaFactory(getter proxy.ClientGetter,
	as *accesscontrol.AccessStore,
	k8s kubernetes.Interface,
	ccache clustercache.ClusterCache,
	configMaps corev1controller.ConfigMapClient,
	secrets corev1controller.SecretClient,
) *schema.Collection {
	baseSchema := types.EmptySchemas()
	collection := schema.NewCollection(baseSchema, as)

	core.Register(collection)

	counts.Register(baseSchema, ccache)
	subscribe.Register(baseSchema)
	apigroups.Register(baseSchema, k8s.Discovery())
	apiroot.Register(baseSchema, []string{"v1"}, []string{"proxy:/apis"})
	helmrelease.Register(baseSchema, configMaps, secrets)

	common.Register(collection, getter)

	return collection
}
