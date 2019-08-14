package resources

import (
	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/resources/apigroups"
	"github.com/rancher/naok/pkg/resources/common"
	"github.com/rancher/naok/pkg/resources/counts"
	"github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/norman/pkg/store/apiroot"
	"github.com/rancher/norman/pkg/store/proxy"
	"github.com/rancher/norman/pkg/subscribe"
	"github.com/rancher/norman/pkg/types"
	"k8s.io/client-go/kubernetes"
)

func SchemaFactory(getter proxy.ClientGetter, as *accesscontrol.AccessStore, k8s kubernetes.Interface) *schema.Collection {
	baseSchema := types.EmptySchemas()
	collection := schema.NewCollection(baseSchema, as)

	counts.Register(baseSchema)
	subscribe.Register(baseSchema)
	apigroups.Register(baseSchema, k8s.Discovery())
	apiroot.Register(baseSchema, []string{"v1"}, nil)

	common.Register(collection, getter)

	return collection
}
