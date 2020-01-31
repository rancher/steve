package resources

import (
	"github.com/rancher/norman/v2/pkg/store/apiroot"
	"github.com/rancher/norman/v2/pkg/subscribe"
	"github.com/rancher/norman/v2/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/steve/pkg/resources/apigroups"
	"github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/core"
	"github.com/rancher/steve/pkg/resources/counts"
	"github.com/rancher/steve/pkg/resources/schema"
	"k8s.io/client-go/kubernetes"
)

func SchemaFactory(
	cf *client.Factory,
	as *accesscontrol.AccessStore,
	k8s kubernetes.Interface,
	ccache clustercache.ClusterCache,
) (*schema.Collection, error) {
	baseSchema := types.EmptySchemas()
	collection := schema.NewCollection(baseSchema, as)

	core.Register(collection)
	counts.Register(baseSchema, ccache)
	subscribe.Register(baseSchema)
	apigroups.Register(baseSchema, k8s.Discovery())
	apiroot.Register(baseSchema, []string{"v1"}, []string{"proxy:/apis"})

	if err := common.Register(collection, cf); err != nil {
		return nil, err
	}

	return collection, nil
}
