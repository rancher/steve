package resources

import (
	"context"

	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/schemaserver/store/apiroot"
	"github.com/rancher/steve/pkg/schemaserver/subscribe"
	"github.com/rancher/steve/pkg/schemaserver/types"
	"github.com/rancher/steve/pkg/server/resources/apigroups"
	"github.com/rancher/steve/pkg/server/resources/clusters"
	"github.com/rancher/steve/pkg/server/resources/common"
	"github.com/rancher/steve/pkg/server/resources/counts"
	"github.com/rancher/steve/pkg/server/resources/helm"
	"github.com/rancher/steve/pkg/server/resources/userpreferences"
	"github.com/rancher/steve/pkg/server/store/proxy"
	"k8s.io/client-go/discovery"
)

func DefaultSchemas(ctx context.Context, baseSchema *types.APISchemas, ccache clustercache.ClusterCache, cg proxy.ClientGetter) (*types.APISchemas, error) {
	counts.Register(baseSchema, ccache)
	subscribe.Register(baseSchema)
	apiroot.Register(baseSchema, []string{"v1"}, []string{"proxy:/apis"})
	userpreferences.Register(baseSchema, cg)
	helm.Register(baseSchema)

	err := clusters.Register(ctx, baseSchema, cg, ccache)
	return baseSchema, err
}

func DefaultSchemaTemplates(cf *client.Factory, lookup accesscontrol.AccessSetLookup, discovery discovery.DiscoveryInterface) []schema.Template {
	return []schema.Template{
		common.DefaultTemplate(cf, lookup),
		apigroups.Template(discovery),
		{
			ID:        "configmap",
			Formatter: helm.DropHelmData,
		},
		{
			ID:        "secret",
			Formatter: helm.DropHelmData,
		},
	}
}
