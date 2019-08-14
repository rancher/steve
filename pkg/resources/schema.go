package resources

import (
	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/naok/pkg/resources/common"
	"github.com/rancher/naok/pkg/resources/counts"
	"github.com/rancher/naok/pkg/resources/schema"
	"github.com/rancher/norman/pkg/store/proxy"
	"github.com/rancher/norman/pkg/subscribe"
	"github.com/rancher/norman/pkg/types"
)

func SchemaFactory(getter proxy.ClientGetter, as *accesscontrol.AccessStore) *schema.Collection {
	baseSchema := types.EmptySchemas()
	collection := schema.NewCollection(baseSchema, as)

	counts.Register(baseSchema)
	subscribe.Register(baseSchema)

	common.Register(collection, getter)

	return collection
}
