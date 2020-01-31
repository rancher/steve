package common

import (
	"github.com/rancher/norman/v2/pkg/store/proxy"
	"github.com/rancher/norman/v2/pkg/types"
	"github.com/rancher/norman/v2/pkg/types/convert"
	"github.com/rancher/norman/v2/pkg/types/values"
	"github.com/rancher/steve/pkg/resources/schema"
)

func Register(collection *schema.Collection, clientGetter proxy.ClientGetter) error {
	collection.AddTemplate(&schema.Template{
		Store:     proxy.NewProxyStore(clientGetter),
		Formatter: Formatter,
		Mapper:    &DefaultColumns{},
	})

	return nil
}

func Formatter(request *types.APIRequest, resource *types.RawResource) {
	selfLink := convert.ToString(values.GetValueN(resource.Values, "metadata", "selfLink"))
	if selfLink == "" {
		return
	}

	u := request.URLBuilder.RelativeToRoot(selfLink)
	resource.Links["view"] = u

	if _, ok := resource.Links["update"]; !ok {
		resource.Links["update"] = u
	}
}
