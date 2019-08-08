package resources

import (
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	"github.com/rancher/norman/pkg/types/values"
)

func Formatter(request *types.APIRequest, resource *types.RawResource) {
	selfLink := convert.ToString(values.GetValueN(resource.Values, "metadata", "selfLink"))
	if selfLink == "" {
		return
	}

	u := request.URLBuilder.RelativeToRoot(selfLink)
	resource.Links["view"] = u

	if _, ok := resource.Links["update"]; ok {
		resource.Links["update"] = u
	}
}
