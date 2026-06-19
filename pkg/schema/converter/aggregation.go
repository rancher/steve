package converter

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// AddAPIServiceAggregation marks schemas whose group/version is served by an aggregated (extension) API server (an APIService with a non-nil .spec.service).
func AddAPIServiceAggregation(apiServices []*apiregv1.APIService, schemas map[string]*types.APISchema) {
	type groupVersion struct {
		group   string
		version string
	}

	aggregated := make(map[groupVersion]bool)
	for _, apiService := range apiServices {
		if apiService == nil || apiService.Spec.Service == nil {
			// nil .spec.service => served locally by kube-apiserver (built-in/CRD).
			continue
		}
		aggregated[groupVersion{apiService.Spec.Group, apiService.Spec.Version}] = true
	}

	if len(aggregated) == 0 {
		return
	}

	for _, schema := range schemas {
		gv := groupVersion{attributes.Group(schema), attributes.Version(schema)}
		if aggregated[gv] {
			attributes.SetAggregated(schema, true)
		}
	}
}
