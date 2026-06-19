package converter

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	wschemas "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

func schemaFor(gvk schema.GroupVersionKind) *types.APISchema {
	s := &types.APISchema{Schema: &wschemas.Schema{}}
	attributes.SetGVK(s, gvk)
	return s
}

func TestAddAPIServiceAggregation(t *testing.T) {
	aggregated := schema.GroupVersionKind{Group: "ext.example", Version: "v1", Kind: "Foo"}
	otherVersion := schema.GroupVersionKind{Group: "ext.example", Version: "v2", Kind: "Foo"}
	builtin := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	schemas := map[string]*types.APISchema{
		"aggregated":   schemaFor(aggregated),
		"otherVersion": schemaFor(otherVersion),
		"builtin":      schemaFor(builtin),
	}
	apiServices := []*apiregv1.APIService{
		{Spec: apiregv1.APIServiceSpec{Group: "ext.example", Version: "v1", Service: &apiregv1.ServiceReference{}}}, // aggregated
		{Spec: apiregv1.APIServiceSpec{Group: "apps", Version: "v1"}},                                              // local (built-in): nil .spec.service
		nil,
	}

	AddAPIServiceAggregation(apiServices, schemas)

	assert.True(t, attributes.Aggregated(schemas["aggregated"]), "aggregated group/version should be tagged")
	assert.False(t, attributes.Aggregated(schemas["otherVersion"]), "same group, different version should not be tagged")
	assert.False(t, attributes.Aggregated(schemas["builtin"]), "built-in (local APIService) should not be tagged")
}
