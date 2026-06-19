package watchlist

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	wschemas "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func builtinSchema(gvk schema.GroupVersionKind) *types.APISchema {
	s := &types.APISchema{Schema: &wschemas.Schema{}}
	attributes.SetGVK(s, gvk)
	return s
}

func aggregatedSchema(gvk schema.GroupVersionKind) *types.APISchema {
	s := builtinSchema(gvk)
	attributes.SetAggregated(s, true)
	return s
}

func TestDisabled(t *testing.T) {
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	antreaGVK := schema.GroupVersionKind{Group: "controlplane.antrea.io", Version: "v1beta2", Kind: "AddressGroup"}

	t.Run("nil schema keeps watch-list enabled", func(t *testing.T) {
		assert.False(t, Disabled(nil))
	})

	t.Run("built-in resource keeps watch-list enabled", func(t *testing.T) {
		assert.False(t, Disabled(builtinSchema(deploymentGVK)))
	})

	t.Run("aggregated resource not whitelisted disables watch-list", func(t *testing.T) {
		assert.True(t, Disabled(aggregatedSchema(antreaGVK)))
	})

	t.Run("aggregated resource on the whitelist keeps watch-list enabled", func(t *testing.T) {
		Whitelist[antreaGVK] = true
		t.Cleanup(func() { delete(Whitelist, antreaGVK) })
		assert.False(t, Disabled(aggregatedSchema(antreaGVK)))
	})

	t.Run("whitelisting one aggregated GVK does not enable another", func(t *testing.T) {
		Whitelist[antreaGVK] = true
		t.Cleanup(func() { delete(Whitelist, antreaGVK) })
		otherGVK := schema.GroupVersionKind{Group: "controlplane.antrea.io", Version: "v1beta2", Kind: "AppliedToGroup"}
		assert.True(t, Disabled(aggregatedSchema(otherGVK)))
	})
}
