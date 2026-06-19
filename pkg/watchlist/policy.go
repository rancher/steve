// Package watchlist decides whether Steve's informers use the client-go watch-list protocol (sendInitialEvents) or fall back to LIST+WATCH.
package watchlist

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Whitelist holds aggregated (external) GVKs known to implement watch-list correctly, which may therefore keep watch-list enabled.
var Whitelist = map[schema.GroupVersionKind]bool{
	{Group: "ext.cattle.io", Version: "v1", Kind: "Token"}:      true,
	{Group: "ext.cattle.io", Version: "v1", Kind: "Kubeconfig"}: true,
}

// Disabled reports whether watch-list should be disabled for the schema's informer: true only for aggregated, non-whitelisted APIs (nil and built-in stay enabled, failing safe).
func Disabled(s *types.APISchema) bool {
	if s == nil {
		return false
	}
	if !attributes.Aggregated(s) {
		// Built-in (core + CRDs): compliant by construction.
		return false
	}
	// Aggregated: only allowed to use watch-list if explicitly whitelisted.
	return !Whitelist[attributes.GVK(s)]
}
