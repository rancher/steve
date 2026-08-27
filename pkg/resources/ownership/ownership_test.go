package ownership

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestLookupRequiresExactGVK guards against a real regression: a Filter
// registered with a Version-less (or otherwise partial) GVK silently never
// matches the fully-qualified GVK steve looks up at request time (see
// pkg/attributes.GVK, which always populates Version) -- Lookup must NOT
// match a GVK that only partially overlaps with what's registered.
func TestLookupRequiresExactGVK(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "test.cattle.io", Version: "v1", Kind: "Thing"}
	Register(gvk, NewUserIDLabelFilter("cattle.io/user-id"))
	defer Unregister(gvk)

	tests := []struct {
		description string
		lookup      schema.GroupVersionKind
		expectFound bool
	}{
		{
			description: "exact GVK match is found",
			lookup:      schema.GroupVersionKind{Group: "test.cattle.io", Version: "v1", Kind: "Thing"},
			expectFound: true,
		},
		{
			description: "missing Version does not match a versioned registration",
			lookup:      schema.GroupVersionKind{Group: "test.cattle.io", Kind: "Thing"},
			expectFound: false,
		},
		{
			description: "different Version does not match",
			lookup:      schema.GroupVersionKind{Group: "test.cattle.io", Version: "v2", Kind: "Thing"},
			expectFound: false,
		},
		{
			description: "different Kind does not match",
			lookup:      schema.GroupVersionKind{Group: "test.cattle.io", Version: "v1", Kind: "OtherThing"},
			expectFound: false,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			_, ok := Lookup(test.lookup)
			if ok != test.expectFound {
				t.Errorf("Lookup(%s): expected found=%v, got %v", test.lookup, test.expectFound, ok)
			}
		})
	}
}
