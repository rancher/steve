// Package ownership provides a single place to describe "who owns what" for
// resources whose visibility is scoped per-user by something other than
// Kubernetes RBAC (eg: ext.cattle.io Tokens and Kubeconfigs, which are
// served by rancher's extension apiserver and filtered by a
// cattle.io/user-id label rather than by SubjectAccessReview).
//
// Steve itself has no hardcoded knowledge of which GVKs need this -- that's
// rancher-specific domain knowledge that rancher/rancher's pkg/ext owns (the
// actual GVKs and label constants live in pkg/ext/stores/tokens and
// pkg/ext/stores/kubeconfig). rancher/rancher registers a Filter for each
// such GVK during extension-apiserver startup, in
// pkg/ext/stores/install.go's InstallStores, right where those stores are
// otherwise wired up.
package ownership

import (
	"sync"

	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

// Filter scopes visibility of a resource to its owning user, for resources
// where visibility isn't determined by RBAC alone.
type Filter interface {
	// SQLFilter returns the additional filter that should be applied when
	// listing this resource out of steve's SQLite cache for the given user.
	// Returns nil (no restriction) when the user is an admin.
	SQLFilter(userInfo user.Info, isAdmin bool) *sqltypes.OrFilter

	// Matches reports whether an object with the given labels should be
	// visible to the given user. Used by callers (like the count store)
	// that only have an object's metadata in-hand -- e.g.
	// counts.Store.getCount, which sees *summary.SummarizedObject values
	// out of the in-memory clustercache, not unstructured.Unstructured --
	// and can't run a SQL-level filter.
	Matches(userInfo user.Info, isAdmin bool, labels map[string]string) bool
}

var (
	mu       sync.RWMutex
	registry = map[schema.GroupVersionKind]Filter{}
)

// Register adds a Filter for the given GVK. Called by rancher/rancher
// during extension-apiserver startup (see pkg/ext/stores/install.go's
// InstallStores) for each resource whose visibility rancher's stores scope
// per-user rather than by RBAC. Also used directly by integration tests to
// register a Filter for a test CRD's GVK, letting them exercise the shared
// list/count filtering logic without needing a real extension apiserver.
func Register(gvk schema.GroupVersionKind, f Filter) {
	mu.Lock()
	defer mu.Unlock()
	registry[gvk] = f
}

// Unregister removes any Filter registered for gvk. Primarily useful for
// tests that register a Filter for the duration of a single test.
func Unregister(gvk schema.GroupVersionKind) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, gvk)
}

// Lookup returns the Filter registered for gvk, if any.
func Lookup(gvk schema.GroupVersionKind) (Filter, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := registry[gvk]
	return f, ok
}
