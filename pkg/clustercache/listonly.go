package clustercache

import (
	"sync"

	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
)

// Steve decides whether to cache a resource via a server-side watch or via a
// polling "list-only" watcher purely from the verbs the resource advertises in
// discovery (see schemaSupportsWatch). That is the right behaviour in
// production, but it leaves no in-cluster resource that is both list-only and
// controllable: CRDs always advertise "watch", and the genuinely list-only
// resources (eg: componentstatuses, metrics.k8s.io) can't have instances
// created on demand.
//
// forcedListOnly lets a caller mark a GVK as list-only regardless of its
// advertised verbs. Integration tests use it to drive a CRD they control (so
// they can create/delete instances and assert exact counts) through the same
// synthetic-watch code path that aggregated, list-only APIs - such as Kasten
// K10's actions.kio.kasten.io - exercise in the field.
var (
	forcedListOnlyMu sync.RWMutex
	forcedListOnly   = map[schema2.GroupVersionKind]bool{}
)

// ForceListOnly makes the cluster cache treat gvk as list-only (no server-side
// watch), so it is cached by periodically re-listing instead of watching. It is
// primarily a testing hook; see the package comment above.
func ForceListOnly(gvk schema2.GroupVersionKind) {
	forcedListOnlyMu.Lock()
	defer forcedListOnlyMu.Unlock()
	forcedListOnly[gvk] = true
}

// UnforceListOnly undoes a previous ForceListOnly for gvk. Tests typically call
// it via defer so the override can't leak into other tests.
func UnforceListOnly(gvk schema2.GroupVersionKind) {
	forcedListOnlyMu.Lock()
	defer forcedListOnlyMu.Unlock()
	delete(forcedListOnly, gvk)
}

// isForcedListOnly reports whether gvk has been marked list-only via
// ForceListOnly.
func isForcedListOnly(gvk schema2.GroupVersionKind) bool {
	forcedListOnlyMu.RLock()
	defer forcedListOnlyMu.RUnlock()
	return forcedListOnly[gvk]
}
