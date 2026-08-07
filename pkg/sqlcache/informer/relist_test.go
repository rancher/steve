package informer

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/store"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// TestRelistAfterWatchError exercises the path the user actually hits in
// rancher/rancher#55228: the reflector's watch dies, the reflector re-lists
// from a source whose contents have changed during the gap, and steve's
// SQLite cache must end up reflecting the new state.
//
// This is a behavioral test, deliberately not pinned to any particular
// client-go FIFO. On client-go v0.34/v0.35 (DeltaFIFO) the deletion travels
// through processDeltas → Store.Delete(DeletedFinalStateUnknown). On v0.36+
// with AtomicFIFO it travels through processReplacedAllInfo → Store.Replace.
// The assertion is the same either way: rows that vanished from the source
// must vanish from SQLite.
func TestRelistAfterWatchError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gvk := schema.GroupVersionKind{Group: "fruits.cattle.io", Version: "v1", Kind: "Banana"}

	bananaA := makeRelistBanana("a", "10")
	bananaB := makeRelistBanana("b", "20")

	src := newRelistSource(gvk)
	src.setItems([]*unstructured.Unstructured{bananaA, bananaB})

	m, err := encryption.NewManager()
	require.NoError(t, err)
	dbClient, dbPath, err := db.NewClient(ctx, nil, m, m, true)
	require.NoError(t, err)
	defer cleanTempFiles(dbPath)

	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)
	s, err := store.NewStore(
		ctx, example, cache.DeletionHandlingMetaNamespaceKeyFunc,
		dbClient, false, gvk, informerNameFromGVK(gvk), nil, nil,
	)
	require.NoError(t, err)

	loi, err := NewListOptionIndexer(ctx, s, ListOptionIndexerOptions{
		IsNamespaced: false,
		GCKeepCount:  1000,
	})
	require.NoError(t, err)

	sii := cache.NewSharedIndexInformer(
		&noWatchListListWatch{ListWatch: src.listWatch()},
		example,
		0, // resyncPeriod
		cache.Indexers{},
	)
	UnsafeSet(sii, "indexer", loi)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go sii.RunWithContext(runCtx)
	require.True(t, cache.WaitForCacheSync(runCtx.Done(), sii.HasSynced), "informer never synced")

	// Both Bananas land in SQLite after the initial list.
	require.Eventually(t, func() bool {
		_, exA, _ := sii.GetStore().GetByKey("a")
		_, exB, _ := sii.GetStore().GetByKey("b")
		return exA && exB
	}, 5*time.Second, 50*time.Millisecond, "Bananas never landed in SQLite")

	// Simulate "B was deleted while the watch was down": remove B from the
	// source, then close the watch with an error to force the reflector to
	// abandon ListAndWatch and re-list from scratch.
	src.setItems([]*unstructured.Unstructured{bananaA})
	require.Eventually(t, func() bool {
		return src.currentWatcher() != nil
	}, 2*time.Second, 50*time.Millisecond, "reflector never opened a watch")
	src.currentWatcher().Error(&metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonGone,
		Code:    410,
		Message: "test-induced watch close",
	})

	// After the re-list, A must still be present and B must be gone.
	require.Eventually(t, func() bool {
		_, exA, errA := sii.GetStore().GetByKey("a")
		_, exB, errB := sii.GetStore().GetByKey("b")
		return errA == nil && errB == nil && exA && !exB
	}, 10*time.Second, 100*time.Millisecond,
		"SQLite cache not reconciled after watch-error + re-list (B should be gone, A should remain)")
}

// relistSource is a test list/watch source that lets the test control both
// the contents returned by List and the active watcher returned by Watch.
type relistSource struct {
	gvk schema.GroupVersionKind

	mu      sync.Mutex
	items   []*unstructured.Unstructured
	rv      int
	watcher *watch.FakeWatcher
}

func newRelistSource(gvk schema.GroupVersionKind) *relistSource {
	return &relistSource{gvk: gvk}
}

func (s *relistSource) setItems(items []*unstructured.Unstructured) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = items
}

func (s *relistSource) currentWatcher() *watch.FakeWatcher {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watcher
}

func (s *relistSource) listWatch() *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(_ metav1.ListOptions) (runtime.Object, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.rv++
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   s.gvk.Group,
				Version: s.gvk.Version,
				Kind:    s.gvk.Kind + "List",
			})
			list.SetResourceVersion(strconv.Itoa(s.rv))
			for _, it := range s.items {
				list.Items = append(list.Items, *it.DeepCopy())
			}
			return list, nil
		},
		WatchFunc: func(_ metav1.ListOptions) (watch.Interface, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.watcher = watch.NewFake()
			return s.watcher, nil
		},
	}
}

func makeRelistBanana(name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "fruits.cattle.io/v1",
			"kind":       "Banana",
			"metadata": map[string]any{
				"name":            name,
				"resourceVersion": rv,
			},
		},
	}
}
