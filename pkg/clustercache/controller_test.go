package clustercache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	steveschema "github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/rancher/wrangler/v3/pkg/summary/client"
	v1schema "github.com/rancher/wrangler/v3/pkg/schemas"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/workqueue"
)

// blockingListClient blocks List until unblockCh is closed and signals listStarted
// on the first List call, so tests can synchronise deterministically.
type blockingListClient struct {
	listStarted chan struct{}
	unblockCh   chan struct{}
	startedOnce sync.Once
}

var _ client.ExtendedInterface = (*blockingListClient)(nil)

func (m *blockingListClient) IsWatchListSemanticsUnSupported() bool { return true }
func (m *blockingListClient) Resource(_ schema2.GroupVersionResource) client.NamespaceableResourceInterface {
	return m
}
func (m *blockingListClient) ResourceWithOptions(_ schema2.GroupVersionResource, _ *client.Options) client.NamespaceableResourceInterface {
	return m
}
func (m *blockingListClient) Namespace(string) client.ResourceInterface { return m }

func (m *blockingListClient) List(ctx context.Context, _ metav1.ListOptions) (*summary.SummarizedObjectList, error) {
	m.startedOnce.Do(func() { close(m.listStarted) })
	select {
	case <-m.unblockCh:
		return &summary.SummarizedObjectList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *blockingListClient) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func fakeSchema(id string, gvr schema2.GroupVersionResource, gvk schema2.GroupVersionKind) *types.APISchema {
	s := &types.APISchema{
		Schema: &v1schema.Schema{ID: id},
	}
	attributes.SetGVR(s, gvr)
	attributes.SetGVK(s, gvk)
	attributes.SetVerbs(s, []string{"list", "watch"})
	return s
}

func newTestCache(ctx context.Context, sc client.ExtendedInterface) *clusterCache {
	return &clusterCache{
		ctx:           ctx,
		summaryClient: sc,
		watchers:      map[schema2.GroupVersionKind]*watcher{},
		workqueue:     workqueue.NewTypedDelayingQueue[any](),
	}
}

// TestOnSchemasDoesNotHoldClusterCacheLockWhileWaitingForSync verifies that
// cc.List() is not blocked behind the write lock while OnSchemas() is waiting
// for a broken informer to sync.
func TestOnSchemasDoesNotHoldClusterCacheLockWhileWaitingForSync(t *testing.T) {
	listStarted := make(chan struct{})
	unblockCh := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc := newTestCache(ctx, &blockingListClient{
		listStarted: listStarted,
		unblockCh:   unblockCh,
	})

	gvr := schema2.GroupVersionResource{Group: "s3.aws.upbound.io", Version: "v1beta1", Resource: "buckets"}
	gvk := schema2.GroupVersionKind{Group: "s3.aws.upbound.io", Version: "v1beta1", Kind: "Bucket"}

	col := steveschema.NewCollection(ctx, types.EmptyAPISchemas(), nil)
	col.Reset(map[string]*types.APISchema{
		"buckets.s3.aws.upbound.io": fakeSchema("buckets.s3.aws.upbound.io", gvr, gvk),
	})

	onSchemasDone := make(chan struct{})
	go func() {
		defer close(onSchemasDone)
		_ = cc.OnSchemas(col)
	}()

	// Wait until the informer has entered its blocking List call.
	// cc.List below then verifies that OnSchemas does not keep the
	// cluster-cache write lock while waiting for informer synchronization.
	select {
	case <-listStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("informer List did not start within timeout")
	}

	// cc.List must return immediately — it must not block behind the write lock.
	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		cc.List(gvk)
	}()

	select {
	case <-listDone:
	case <-time.After(5 * time.Second):
		t.Fatal("cc.List blocked behind the cache write lock while OnSchemas was waiting for sync")
	}

	// Unblock the informer and confirm OnSchemas exits cleanly.
	close(unblockCh)
	select {
	case <-onSchemasDone:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSchemas did not return after informer was unblocked")
	}
}

// TestOnSchemasStaleWatcherDeletionProtection verifies that a failing older watcher
// does not delete a newer replacement watcher for the same GVK.
//
// The test guarantees ordering by holding the cache lock while cancelling watcher A
// and installing watcher B, so that watcher A's cleanup (which must acquire the same
// lock) is forced to run after watcher B is already in the map.
func TestOnSchemasStaleWatcherDeletionProtection(t *testing.T) {
	listStarted := make(chan struct{})
	unblockA := make(chan struct{})
	defer close(unblockA)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc := newTestCache(ctx, &blockingListClient{
		listStarted: listStarted,
		unblockCh:   unblockA,
	})

	gvr := schema2.GroupVersionResource{Group: "s3.aws.upbound.io", Version: "v1beta1", Resource: "buckets"}
	gvk := schema2.GroupVersionKind{Group: "s3.aws.upbound.io", Version: "v1beta1", Kind: "Bucket"}
	id := "buckets.s3.aws.upbound.io"

	col := steveschema.NewCollection(ctx, types.EmptyAPISchemas(), nil)
	col.Reset(map[string]*types.APISchema{id: fakeSchema(id, gvr, gvk)})

	// Start watcher A — it will block inside WaitForCacheSync.
	onSchemasDone := make(chan struct{})
	go func() {
		defer close(onSchemasDone)
		_ = cc.OnSchemas(col)
	}()

	select {
	case <-listStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher A List did not start")
	}

	cc.RLock()
	watcherA := cc.watchers[gvk]
	cc.RUnlock()
	if watcherA == nil {
		t.Fatal("expected watcher A to be registered")
	}

	watcherB := &watcher{gvk: gvk, gvr: gvr}

	// Hold the cache lock while cancelling A and installing B.
	// Watcher A's cleanup must acquire this same lock, so it cannot run
	// until after B is in the map — making the ordering fully deterministic.
	cc.Lock()
	watcherA.cancel()
	cc.watchers[gvk] = watcherB
	cc.Unlock()

	// Watcher A's context is cancelled, so WaitForCacheSync returns immediately.
	// Its cleanup runs the identity check: h.watchers[gvk] == watcherA is false,
	// so it must not delete watcherB.
	select {
	case <-onSchemasDone:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher A OnSchemas did not return after context cancellation")
	}

	cc.RLock()
	got := cc.watchers[gvk]
	cc.RUnlock()

	if got != watcherB {
		t.Fatal("stale watcher A cleanup incorrectly deleted watcher B")
	}
}
