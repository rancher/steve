package clustercache

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/steve/pkg/synthetic"
	"github.com/rancher/steve/pkg/watchlist"
	"github.com/rancher/wrangler/v3/pkg/merr"
	"github.com/rancher/wrangler/v3/pkg/summary/client"
	"github.com/rancher/wrangler/v3/pkg/summary/informer"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Handler func(gvr schema2.GroupVersionKind, key string, obj runtime.Object) error
type ChangeHandler func(gvr schema2.GroupVersionKind, key string, obj, oldObj runtime.Object) error

type ClusterCache interface {
	Get(gvk schema2.GroupVersionKind, namespace, name string) (interface{}, bool, error)
	List(gvk schema2.GroupVersionKind) []interface{}
	OnAdd(ctx context.Context, handler Handler)
	OnRemove(ctx context.Context, handler Handler)
	OnChange(ctx context.Context, handler ChangeHandler)
	OnSchemas(schemas *schema.Collection) error
}

type event struct {
	add    bool
	gvk    schema2.GroupVersionKind
	obj    runtime.Object
	oldObj runtime.Object
}

type watcher struct {
	ctx      context.Context
	cancel   func()
	informer cache.SharedIndexInformer
	gvk      schema2.GroupVersionKind
	gvr      schema2.GroupVersionResource
}

type clusterCache struct {
	sync.RWMutex

	ctx           context.Context
	dynamicClient dynamic.Interface
	summaryClient client.ExtendedInterface
	watchers      map[schema2.GroupVersionKind]*watcher
	workqueue     workqueue.DelayingInterface

	addHandlers    cancelCollection
	removeHandlers cancelCollection
	changeHandlers cancelCollection
}

func NewClusterCache(ctx context.Context, dynamicClient dynamic.Interface) ClusterCache {
	c := &clusterCache{
		ctx:           ctx,
		dynamicClient: dynamicClient,
		summaryClient: client.NewForExtendedDynamicClient(dynamicClient),
		watchers:      map[schema2.GroupVersionKind]*watcher{},
		workqueue:     workqueue.NewNamedDelayingQueue("cluster-cache"),
	}
	go c.start()
	return c
}

// listOnlyPollInterval is how often the cluster cache re-lists resources that
// support "list" but not "watch" to keep their cached data current.
const listOnlyPollInterval = 30 * time.Second

// validSchema reports whether a schema can be cached. A schema must at least
// support "list"; resources that also support "watch" are cached via a normal
// informer, while list-only resources are cached via a polling watcher (see
// schemaSupportsWatch).
func validSchema(schema *types.APISchema) bool {
	for _, verb := range attributes.Verbs(schema) {
		if verb == "list" {
			return true
		}
	}
	return false
}

// schemaSupportsWatch reports whether the schema advertises the "watch" verb.
// Schemas that do not are cached by polling instead of a server-side watch.
func schemaSupportsWatch(schema *types.APISchema) bool {
	for _, verb := range attributes.Verbs(schema) {
		if verb == "watch" {
			return true
		}
	}
	return false
}

func (h *clusterCache) addResourceEventHandler(gvk schema2.GroupVersionKind, informer cache.SharedIndexInformer) {
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if rObj, ok := obj.(runtime.Object); ok {
				h.workqueue.Add(event{
					add: true,
					obj: rObj,
					gvk: gvk,
				})
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if rObj, ok := newObj.(runtime.Object); ok {
				if rOldObj, ok := oldObj.(runtime.Object); ok {
					h.workqueue.Add(event{
						obj:    rObj,
						oldObj: rOldObj,
						gvk:    gvk,
					})
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if rObj, ok := obj.(runtime.Object); ok {
				h.workqueue.Add(event{
					obj: rObj,
					gvk: gvk,
				})
			}
		},
	})
}

func (h *clusterCache) OnSchemas(schemas *schema.Collection) error {
	h.Lock()

	var (
		gvks   = map[schema2.GroupVersionKind]bool{}
		toWait []*watcher
	)

	for _, id := range schemas.IDs() {
		schema := schemas.Schema(id)
		if !validSchema(schema) {
			continue
		}

		gvr := attributes.GVR(schema)
		gvk := attributes.GVK(schema)
		gvks[gvk] = true

		if h.watchers[gvk] != nil {
			continue
		}

		ctx, cancel := context.WithCancel(h.ctx)
		var sharedInformer cache.SharedIndexInformer
		if schemaSupportsWatch(schema) {
			opts := &client.Options{
				Schema: schema.Schema,
			}
			summaryClient := h.summaryClient
			if watchlist.Disabled(schema) {
				// Non-whitelisted aggregated API: disable watch-list (fall back to LIST+WATCH).
				summaryClient = &noWatchListClient{ExtendedInterface: h.summaryClient}
			}
			summaryInformer := informer.NewFilteredSummaryInformerWithOptions(summaryClient, gvr, opts, metav1.NamespaceAll, 2*time.Hour,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, nil)
			sharedInformer = summaryInformer.Informer()
		} else {
			// Resource supports "list" but not "watch" (e.g. some CRDs that only
			// expose get/list/create/delete). A normal informer cannot watch it,
			// so synthesize watch events by polling to keep the cache current.
			logrus.Infof("Polling metadata for %s (no watch verb)", gvk)
			sharedInformer = h.newListOnlyInformer(ctx, gvk, gvr)
		}
		w := &watcher{
			ctx:      ctx,
			cancel:   cancel,
			gvk:      gvk,
			gvr:      gvr,
			informer: sharedInformer,
		}
		h.watchers[gvk] = w
		toWait = append(toWait, w)

		logrus.Infof("Watching metadata for %s", w.gvk)
		h.addResourceEventHandler(w.gvk, w.informer)
		go w.informer.Run(w.ctx.Done())
	}

	for gvk, w := range h.watchers {
		if !gvks[gvk] {
			logrus.Infof("Stopping metadata watch on %s", gvk)
			w.cancel()
			delete(h.watchers, gvk)
		}
	}
	h.Unlock()

	for _, w := range toWait {
		ctx, cancel := context.WithTimeout(w.ctx, 15*time.Minute)
		if !cache.WaitForCacheSync(ctx.Done(), w.informer.HasSynced) {
			logrus.Errorf("failed to sync cache for %v", w.gvk)
			cancel()
			w.cancel()
			h.Lock()
			if h.watchers[w.gvk] == w {
				delete(h.watchers, w.gvk)
			}
			h.Unlock()
		}
		cancel()
	}

	return nil
}

func (h *clusterCache) Get(gvk schema2.GroupVersionKind, namespace, name string) (interface{}, bool, error) {
	h.RLock()
	defer h.RUnlock()

	w, ok := h.watchers[gvk]
	if !ok {
		return nil, false, nil
	}

	var key string
	if namespace == "" {
		key = name
	} else {
		key = namespace + "/" + name
	}

	return w.informer.GetStore().GetByKey(key)
}

func (h *clusterCache) List(gvk schema2.GroupVersionKind) []interface{} {
	h.RLock()
	defer h.RUnlock()

	w, ok := h.watchers[gvk]
	if !ok {
		return nil
	}

	return w.informer.GetStore().List()
}

func (h *clusterCache) start() {
	defer h.workqueue.ShutDown()
	for {
		eventObj, ok := h.workqueue.Get()
		if ok {
			break
		}

		event := eventObj.(event)
		h.RLock()
		w := h.watchers[event.gvk]
		h.RUnlock()
		if w == nil {
			h.workqueue.Done(eventObj)
			continue
		}

		key := toKey(event.obj)
		if event.oldObj != nil {
			_, err := callAll(h.changeHandlers.List(), event.gvk, key, event.obj, event.oldObj)
			if err != nil {
				logrus.Errorf("failed to handle add event: %v", err)
			}
		} else if event.add {
			_, err := callAll(h.addHandlers.List(), event.gvk, key, event.obj, nil)
			if err != nil {
				logrus.Errorf("failed to handle add event: %v", err)
			}
		} else {
			_, err := callAll(h.removeHandlers.List(), event.gvk, key, event.obj, nil)
			if err != nil {
				logrus.Errorf("failed to handle remove event: %v", err)
			}
		}
		h.workqueue.Done(eventObj)
	}
}

func toKey(obj runtime.Object) string {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	ns := meta.GetNamespace()
	if ns == "" {
		return meta.GetName()
	}
	return ns + "/" + meta.GetName()
}

func (h *clusterCache) OnAdd(ctx context.Context, handler Handler) {
	h.addHandlers.Add(ctx, handler)
}

func (h *clusterCache) OnRemove(ctx context.Context, handler Handler) {
	h.removeHandlers.Add(ctx, handler)
}

func (h *clusterCache) OnChange(ctx context.Context, handler ChangeHandler) {
	h.changeHandlers.Add(ctx, handler)
}

func callAll(handlers []interface{}, gvr schema2.GroupVersionKind, key string, obj, oldObj runtime.Object) (runtime.Object, error) {
	var errs []error
	for _, handler := range handlers {
		if f, ok := handler.(Handler); ok {
			if err := f(gvr, key, obj); err != nil {
				errs = append(errs, err)
			}
		}
		if f, ok := handler.(ChangeHandler); ok {
			if err := f(gvr, key, obj, oldObj); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return obj, merr.NewErrors(errs...)
}

// noWatchListClient wraps a summary client so its informer disables watch-list (via IsWatchListSemanticsUnSupported) and falls back to LIST+WATCH.
type noWatchListClient struct {
	client.ExtendedInterface
}

func (n *noWatchListClient) IsWatchListSemanticsUnSupported() bool {
	return true
}

// newListOnlyInformer builds a dynamic informer for a resource that supports
// "list" but not "watch". It stores *unstructured.Unstructured objects and
// synthesizes Added/Modified/Deleted events by periodically re-listing, so the
// cluster cache (and consumers such as counts) stay populated for these
// resources.
func (h *clusterCache) newListOnlyInformer(ctx context.Context, gvk schema2.GroupVersionKind, gvr schema2.GroupVersionResource) cache.SharedIndexInformer {
	resourceClient := h.dynamicClient.Resource(gvr)
	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)
	lw := &noWatchListListWatcher{
		ListWatch: &cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return resourceClient.List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				watchCtx, cancel := context.WithCancel(ctx)
				return synthetic.NewSyntheticWatcher(watchCtx, cancel, gvk).Watch(resourceClient, options, listOnlyPollInterval)
			},
		},
	}
	return cache.NewSharedIndexInformer(lw, example, 2*time.Hour,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
}

// noWatchListListWatcher wraps a ListWatch so its reflector disables watch-list
// (via IsWatchListSemanticsUnSupported) and falls back to LIST+WATCH. This keeps
// the reflector's initial LIST seeding the store immediately, instead of waiting
// for the synthetic watcher's first poll to emit an initial-sync bookmark.
type noWatchListListWatcher struct {
	*cache.ListWatch
}

func (n *noWatchListListWatcher) IsWatchListSemanticsUnSupported() bool {
	return true
}
