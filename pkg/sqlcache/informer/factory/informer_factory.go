/*
Package factory provides a cache factory for the sql-based cache.
*/
package factory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/log"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/encryption"
	"github.com/rancher/steve/pkg/sqlcache/informer"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// EncryptAllEnvVar is set to "true" if users want all types' data blobs to be encrypted in SQLite
// otherwise only variables in defaultEncryptedResourceTypes will have their blobs encrypted
const EncryptAllEnvVar = "CATTLE_ENCRYPT_CACHE_ALL"

// CacheFactory builds Informer instances and keeps a cache of instances it created
type CacheFactory struct {
	dbClient db.Client

	mutex      sync.RWMutex
	encryptAll bool

	gcInterval  time.Duration
	gcKeepCount int

	newInformer newInformer

	informers      map[schema.GroupVersionKind]*guardedInformer
	informersMutex sync.Mutex
}

type guardedInformer struct {
	informer *informer.Informer
	mutex    *sync.Mutex

	wg     *wait.Group
	ctx    context.Context
	cancel context.CancelFunc
}

type newInformer func(ctx context.Context, client dynamic.ResourceInterface, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, gvk schema.GroupVersionKind, db db.Client, shouldEncrypt bool, namespace bool, watchable bool, gcInterval time.Duration, gcKeepCount int) (*informer.Informer, error)

type Cache struct {
	informer.ByOptionsLister
}

var defaultEncryptedResourceTypes = map[schema.GroupVersionKind]struct{}{
	{
		Version: "v1",
		Kind:    "Secret",
	}: {},
	{
		Group:   "management.cattle.io",
		Version: "v3",
		Kind:    "Token",
	}: {},
}

type CacheFactoryOptions struct {
	// GCInterval is how often to run the garbage collection
	GCInterval time.Duration
	// GCKeepCount is how many events to keep in _events table when gc runs
	GCKeepCount int
}

// NewCacheFactory returns an informer factory instance
// This is currently called from steve via initial calls to `s.cacheFactory.CacheFor(...)`
func NewCacheFactory(opts CacheFactoryOptions) (*CacheFactory, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, err
	}
	dbClient, _, err := db.NewClient(nil, m, m, false)
	if err != nil {
		return nil, err
	}
	return &CacheFactory{
		encryptAll: os.Getenv(EncryptAllEnvVar) == "true",
		dbClient:   dbClient,

		gcInterval:  opts.GCInterval,
		gcKeepCount: opts.GCKeepCount,

		newInformer: informer.NewInformer,
		informers:   map[schema.GroupVersionKind]*guardedInformer{},
	}, nil
}

// CacheFor returns an informer for given GVK, using sql store indexed with fields, using the specified client. For virtual fields, they must be added by the transform function
// and specified by fields to be used for later fields.
//
// Don't forget to call DoneWithCache with the given informer once done with it.
func (f *CacheFactory) CacheFor(ctx context.Context, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool, watchable bool) (*Cache, error) {
	f.mutex.RLock()

	// Second, check if the informer and its accompanying informer-specific mutex exist already in the informers cache
	// If not, start by creating such informer-specific mutex. That is used later to ensure no two goroutines create
	// informers for the same GVK at the same type
	f.informersMutex.Lock()
	// Note: the informers cache is protected by informersMutex, which we don't want to hold for very long because
	// that blocks CacheFor for other GVKs, hence not deferring unlock here
	gi, ok := f.informers[gvk]
	if !ok {
		gi = &guardedInformer{
			informer: nil,
			mutex:    &sync.Mutex{},
		}
		f.informers[gvk] = gi
	}
	f.informersMutex.Unlock()

	start, err := f.getOrCreateCache(ctx, gi, fields, externalUpdateInfo, selfUpdateInfo, transform, client, gvk, namespaced, watchable)
	defer func() {
		if !start.IsZero() {
			log.Infof("CacheFor IS DONE creating informer for %v (took %v)", gvk, time.Since(start))
		}
	}()
	if err != nil {
		f.mutex.RUnlock()
		return nil, err
	}

	if !cache.WaitForCacheSync(gi.ctx.Done(), gi.informer.HasSynced) {
		return nil, fmt.Errorf("failed to sync SQLite Informer cache for GVK %v", gvk)
	}

	return &Cache{
		ByOptionsLister: gi.informer,
	}, nil
}

func (f *CacheFactory) getOrCreateCache(
	ctx context.Context,
	gi *guardedInformer,
	fields [][]string,
	externalUpdateInfo *sqltypes.ExternalGVKUpdates,
	selfUpdateInfo *sqltypes.ExternalGVKUpdates,
	transform cache.TransformFunc,
	client dynamic.ResourceInterface,
	gvk schema.GroupVersionKind,
	namespaced bool,
	watchable bool,
) (time.Time, error) {
	var start time.Time
	// At this point an informer-specific mutex (gi.mutex) is guaranteed to exist. Lock it
	gi.mutex.Lock()
	defer gi.mutex.Unlock()

	// Then: if the informer really was not created yet (first time here or previous times have errored out)
	// actually create the informer
	if gi.informer == nil {
		start = time.Now()
		log.Infof("CacheFor STARTS creating informer for %v", gvk)

		cacheCtx, cacheCancel := context.WithCancel(context.Background())

		_, encryptResourceAlways := defaultEncryptedResourceTypes[gvk]
		shouldEncrypt := f.encryptAll || encryptResourceAlways
		i, err := f.newInformer(cacheCtx, client, fields, externalUpdateInfo, selfUpdateInfo, transform, gvk, f.dbClient, shouldEncrypt, namespaced, watchable, f.gcInterval, f.gcKeepCount)
		if err != nil {
			cacheCancel()
			return start, err
		}

		err = i.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			if !watchable && errors.IsMethodNotSupported(err) {
				// expected, continue without logging
				return
			}
			cache.DefaultWatchErrorHandler(ctx, r, err)
		})
		if err != nil {
			cacheCancel()
			return start, err
		}

		gi.informer = i
		gi.wg = &wait.Group{}
		gi.ctx = cacheCtx
		gi.cancel = cacheCancel
		gi.wg.StartWithChannel(gi.ctx.Done(), i.Run)
	}

	return start, nil
}

// DoneWithCache must be called for every CacheFor call.
//
// This ensures that there aren't any inflight list requests while we are resetting the database.
func (f *CacheFactory) DoneWithCache(c *Cache) {
	f.mutex.RUnlock()
}

// Stop stops the informer of the given GVK and drops the tables in the
// database.
//
// It will block to ensure that the informers are fully stopped and that there
// are no inflight requests before dropping the tables. This means all CacheFor
// calls will have to have called DoneWithCache.
func (f *CacheFactory) Stop(gvk schema.GroupVersionKind) error {
	f.informersMutex.Lock()
	gi, ok := f.informers[gvk]
	if !ok {
		f.informersMutex.Unlock()
		return nil
	}
	f.informersMutex.Unlock()

	gi.mutex.Lock()
	defer gi.mutex.Unlock()

	// We must cancel the context before locking f.mutex because CacheFor
	// might be stuck waiting for the cache to be synced. This would result
	// in a deadlock otherwise.
	gi.cancel()
	gi.wg.Wait()

	f.mutex.Lock()
	defer f.mutex.Unlock()

	err := gi.informer.ByOptionsLister.DropAll()
	if err != nil {
		return fmt.Errorf("dropall: %w", err)
	}
	gi.informer = nil

	return nil
}
