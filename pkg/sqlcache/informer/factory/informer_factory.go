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
	wg       wait.Group
	dbClient db.Client

	// ctx determines when informers need to stop
	ctx    context.Context
	cancel context.CancelFunc

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
	ctx, cancel := context.WithCancel(context.Background())
	return &CacheFactory{
		wg: wait.Group{},

		ctx:    ctx,
		cancel: cancel,

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
	// First of all block Reset() until we are done
	f.mutex.RLock()
	cache, err := f.cacheFor(ctx, fields, externalUpdateInfo, selfUpdateInfo, transform, client, gvk, namespaced, watchable)
	if err != nil {
		f.mutex.RUnlock()
		return nil, err
	}
	return cache, nil
}

func (f *CacheFactory) cacheFor(ctx context.Context, fields [][]string, externalUpdateInfo *sqltypes.ExternalGVKUpdates, selfUpdateInfo *sqltypes.ExternalGVKUpdates, transform cache.TransformFunc, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool, watchable bool) (*Cache, error) {
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

	// At this point an informer-specific mutex (gi.mutex) is guaranteed to exist. Lock it
	gi.mutex.Lock()
	defer gi.mutex.Unlock()

	// Then: if the informer really was not created yet (first time here or previous times have errored out)
	// actually create the informer
	if gi.informer == nil {
		start := time.Now()
		log.Infof("CacheFor STARTS creating informer for %v", gvk)
		defer func() {
			log.Infof("CacheFor IS DONE creating informer for %v (took %v)", gvk, time.Now().Sub(start))
		}()

		_, encryptResourceAlways := defaultEncryptedResourceTypes[gvk]
		shouldEncrypt := f.encryptAll || encryptResourceAlways
		i, err := f.newInformer(f.ctx, client, fields, externalUpdateInfo, selfUpdateInfo, transform, gvk, f.dbClient, shouldEncrypt, namespaced, watchable, f.gcInterval, f.gcKeepCount)
		if err != nil {
			return nil, err
		}

		err = i.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			if !watchable && errors.IsMethodNotSupported(err) {
				// expected, continue without logging
				return
			}
			cache.DefaultWatchErrorHandler(ctx, r, err)
		})
		if err != nil {
			return nil, err
		}

		f.wg.StartWithChannel(f.ctx.Done(), i.Run)

		gi.informer = i
	}

	if !cache.WaitForCacheSync(f.ctx.Done(), gi.informer.HasSynced) {
		return nil, fmt.Errorf("failed to sync SQLite Informer cache for GVK %v", gvk)
	}

	// At this point the informer is ready, return it
	return &Cache{ByOptionsLister: gi.informer}, nil
}

// DoneWithCache must be called for every CacheFor call.
//
// This ensures that there aren't any inflight list requests while we are resetting the database.
func (f *CacheFactory) DoneWithCache(_ *Cache) {
	f.mutex.RUnlock()
}

// Reset cancels ctx which stops any running informers, assigns a new ctx, resets the GVK-informer cache, and resets
// the database connection which wipes any current sqlite database at the default location.
func (f *CacheFactory) Reset() error {
	if f.dbClient == nil {
		// nothing to reset
		return nil
	}

	// first of all wait until all CacheFor() calls that create new informers are finished. Also block any new ones
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// now that we are alone, stop all informers created until this point
	f.cancel()
	f.wg.Wait()
	f.ctx, f.cancel = context.WithCancel(context.Background())

	// and get rid of all references to those informers and their mutexes
	f.informersMutex.Lock()
	defer f.informersMutex.Unlock()
	f.informers = make(map[schema.GroupVersionKind]*guardedInformer)

	// finally, reset the DB connection
	_, err := f.dbClient.NewConnection(false)
	if err != nil {
		return err
	}

	return nil
}
