package sqlcache

import (
	"fmt"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"sync"
)

// InformerFactory builds Informer instances and keeps a cache of instances it created
type InformerFactory struct {
	mutex  sync.Mutex
	wg     wait.Group
	stopCh chan struct{}

	cache map[cacheKey]Informer
}

// cacheKey instances uniquely identify Informer instances created by InformerFactory
// one Informer is created per (user, GVK) combination
type cacheKey struct {
	uuid string
	gvk  schema.GroupVersionKind
}

// NewInformerFactory returns an informer factory instance
func NewInformerFactory() *InformerFactory {
	return &InformerFactory{
		wg:     wait.Group{},
		stopCh: make(chan struct{}),
		cache:  map[cacheKey]Informer{},
	}
}

// InformerFor returns an informer for a given user (taken from apiOp), GVK (taken from schema) using the specified client
func (f *InformerFactory) InformerFor(apiOp *types.APIRequest, client dynamic.ResourceInterface, schema *types.APISchema) (Informer, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	uuid := ""
	user, ok := request.UserFrom(apiOp.Context())
	if ok && user.GetName() != "system:cattle:error" {
		uuid = user.GetUID()
	}

	gvk := attributes.GVK(schema)

	key := cacheKey{
		uuid: uuid,
		gvk:  gvk,
	}

	result, ok := f.cache[key]
	if !ok {
		sqlitePath := fmt.Sprintf("informer_cache_%s_%s_%s_%s.db", uuid, gvk.Group, gvk.Version, gvk.Kind)
		i, err := NewInformer(client, gvk, sqlitePath)
		if err != nil {
			return nil, err
		}
		f.cache[key] = i
		f.wg.StartWithChannel(f.stopCh, i.Run)

		if !cache.WaitForCacheSync(f.stopCh, i.HasSynced) {
			logrus.Errorf("Failed to sync SQLite Informer cache for GVK %v, UUID %v", key.gvk, key.uuid)
		}

		return i, nil
	}

	return result, nil
}
