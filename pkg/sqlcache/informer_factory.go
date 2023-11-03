package sqlcache

import (
	"os"
	"sync"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// InformerFactory builds Informer instances and keeps a cache of instances it created
type InformerFactory struct {
	mutex  sync.Mutex
	wg     wait.Group
	stopCh chan struct{}

	cache map[schema.GroupVersionKind]*Informer
}

const InformerCacheDbPath = "informer_cache.db"

// NewInformerFactory returns an informer factory instance
func NewInformerFactory() *InformerFactory {
	return &InformerFactory{
		wg:     wait.Group{},
		stopCh: make(chan struct{}),
		cache:  map[schema.GroupVersionKind]*Informer{},
	}
}

// InformerFor returns an informer for a given user (taken from apiOp), GVK (taken from schema) using the specified client
func (f *InformerFactory) InformerFor(apiOp *types.APIRequest, client dynamic.ResourceInterface, schema *types.APISchema) (*Informer, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if len(f.cache) == 0 {
		err := os.RemoveAll(InformerCacheDbPath)
		if err != nil {
			return nil, err
		}
	}

	gvk := attributes.GVK(schema)

	result, ok := f.cache[gvk]
	if !ok {
		i, err := NewInformer(client, gvk, InformerCacheDbPath)
		if err != nil {
			return nil, err
		}
		f.cache[gvk] = i
		f.wg.StartWithChannel(f.stopCh, i.Run)

		if !cache.WaitForCacheSync(f.stopCh, i.HasSynced) {
			logrus.Errorf("Failed to sync SQLite Informer cache for GVK %v", gvk)
		}

		return i, nil
	}

	return result, nil
}
