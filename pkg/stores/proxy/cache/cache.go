package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/cache"
)

var (
	ErrNotFound = errors.New("key was not found in cache")
)

type SizedRevisionCache struct {
	listRevisionCache *cache.LRUExpireCache
	cacheLock         sync.Mutex
	size              int
	sizeLimit         int
}

type Cacher interface {
	Get(key CacheKey) (*unstructured.UnstructuredList, error)
	Add(key CacheKey, list *unstructured.UnstructuredList) error
}

type CacheKey struct {
	resourcePath string
	revision     string
	namespace    string
	cont         string
}

type cacheObj struct {
	size int
	obj  interface{}
}

func NewSizedRevisionCache(sizeLimit, maxElements int) *SizedRevisionCache {
	return &SizedRevisionCache{
		listRevisionCache: cache.NewLRUExpireCache(maxElements),
		sizeLimit:         sizeLimit,
	}
}

func (c CacheKey) String() string {
	return fmt.Sprintf("resourcePath: %s, revision: %s, namespace: %s, cont: %s", c.resourcePath, c.revision, c.namespace, c.cont)
}

func (s *SizedRevisionCache) Get(key CacheKey) (*unstructured.UnstructuredList, error) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	// check if cache stored all namespaces

	obj, ok := s.listRevisionCache.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	uList, ok := obj.(cacheObj)
	if !ok {
		return nil, fmt.Errorf("could not assert object stored with key [%s] key as UnstructuredList", key)
	}
	return uList.obj.(*unstructured.UnstructuredList), nil
}

func (s *SizedRevisionCache) Add(key CacheKey, list *unstructured.UnstructuredList) error {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	objBytes, err := list.MarshalJSON()
	if err != nil {
		return err
	}

	cacheListObj := cacheObj{
		size: len(objBytes),
		obj:  list,
	}

	currentSize := s.sizeOfCurrentEntry(key)

	if err = s.adjustSize(cacheListObj.size - currentSize); err != nil {
		return err
	}
	s.listRevisionCache.Add(key, cacheListObj, 30*time.Minute)

	return nil
}

func (s *SizedRevisionCache) calculateSize() int {
	var total int
	for _, key := range s.listRevisionCache.Keys() {
		total += s.sizeOfCurrentEntry(key)
	}
	return total
}

func (s *SizedRevisionCache) sizeOfCurrentEntry(key interface{}) int {
	obj, ok := s.listRevisionCache.Get(key)
	if !ok {
		return 0
	}

	cacheObject, ok := obj.(cacheObj)
	if !ok {
		return 0
	}

	return cacheObject.size
}

func (s *SizedRevisionCache) adjustSize(diff int) error {
	if !(s.size+diff > s.sizeLimit) {
		s.size += diff
		return nil
	}

	s.size = s.calculateSize()

	if !(s.size+diff > s.sizeLimit) {
		s.sizeLimit += diff
		return nil
	}

	return fmt.Errorf("[steve proxy cache]: cache is near full with a size of [%d] and limit of [%d], cannot increment size by [%d]", s.size, s.sizeLimit, diff)
}

func GetCacheKey(resourcePath, revision, ns, cont string) CacheKey {
	return CacheKey{
		resourcePath: resourcePath,
		revision:     revision,
		namespace:    ns,
		cont:         cont,
	}
}
