package cache

import (
	"time"

	"k8s.io/apimachinery/pkg/util/cache"
)

func newLRUExpire[K, V any](maxSize int, ttl time.Duration) Cache[K, V] {
	return &wrapLRUExpire[K, V]{maxSize: maxSize, ttl: ttl, lru: cache.NewLRUExpireCache(maxSize)}
}

// wrapLRUExpire imposes some static type restrictions around a generic cache.LRUExpireCache
type wrapLRUExpire[K, V any] struct {
	maxSize int
	ttl     time.Duration
	lru     *cache.LRUExpireCache
}

func (w wrapLRUExpire[K, V]) Get(k K) (V, bool) {
	if v, ok := w.lru.Get(k); ok {
		return v.(V), true
	}
	// zero value of V
	return *new(V), false
}

func (w wrapLRUExpire[K, V]) Set(k K, v V) {
	w.lru.Add(k, v, w.ttl)
}

func (w wrapLRUExpire[K, V]) Delete(k K) {
	w.lru.Remove(k)
}

func (w wrapLRUExpire[K, V]) Len() int {
	return len(w.lru.Keys())
}

func (w wrapLRUExpire[K, V]) Reset() {
	w.lru = cache.NewLRUExpireCache(w.maxSize)
}
