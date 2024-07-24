package cache

import (
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/cache"
)

func newExpiring[K, V any](maxSize int, ttl time.Duration) Cache[K, V] {
	return &wrapExpiring[K, V]{maxSize: maxSize, ttl: ttl, cache: cache.NewExpiring()}
}

// wrapExpiring imposes some static type restrictions around a generic cache.Expiring cache
type wrapExpiring[K, V any] struct {
	maxSize int
	ttl     time.Duration
	cache   *cache.Expiring
}

func (w wrapExpiring[K, V]) Get(k K) (V, bool) {
	if v, ok := w.cache.Get(k); ok {
		return v.(V), true
	}
	// zero value of V
	return *new(V), false
}

func (w wrapExpiring[K, V]) Set(k K, v V) {
	w.cache.Set(k, v, w.ttl)
	if current, max := w.cache.Len(), w.maxSize; current >= max {
		logrus.WithField("max", max).WithField("current", current).Warnf("cache reached soft limit")
	}
}

func (w wrapExpiring[K, V]) Delete(k K) {
	w.cache.Delete(k)
}

func (w wrapExpiring[K, V]) Len() int {
	return w.cache.Len()
}

func (w wrapExpiring[K, V]) Reset() {
	w.cache = cache.NewExpiring()
}
