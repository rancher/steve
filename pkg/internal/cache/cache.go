package cache

import "time"

type Cache[K, V any] interface {
	Get(K) (V, bool)
	Set(K, V)
	Delete(K)
	Len() int
	Reset()
}

func NewCache[K, V any](maxSize int, ttl time.Duration) Cache[K, V] {
	return newLRUExpire[K, V](maxSize, ttl)
}
