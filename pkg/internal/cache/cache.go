package cache

import (
	"os"
	"time"
)

const cacheBackendEnvVar = "CATTLE_STEVE_CACHE_BACKEND"

type Cache[K, V any] interface {
	Get(K) (V, bool)
	Set(K, V)
	Delete(K)
	Len() int
	Reset()
}

func NewCache[K, V any](maxSize int, ttl time.Duration) Cache[K, V] {
	switch os.Getenv(cacheBackendEnvVar) {
	case "LRU", "lru":
		return newLRUExpire[K, V](maxSize, ttl)
	default:
		return newExpiring[K, V](maxSize, ttl)
	}
}
