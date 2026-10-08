package kvstore

import "sync"

// Cache is the concurrency-safe interface used by the store.
type Cache interface {
	Get(key string) (string, bool)
	Set(key, value string) (evictedKey string, evicted bool)
	Delete(key string) bool
	Len() int
	Close() error
}

// MutexCache is the simple baseline: one mutex protects the whole LRU.
type MutexCache struct {
	mu    sync.Mutex
	cache *lru
}

func newMutexCache(capacity int) (*MutexCache, error) {
	cache, err := newLRU(capacity)
	if err != nil {
		return nil, err
	}
	return &MutexCache{cache: cache}, nil
}

// Get takes the mutex because a cache hit changes LRU order.
// An RWMutex would be wrong here: Get mutates the list, so it is a write.
func (c *MutexCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.get(key)
}

func (c *MutexCache) Set(key, value string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.set(key, value)
}

func (c *MutexCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.delete(key)
}

func (c *MutexCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.len()
}

func (c *MutexCache) Close() error {
	return nil
}
