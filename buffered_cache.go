package kvstore

import (
	"sync"
	"time"
)

// BufferedCache trades exact LRU order for less lock contention on reads.
// A read only observes the value under RLock and queues a best-effort promotion.
type BufferedCache struct {
	mu       sync.RWMutex
	cache    *lru
	promote  chan string
	done     chan struct{}
	wg       sync.WaitGroup
	closeMu  sync.Mutex
	closed   bool
	batchCap int
}

func newBufferedCache(capacity int) (*BufferedCache, error) {
	cache, err := newLRU(capacity)
	if err != nil {
		return nil, err
	}

	c := &BufferedCache{
		cache:    cache,
		promote:  make(chan string, capacity*4),
		done:     make(chan struct{}),
		batchCap: 64,
	}
	c.wg.Add(1)
	go c.promotionLoop()
	return c, nil
}

func (c *BufferedCache) Get(key string) (string, bool) {
	c.mu.RLock()
	value, ok := c.cache.peek(key)
	c.mu.RUnlock()
	if !ok {
		return "", false
	}

	select {
	case c.promote <- key:
	default:
	}
	return value, true
}

func (c *BufferedCache) Set(key, value string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.set(key, value)
}

func (c *BufferedCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.delete(key)
}

func (c *BufferedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache.len()
}

func (c *BufferedCache) promotionLoop() {
	defer c.wg.Done()

	batch := make([]string, 0, c.batchCap)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		c.mu.Lock()
		for _, key := range batch {
			// The key may have disappeared since Get, so touch is deliberately best effort.
			c.cache.touch(key)
		}
		c.mu.Unlock()
		batch = batch[:0]
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case key := <-c.promote:
			batch = append(batch, key)
			if len(batch) == c.batchCap {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-c.done:
			for {
				select {
				case key := <-c.promote:
					batch = append(batch, key)
					if len(batch) == c.batchCap {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (c *BufferedCache) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.closeMu.Unlock()
	c.wg.Wait()
	return nil
}
