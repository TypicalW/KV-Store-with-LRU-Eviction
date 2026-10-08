package kvstore

import (
	"sync"
	"testing"
	"time"
)

func TestCacheStrategiesConcurrent(t *testing.T) {
	constructors := map[string]func() Cache{
		"mutex": func() Cache {
			c, err := newMutexCache(64)
			if err != nil {
				t.Fatal(err)
			}
			return c
		},
		"sharded": func() Cache {
			c, err := newShardedCache(64, 8)
			if err != nil {
				t.Fatal(err)
			}
			return c
		},
		"buffered": func() Cache {
			c, err := newBufferedCache(64)
			if err != nil {
				t.Fatal(err)
			}
			return c
		},
	}

	for name, newCache := range constructors {
		t.Run(name, func(t *testing.T) {
			cache := newCache()
			defer cache.Close()

			var wg sync.WaitGroup
			for worker := 0; worker < 32; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < 500; i++ {
						key := "key-" + string(rune('a'+(worker+i)%32))
						switch i % 3 {
						case 0:
							cache.Set(key, "value")
						case 1:
							cache.Get(key)
						default:
							cache.Delete(key)
						}
						if cache.Len() > 64 {
							t.Errorf("Len exceeded capacity: %d", cache.Len())
							return
						}
					}
				}(worker)
			}
			wg.Wait()
			if got := cache.Len(); got > 64 {
				t.Fatalf("final Len = %d, exceeds capacity", got)
			}
		})
	}
}

func TestCacheCloseIsIdempotent(t *testing.T) {
	cache, err := newBufferedCache(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedReadKeepsKeyAlive(t *testing.T) {
	cache, err := newBufferedCache(2)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	cache.Set("hot", "value")
	cache.Set("cold", "value")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cache.Get("hot")
		time.Sleep(2 * time.Millisecond)
		cache.Set("probe", "value")
		if _, ok := cache.Get("hot"); ok {
			break
		}
		cache.Set("hot", "value")
	}
	if _, ok := cache.Get("hot"); !ok {
		t.Fatal("hot key could not be promoted before pressure")
	}

	pressureDone := make(chan struct{})
	go func() {
		defer close(pressureDone)
		time.Sleep(20 * time.Millisecond)
		for i := 0; i < 100; i++ {
			cache.Set("pressure-"+string(rune(i)), "value")
			time.Sleep(5 * time.Millisecond)
		}
	}()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cache.Get("hot"); !ok {
			t.Fatal("hot key was evicted despite repeated reads")
		}
		time.Sleep(time.Millisecond)
	}
	<-pressureDone
}
