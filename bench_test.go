package kvstore

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const benchmarkKeyCount = 4096

func BenchmarkCaches(b *testing.B) {
	strategies := []struct {
		name string
		make func() Cache
	}{
		{name: "Mutex", make: func() Cache {
			cache, err := newMutexCache(benchmarkKeyCount)
			if err != nil {
				b.Fatal(err)
			}
			return cache
		}},
		{name: "Sharded4", make: func() Cache {
			cache, err := newShardedCache(benchmarkKeyCount, 4)
			if err != nil {
				b.Fatal(err)
			}
			return cache
		}},
		{name: "Sharded16", make: func() Cache {
			cache, err := newShardedCache(benchmarkKeyCount, 16)
			if err != nil {
				b.Fatal(err)
			}
			return cache
		}},
		{name: "Sharded64", make: func() Cache {
			cache, err := newShardedCache(benchmarkKeyCount, 64)
			if err != nil {
				b.Fatal(err)
			}
			return cache
		}},
		{name: "Buffered", make: func() Cache {
			cache, err := newBufferedCache(benchmarkKeyCount)
			if err != nil {
				b.Fatal(err)
			}
			return cache
		}},
	}

	workloads := []struct {
		name        string
		writeChance int
	}{
		{name: "Read95Write5", writeChance: 5},
		{name: "Read50Write50", writeChance: 50},
	}
	distributions := []string{"Uniform", "Zipf"}

	for _, strategy := range strategies {
		for _, workload := range workloads {
			for _, distribution := range distributions {
				name := strategy.name + "/" + workload.name + "/" + distribution
				b.Run(name, func(b *testing.B) {
					cache := strategy.make()
					prefillCache(cache)

					var seed atomic.Uint64
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						r := rand.New(rand.NewSource(int64(seed.Add(1))))
						var zipf *rand.Zipf
						if distribution == "Zipf" {
							zipf = rand.NewZipf(r, 1.2, 1, benchmarkKeyCount-1)
						}

						for pb.Next() {
							var keyIndex int
							if zipf != nil {
								keyIndex = int(zipf.Uint64())
							} else {
								keyIndex = r.Intn(benchmarkKeyCount)
							}
							key := "key-" + strconv.Itoa(keyIndex)
							if r.Intn(100) < workload.writeChance {
								cache.Set(key, "updated")
							} else {
								cache.Get(key)
							}
						}
					})
					b.StopTimer()
					if err := cache.Close(); err != nil {
						b.Fatal(err)
					}
				})
			}
		}
	}
}

func prefillCache(cache Cache) {
	for i := 0; i < benchmarkKeyCount; i++ {
		cache.Set("key-"+strconv.Itoa(i), "value")
	}
}

func BenchmarkWAL(b *testing.B) {
	policies := []struct {
		name   string
		policy SyncPolicy
	}{
		{name: "SyncAlways", policy: SyncAlways},
		{name: "SyncInterval", policy: SyncInterval(10 * time.Millisecond)},
		{name: "SyncNever", policy: SyncNever},
	}

	for _, test := range policies {
		b.Run(test.name, func(b *testing.B) {
			path := b.TempDir() + "/bench.wal"
			w, err := newWAL(path, test.policy)
			if err != nil {
				b.Fatal(err)
			}

			var sequence atomic.Uint64
			var errOnce sync.Once
			var walErr error
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := "key-" + strconv.FormatUint(sequence.Add(1), 10)
					if err := w.append(Record{Op: "set", Key: key, Value: "value"}); err != nil {
						errOnce.Do(func() { walErr = err })
						return
					}
				}
			})
			if walErr != nil {
				b.Fatal(walErr)
			}
			b.StopTimer()
			if err := w.close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
