package kvstore

import (
	"fmt"
	"hash/fnv"
)

type ShardedCache struct {
	shards []*MutexCache
}

func newShardedCache(capacity, shardCount int) (*ShardedCache, error) {
	if shardCount < 1 {
		return nil, fmt.Errorf("shard count must be at least 1")
	}
	if capacity < 1 {
		return nil, fmt.Errorf("capacity must be at least 1")
	}
	if shardCount > capacity {
		shardCount = capacity
	}

	shards := make([]*MutexCache, shardCount)
	base := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		shardCapacity := base
		if i < extra {
			shardCapacity++
		}
		shard, err := newMutexCache(shardCapacity)
		if err != nil {
			return nil, err
		}
		shards[i] = shard
	}

	return &ShardedCache{shards: shards}, nil
}

func (c *ShardedCache) shard(key string) *MutexCache {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return c.shards[uint64(h.Sum32())%uint64(len(c.shards))]
}

func (c *ShardedCache) Get(key string) (string, bool) {
	return c.shard(key).Get(key)
}

func (c *ShardedCache) Set(key, value string) (string, bool) {
	return c.shard(key).Set(key, value)
}

func (c *ShardedCache) Delete(key string) bool {
	return c.shard(key).Delete(key)
}

func (c *ShardedCache) Len() int {
	total := 0
	for _, shard := range c.shards {
		total += shard.Len()
	}
	return total
}

func (c *ShardedCache) Close() error {
	return nil
}
