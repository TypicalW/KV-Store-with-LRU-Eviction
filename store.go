package kvstore

import (
	"errors"
	"fmt"
	"sync"
)

type strategyKind int

const (
	strategyMutex strategyKind = iota
	strategySharded
	strategyBuffered
)

// Strategy selects the cache concurrency strategy.
type Strategy struct {
	kind   strategyKind
	shards int
}

var Mutex = Strategy{kind: strategyMutex}

func Sharded(shards int) Strategy {
	return Strategy{kind: strategySharded, shards: shards}
}

var Buffered = Strategy{kind: strategyBuffered}

type options struct {
	strategy   Strategy
	walPath    string
	syncPolicy SyncPolicy
}

type Option func(*options) error

func WithStrategy(strategy Strategy) Option {
	return func(o *options) error {
		o.strategy = strategy
		return nil
	}
}

func WithWAL(path string) Option {
	return func(o *options) error {
		if path == "" {
			return errors.New("WAL path must not be empty")
		}
		o.walPath = path
		return nil
	}
}

func WithSyncPolicy(policy SyncPolicy) Option {
	return func(o *options) error {
		if policy.mode != syncAlways && policy.mode != syncInterval && policy.mode != syncNever {
			return errors.New("invalid WAL sync policy")
		}
		if policy.mode == syncInterval && policy.interval <= 0 {
			return errors.New("WAL sync interval must be positive")
		}
		o.syncPolicy = policy
		return nil
	}
}

type Store struct {
	cache Cache
	wal   *wal
	mu    sync.Mutex
}

func New(capacity int, opts ...Option) (*Store, error) {
	if capacity < 1 {
		return nil, errors.New("capacity must be at least 1")
	}

	cfg := options{
		strategy:   Mutex,
		syncPolicy: SyncAlways,
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("nil option")
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	cache, err := newCache(capacity, cfg.strategy)
	if err != nil {
		return nil, err
	}

	// Replay happens before opening the WAL for append so recovery never re-logs old records.
	if cfg.walPath != "" {
		if err := Replay(cfg.walPath, func(record Record) {
			if record.Op == "set" {
				cache.Set(record.Key, record.Value)
				return
			}
			cache.Delete(record.Key)
		}); err != nil {
			cache.Close()
			return nil, err
		}

		log, err := newWAL(cfg.walPath, cfg.syncPolicy)
		if err != nil {
			cache.Close()
			return nil, err
		}
		return &Store{cache: cache, wal: log}, nil
	}

	return &Store{cache: cache}, nil
}

func newCache(capacity int, strategy Strategy) (Cache, error) {
	switch strategy.kind {
	case strategyMutex:
		return newMutexCache(capacity)
	case strategySharded:
		if strategy.shards < 1 {
			return nil, errors.New("shard count must be at least 1")
		}
		return newShardedCache(capacity, strategy.shards)
	case strategyBuffered:
		return newBufferedCache(capacity)
	default:
		return nil, fmt.Errorf("unknown cache strategy")
	}
}

func (s *Store) Get(key string) (string, bool) {
	return s.cache.Get(key)
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		if err := s.wal.append(Record{Op: "set", Key: key, Value: value}); err != nil {
			return err
		}
	}
	s.cache.Set(key, value)
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		if err := s.wal.append(Record{Op: "del", Key: key}); err != nil {
			return err
		}
	}
	s.cache.Delete(key)
	return nil
}

func (s *Store) Len() int {
	return s.cache.Len()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var walErr error
	if s.wal != nil {
		walErr = s.wal.close()
		s.wal = nil
	}
	cacheErr := s.cache.Close()
	if walErr != nil {
		return walErr
	}
	return cacheErr
}
