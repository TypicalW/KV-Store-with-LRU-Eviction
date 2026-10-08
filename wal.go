package kvstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type syncMode int

const (
	syncAlways syncMode = iota
	syncInterval
	syncNever
)

// SyncPolicy controls when WAL writes are flushed to durable storage.
type SyncPolicy struct {
	mode     syncMode
	interval time.Duration
}

var SyncAlways = SyncPolicy{mode: syncAlways}
var SyncNever = SyncPolicy{mode: syncNever}

func SyncInterval(d time.Duration) SyncPolicy {
	return SyncPolicy{mode: syncInterval, interval: d}
}

type Record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type wal struct {
	mu     sync.Mutex
	file   *os.File
	policy SyncPolicy
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool
}

func newWAL(path string, policy SyncPolicy) (*wal, error) {
	if path == "" {
		return nil, errors.New("WAL path must not be empty")
	}
	if policy.mode == syncInterval && policy.interval <= 0 {
		return nil, errors.New("WAL sync interval must be positive")
	}
	if policy.mode != syncAlways && policy.mode != syncInterval && policy.mode != syncNever {
		return nil, errors.New("invalid WAL sync policy")
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	w := &wal{
		file:   file,
		policy: policy,
	}
	if policy.mode == syncInterval {
		w.done = make(chan struct{})
		w.wg.Add(1)
		go w.syncLoop()
	}
	return w, nil
}

func (w *wal) append(record Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("WAL is closed")
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal WAL record: %w", err)
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("append WAL record: %w", err)
	}
	if w.policy.mode == syncAlways {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
	}
	return nil
}

func (w *wal) syncLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.policy.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if !w.closed {
				_ = w.file.Sync()
			}
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

func (w *wal) close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	if w.done != nil {
		close(w.done)
	}
	w.mu.Unlock()

	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.policy.mode == syncAlways || w.policy.mode == syncInterval {
		if err := w.file.Sync(); err != nil {
			_ = w.file.Close()
			return fmt.Errorf("final WAL sync: %w", err)
		}
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	return nil
}

func Replay(path string, apply func(record Record)) error {
	if apply == nil {
		return errors.New("replay callback must not be nil")
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open WAL for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var goodOffset int64
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read WAL: %w", readErr)
		}
		if len(line) == 0 || line[len(line)-1] != '\n' {
			return truncateWAL(file, goodOffset)
		}

		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			return truncateWAL(file, goodOffset)
		}
		if record.Op != "set" && record.Op != "del" {
			return truncateWAL(file, goodOffset)
		}
		if record.Key == "" {
			return truncateWAL(file, goodOffset)
		}
		if record.Op == "del" && record.Value != "" {
			return truncateWAL(file, goodOffset)
		}

		apply(record)
		goodOffset += int64(len(line))
		if errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func truncateWAL(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	return nil
}
