package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRoundTripEachStrategy(t *testing.T) {
	strategies := map[string]Strategy{
		"mutex":    Mutex,
		"sharded":  Sharded(4),
		"buffered": Buffered,
	}

	for name, strategy := range strategies {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.wal")
			store, err := New(8, WithStrategy(strategy), WithWAL(path), WithSyncPolicy(SyncAlways))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Set("a", "1"); err != nil {
				t.Fatal(err)
			}
			if err := store.Set("b", "2"); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete("a"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := New(8, WithStrategy(strategy), WithWAL(path))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, ok := reopened.Get("a"); ok {
				t.Fatal("a should have been deleted")
			}
			if value, ok := reopened.Get("b"); !ok || value != "2" {
				t.Fatalf("got (%q, %v), want (2, true)", value, ok)
			}
		})
	}
}

func TestStoreReopenAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	store, err := New(2, WithWAL(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(2, WithWAL(path))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, ok := reopened.Get("a"); !ok || value != "1" {
		t.Fatalf("got (%q, %v), want (1, true)", value, ok)
	}
}

func TestStoreReopenAfterSimulatedCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	store, err := New(2, WithWAL(path), WithSyncPolicy(SyncAlways))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("a", "1"); err != nil {
		t.Fatal(err)
	}

	// Do not call Close: append a torn record to simulate a process dying during a write.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"op":"set","key":"broken"`); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(2, WithWAL(path))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, ok := reopened.Get("a"); !ok || value != "1" {
		t.Fatalf("got (%q, %v), want (1, true)", value, ok)
	}
	if _, ok := reopened.Get("broken"); ok {
		t.Fatal("broken record should not have been replayed")
	}

	// The original store is intentionally not closed to preserve the simulated-crash scenario.
	_ = store
}

func TestStoreValidation(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("expected capacity validation error")
	}
	if _, err := New(2, WithStrategy(Sharded(0))); err == nil {
		t.Fatal("expected shard validation error")
	}
	if _, err := New(2, WithWAL("")); err == nil {
		t.Fatal("expected empty WAL path error")
	}
	if _, err := New(2, WithSyncPolicy(SyncInterval(0))); err == nil {
		t.Fatal("expected sync interval validation error")
	}
	if _, err := New(2, nil); err == nil {
		t.Fatal("expected nil option error")
	}
}
