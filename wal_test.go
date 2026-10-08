package kvstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALWriteReopenReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := newWAL(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(Record{Op: "set", Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	var records []Record
	if err := Replay(path, func(record Record) { records = append(records, record) }); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Key != "a" || records[0].Value != "1" {
		t.Fatalf("unexpected replay: %#v", records)
	}
}

func TestWALDeleteReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := newWAL(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []Record{
		{Op: "set", Key: "a", Value: "1"},
		{Op: "del", Key: "a"},
	} {
		if err := w.append(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	var records []Record
	if err := Replay(path, func(record Record) { records = append(records, record) }); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[1].Op != "del" {
		t.Fatalf("unexpected replay: %#v", records)
	}
}

func TestWALEvictionsReplayDeterministically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := newWAL(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []Record{
		{Op: "set", Key: "a", Value: "1"},
		{Op: "set", Key: "b", Value: "2"},
		{Op: "set", Key: "c", Value: "3"},
	} {
		if err := w.append(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	cache, _ := newLRU(2)
	if err := Replay(path, func(record Record) {
		if record.Op == "set" {
			cache.set(record.Key, record.Value)
		} else {
			cache.delete(record.Key)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.peek("a"); ok {
		t.Fatal("a should have been evicted")
	}
	if _, ok := cache.peek("b"); !ok {
		t.Fatal("b should be present")
	}
	if _, ok := cache.peek("c"); !ok {
		t.Fatal("c should be present")
	}
}

func TestWALTruncatesHalfWrittenLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	good := []byte(`{"op":"set","key":"a","value":"1"}` + "\n")
	bad := []byte(`{"op":"set","key":"b","value":"2"`)
	if err := os.WriteFile(path, append(good, bad...), 0o644); err != nil {
		t.Fatal(err)
	}

	var records []Record
	if err := Replay(path, func(record Record) { records = append(records, record) }); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Key != "a" {
		t.Fatalf("unexpected replay: %#v", records)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(good) {
		t.Fatalf("WAL was not truncated to last good record: %q", data)
	}

	w, err := newWAL(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(Record{Op: "set", Key: "c", Value: "3"}); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALMalformedLineTruncates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	data := []byte("{\"op\":\"set\",\"key\":\"a\",\"value\":\"1\"}\nnot-json\n")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	var records []Record
	if err := Replay(path, func(record Record) { records = append(records, record) }); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := data[:len(data)-len("not-json\n")]
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWALMissingOrEmptyFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.wal")
	if err := Replay(missing, func(Record) {}); err != nil {
		t.Fatal(err)
	}

	empty := filepath.Join(t.TempDir(), "empty.wal")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Replay(empty, func(Record) {}); err != nil {
		t.Fatal(err)
	}
}

func TestWALCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := newWAL(path, SyncInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if err := w.append(Record{Op: "set", Key: "a", Value: "1"}); err == nil {
		t.Fatal("append after close should fail")
	}
}

func TestNewWALRejectsInvalidInterval(t *testing.T) {
	if _, err := newWAL(filepath.Join(t.TempDir(), "store.wal"), SyncInterval(0)); err == nil {
		t.Fatal("expected invalid interval error")
	}
}
