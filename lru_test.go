package kvstore

import "testing"

func TestLRUEvictionOrder(t *testing.T) {
	c, err := newLRU(2)
	if err != nil {
		t.Fatal(err)
	}

	c.set("a", "1")
	c.set("b", "2")
	key, evicted := c.set("c", "3")
	if !evicted || key != "a" {
		t.Fatalf("got eviction (%q, %v), want (a, true)", key, evicted)
	}

	if _, ok := c.peek("a"); ok {
		t.Fatal("a should have been evicted")
	}
}

func TestLRUUpdateMovesToFront(t *testing.T) {
	c, _ := newLRU(2)
	c.set("a", "1")
	c.set("b", "2")
	c.set("a", "updated")

	key, evicted := c.set("c", "3")
	if !evicted || key != "b" {
		t.Fatalf("got eviction (%q, %v), want (b, true)", key, evicted)
	}
	if _, ok := c.peek("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if value, ok := c.peek("a"); !ok || value != "updated" {
		t.Fatalf("got (%q, %v), want (updated, true)", value, ok)
	}
}

func TestLRUGetRefreshesRecency(t *testing.T) {
	c, _ := newLRU(2)
	c.set("a", "1")
	c.set("b", "2")
	if _, ok := c.get("a"); !ok {
		t.Fatal("a should exist")
	}
	c.set("c", "3")
	if _, ok := c.peek("b"); ok {
		t.Fatal("b should have been evicted")
	}
}

func TestLRUDelete(t *testing.T) {
	c, _ := newLRU(2)
	c.set("a", "1")
	if !c.delete("a") {
		t.Fatal("delete should report true")
	}
	if c.delete("a") {
		t.Fatal("second delete should report false")
	}
	if c.len() != 0 {
		t.Fatalf("len = %d, want 0", c.len())
	}
}

func TestLRUCapacityOne(t *testing.T) {
	c, _ := newLRU(1)
	c.set("a", "1")
	key, evicted := c.set("b", "2")
	if !evicted || key != "a" {
		t.Fatalf("got eviction (%q, %v), want (a, true)", key, evicted)
	}
	if value, ok := c.peek("b"); !ok || value != "2" {
		t.Fatalf("got (%q, %v), want (2, true)", value, ok)
	}
}

func TestLRUPeekDoesNotChangeOrder(t *testing.T) {
	c, _ := newLRU(2)
	c.set("a", "1")
	c.set("b", "2")
	if _, ok := c.peek("a"); !ok {
		t.Fatal("a should exist")
	}
	c.set("c", "3")
	if _, ok := c.peek("a"); ok {
		t.Fatal("peek should not refresh a")
	}
}

func TestLRUMapAndListSizesAgree(t *testing.T) {
	c, _ := newLRU(3)
	operations := []struct {
		key    string
		value  string
		delete bool
	}{
		{key: "a", value: "1"},
		{key: "b", value: "2"},
		{key: "c", value: "3"},
		{key: "d", value: "4"},
		{key: "a", value: "5"},
		{key: "b", delete: true},
	}

	for _, op := range operations {
		if op.delete {
			c.delete(op.key)
		} else {
			c.set(op.key, op.value)
		}
		if got, want := c.len(), listLen(c); got != want {
			t.Fatalf("map/list size mismatch: map=%d list=%d", got, want)
		}
	}
}

func listLen(c *lru) int {
	count := 0
	for n := c.head.next; n != c.tail; n = n.next {
		count++
	}
	return count
}

func TestNewLRURejectsInvalidCapacity(t *testing.T) {
	if _, err := newLRU(0); err == nil {
		t.Fatal("expected an error for zero capacity")
	}
	if _, err := newLRU(-1); err == nil {
		t.Fatal("expected an error for negative capacity")
	}
}
