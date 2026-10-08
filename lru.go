package kvstore

import "fmt"

type node struct {
	key   string
	value string
	prev  *node
	next  *node
}

type lru struct {
	capacity int
	items    map[string]*node
	head     *node
	tail     *node
}

func newLRU(capacity int) (*lru, error) {

	if capacity < 1 {
		return nil, fmt.Errorf("capacity must be atleast 1")
	}

	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &lru{
		capacity: capacity,
		items:    make(map[string]*node),
		head:     head,
		tail:     tail,
	}, nil
}

// removes a node by re-assigning node pointers
func (c *lru) remove(n *node) {
	n.prev.next = n.next // assigns the next pointer of the previous node of n to the next node of n
	n.next.prev = n.prev // assingns the previous pointer by assigning the previous pointer of the next node to the previous node
}

// adds a node to the front of the linked list
func (c *lru) addToFront(n *node) {
	n.next = c.head.next
	n.prev = c.head
	c.head.next.prev = n
	c.head.next = n
}

// instead of reassigning each node on moving to front, we remove and add to front
func (c *lru) moveToFront(n *node) {
	c.remove(n)
	c.addToFront(n)
}

func (c *lru) removeLast() *node {
	//check for no nodes in between
	if c.tail.prev == c.head {
		return nil
	}

	n := c.tail.prev
	c.remove(n)
	return n
}

//lru functions

func (c *lru) get(key string) (string, bool) {
	n, ok := c.items[key]
	if !ok {
		return "", false
	}
	c.moveToFront(n)
	return n.value, true

}

func (c *lru) peek(key string) (string, bool) {
	n, ok := c.items[key]
	if !ok {
		return "", false
	}
	return n.value, true

}

func (c *lru) touch(key string) bool {
	n, ok := c.items[key]
	if !ok {
		return false
	}
	c.moveToFront(n)
	return true
}

func (c *lru) set(key, value string) (evictedKey string, evicted bool) {
	if n, ok := c.items[key]; ok {
		n.value = value
		c.moveToFront(n)
		return "", false
	}

	n := &node{key: key, value: value}
	c.items[key] = n
	c.addToFront(n)

	if len(c.items) <= c.capacity {
		return "", false
	}

	victim := c.removeLast()
	delete(c.items, victim.key)
	return victim.key, true
}

func (c *lru) delete(key string) bool {
	n, ok := c.items[key]
	if !ok {
		return false
	}
	c.remove(n)
	delete(c.items, key)
	return true
}

func (c *lru) len() int {
	return len(c.items)
}
