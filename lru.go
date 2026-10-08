package kvstore

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

func newLRU(capacity int) *lru {
	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &lru{
		capacity: capacity,
		items:    make(map[string]*node),
		head:     head,
		tail:     tail,
	}
}
