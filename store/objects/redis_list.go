package objects

import "errors"

type Chunk struct {
	elements []string
	prev     *Chunk
	next     *Chunk

	byteSize int
	size     int
	head     int
}

const CHUNK_SIZE_LIMIT = 4 * 1024
const CHUNK_LENGTH = 64

func NewChunk(prev *Chunk, next *Chunk) *Chunk {
	elements := make([]string, CHUNK_LENGTH)

	return &Chunk{
		elements: elements,
		byteSize: 0,
		size:     0,
		head:     0, // empty Chunk
		prev:     prev,
		next:     next,
	}
}

func (c *Chunk) IsEmpty() bool {
	return c.size == 0
}

func (c *Chunk) CanPush(value string) bool {
	return c.size < CHUNK_LENGTH && (c.size == 0 || c.byteSize + len(value) < CHUNK_SIZE_LIMIT)
}

func (c *Chunk) PushBack(value string) {
	if c.size == CHUNK_LENGTH {
		panic("chunk full, cannot add more")
	}

	// wrap around
	tail := (c.head + c.size) % CHUNK_LENGTH
	c.elements[tail] = value

	c.byteSize += len(value)
	c.size++
}
func (c *Chunk) PushFront(value string) {
	// move head by 1 to left and set
	if c.size == CHUNK_LENGTH {
		panic("chunk full, cannot add more")
	}

	c.head = (c.head - 1 + CHUNK_LENGTH) % CHUNK_LENGTH
	c.elements[c.head] = value

	c.byteSize += len(value)
	c.size++
}
func (c *Chunk) PopBack() (string, error) {
	if c.size == 0 {
		return "", errors.New("No element to pop")
	}

	tail := (c.head + c.size - 1) % CHUNK_LENGTH
	element := c.elements[tail]

	c.size--
	c.byteSize -= len(element)

	return element, nil
}
func (c *Chunk) PopFront() (string, error) {
	// get element, and move head by 1 to right
	if c.size == 0 {
		return "", errors.New("No element to pop")
	}

	element := c.elements[c.head]

	c.head = (c.head + 1) % CHUNK_LENGTH

	c.size--
	c.byteSize -= len(element)

	return element, nil
}

type RedisList struct {
	size int
	head *Chunk
	tail *Chunk
}

func NewList() *RedisList {
	chunk := NewChunk(nil, nil)

	return &RedisList{
		head: chunk,
		tail: chunk,
		size: 0,
	}
}

func (rl *RedisList) LPop() string {
	// pop from left/head
	head := rl.head

	el, _ := head.PopFront()

	if head.size == 0 && head.next != nil {
		rl.head = rl.head.next
		rl.head.prev = nil
	}

	rl.size--
	return el
}

func (rl *RedisList) LPush(value string) int {
	head := rl.head

	if !head.CanPush(value) {
		rl.expandHead()
	}

	// add to the head
	head.PushFront(value)
	rl.size++

	return rl.size
}

func (rl *RedisList) RPop() string {
	tail := rl.tail

	// pop the last
	el, _ := tail.PopBack()

	if tail.size == 0 && tail.prev != nil {
		rl.tail = rl.tail.prev
		rl.tail.next = nil
	}

	rl.size--
	return el
}

func (rl *RedisList) RPush(value string) int {
	tail := rl.tail

	if !tail.CanPush(value) {
		rl.expandTail()
	}

	tail.PushBack(value)
	rl.size++

	return rl.size
}

func (rl *RedisList) IsEmpty() bool {
	return rl.head == rl.tail && rl.head.IsEmpty()
}

func (rl *RedisList) GetSize() int {
	return rl.size
}

func (rl *RedisList) expandHead() {
	chunk := NewChunk(nil, rl.head)
	rl.head.prev = chunk
	rl.head = chunk
}

func (rl *RedisList) expandTail() {
	chunk := NewChunk(rl.tail, nil)
	rl.tail.next = chunk

	rl.tail = chunk
}
