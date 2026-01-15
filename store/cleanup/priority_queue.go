package cleanup

import (
	"container/heap"
	"sync"
	"time"
)

type Expiry struct {
	key string
	at  time.Time
}

type ExpiryPQ struct {
	mu    sync.Mutex
	items []*Expiry
}

var (
	instance *ExpiryPQ
	once sync.Once
)

func GetPQ() *ExpiryPQ {
	once.Do(func() {
		pq := &ExpiryPQ{items: []*Expiry{}}
		heap.Init(pq)
		instance = pq
	})

	return instance
}

func (pq *ExpiryPQ) PeekNext() *Expiry {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil
	}
	return pq.items[0]
}

func (pq *ExpiryPQ) PopNext() *Expiry {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil
	}
	return heap.Pop(pq).(*Expiry)
}

func (pq *ExpiryPQ) HPush(key string, expireAt time.Time) {
	item := &Expiry{
		key: key,
		at: expireAt,
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()

	var oldTop *Expiry
	if pq.Len() > 0 {
		oldTop = pq.items[0]
	}

	heap.Push(pq, item)

	// newly pushed item has an expiry which precedes the current top
	if oldTop == nil || item.at.Before(oldTop.at) {
		wakeUpChannel <- struct{}{}
	}
}

// ——————————————————————————————————————————————————————————————————
// Interface Methods
// ——————————————————————————————————————————————————————————————————

func (pq *ExpiryPQ) Len() int {
	return len(pq.items)
}

func (pq *ExpiryPQ) Less(i, j int) bool {
	return pq.items[i].at.Before(pq.items[j].at)
}

func (pq *ExpiryPQ) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *ExpiryPQ) Push(x any) {
	pq.items = append(pq.items, x.(*Expiry))
}

func (pq *ExpiryPQ) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[:n-1]
	return item
}
