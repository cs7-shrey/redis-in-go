package store

import (
	"server/errs"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	kvMap map[string] string
}

func NewStore() *Store {
	return &Store{
		kvMap: make(map[string]string),
	}
}

func (store *Store) Get(key string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	value, ok := store.kvMap[key];
	if !ok {
		return "", errs.ErrNotFound
	}
	return value, nil
}

func (store *Store) Set(key string, value string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.kvMap[key] = value
}

func (store *Store) Delete(keys []string) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	count := 0

	for _, key := range keys {
		_, exists := store.kvMap[key]
		if exists {
			count++
			delete(store.kvMap, key)
		}
	}

	return count
}

func (store *Store) Exists(keys []string) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	count := 0

	for _, key := range keys {
		_, exists := store.kvMap[key]
		if exists {
			count += 1
		}
	}
	return count
}