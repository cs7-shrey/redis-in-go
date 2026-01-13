package store

import "server/errs"

type Store struct {
	kvMap map[string] string
}

func NewStore() *Store {
	return &Store{
		kvMap: make(map[string]string),
	}
}

func (store *Store) Get(key string) (string, error) {
	value, ok := store.kvMap[key];
	if !ok {
		return "", errs.ErrNotFound
	}
	return value, nil
}

func (store *Store) Set(key string, value string) {
	store.kvMap[key] = value
}

func (store *Store) Delete(keys []string) int {
	count := store.Exists(keys)
	for _, key := range keys {
		delete(store.kvMap, key)
	}

	return count
}

func (store *Store) Exists(keys []string) int {
	count := 0

	for _, key := range keys {
		_, exists := store.kvMap[key]
		if exists {
			count += 1
		}
	}
	return count
}