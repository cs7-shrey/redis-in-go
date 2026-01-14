package store

import (
	"errors"
	"server/errs"
	"server/store/actions"
	"server/store/objects"
	"sync"
)

type Store struct {
	mu    sync.RWMutex
	kvMap map[string]*objects.Object
}

func NewStore() *Store {
	return &Store{
		kvMap: make(map[string]*objects.Object),
	}
}

func (store *Store) validateActionForDataType(object *objects.Object, action actions.Action) (*objects.Object, error) {
	// universal actions
	if action == actions.Del || action == actions.Exists{
		return object, nil
	}

	switch object.DataType {
	case objects.String:
		switch action {
		case actions.Get, actions.Set:
			return object, nil
		default:
			return nil, errs.InvalidMethod
		}

	case objects.List:
		switch action {
		case actions.LPush, actions.LPop, actions.RPush, actions.RPop:
			return object, nil
		default:
			return nil, errs.InvalidMethod
		}
	case objects.Hash:
		return nil, errs.InvalidMethod
	case objects.Set:
		return nil, errs.InvalidMethod
	default:
		return nil, errs.InvalidDataType
	}
}

func (store *Store) Get(key string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	value, ok := store.kvMap[key]
	if !ok {
		return "", errs.ErrNotFound
	}

	value, err := store.validateActionForDataType(value, actions.Get)
	if err != nil {
		return "", err
	}

	return value.Data.(string), nil
}

func (store *Store) Set(key string, value string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.kvMap[key] = objects.NewObject(objects.String, value)
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

type listPushFn func(list *objects.RedisList, items []string) int
type listPopFn func(list *objects.RedisList, count int) []string

func (store *Store) push(key string, items []string, pushFn listPushFn) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.kvMap[key]

	// make a new list
	if !exists {
		redisList := objects.NewList()
		store.kvMap[key] = objects.NewObject(objects.List, redisList)		// place reference
		return pushFn(redisList, items), nil
	}

	object, err := store.validateActionForDataType(object, actions.LPush)		// same for LPush or RPush (use either)

	if err != nil {
		return 0, err
	}

	if redisList, ok := object.Data.(*objects.RedisList); !ok {
		return 0, errors.New("TYPE MISMATCH")
	} else {
		return pushFn(redisList, items), nil
	}
}

func (store *Store) pop(key string, count int, popFn listPopFn) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	
	object, exists := store.kvMap[key]

	if !exists {
		return nil, errs.ErrNotFound
	}

	object, err := store.validateActionForDataType(object, actions.LPop)

	if err != nil {
		return nil, err
	}

	if redisList, ok := object.Data.(*objects.RedisList); !ok {
		return nil, errors.New("TYPE MISMATCH")
	} else {
		// unlikely since we delete an empty list
		if redisList.GetSize() == 0 {
			return nil, errors.New("CANNOT POP AN EMPTY LIST")
		}

		items := popFn(redisList, count)

		if redisList.IsEmpty() {
			delete(store.kvMap, key)
		}
		
		return items, nil
	}
}

func (store *Store) LPush(key string, items []string) (int, error) {
	return store.push(key, items, func(list *objects.RedisList, items []string) int {
		for _, item := range items {
			list.LPush(item)
		}
		return list.GetSize()
	})
}

func (store *Store) RPush(key string, items []string) (int, error) {
	return store.push(key, items, func(list *objects.RedisList, items []string) int {
		for _, item := range items {
			list.RPush(item)
		}
		return list.GetSize()
	})
}

func (store *Store) LPop(key string, count int) ([]string, error) {
	return store.pop(key, count, func(list *objects.RedisList, count int) []string {
		popCount := min(count, list.GetSize())
		items := make([]string, min(count, popCount))

		for i := range popCount {
			item := list.LPop()
			items[i] = item
		}

		return items
	})
}

func (store *Store) RPop(key string, count int) ([]string, error) {
	return store.pop(key, count, func(list *objects.RedisList, count int) []string {
		popCount := min(count, list.GetSize())
		items := make([]string, min(count, popCount))

		for i := range popCount {
			item := list.RPop()
			items[i] = item
		}

		return items
	})
}