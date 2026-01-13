package store

import (
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
	case "string":
		switch action {
		case actions.Get, actions.Set:
			return object, nil
		default:
			return nil, errs.InvalidMethod
		}

	case "list":
		switch action {
		case actions.LPush, actions.LPop, actions.RPush, actions.RPop:
			return object, nil
		default:
			return nil, errs.InvalidMethod
		}
	case "hash":
		return nil, errs.InvalidMethod
	case "set":
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

// func (store *Store)
