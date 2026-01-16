package store

import (
	"errors"
	"server/errs"
	"server/store/actions"
	"server/store/cleanup"
	"server/store/objects"
	"sync"
	"time"
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
	if action == actions.Del || action == actions.Exists || action == actions.Expire || action == actions.TTL || action == actions.Set {
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
		switch action {
		case actions.HGet, actions.HSet, actions.HGetAll, actions.HDel:
			return object, nil
		default:
			return nil, errs.InvalidMethod
		}
	case objects.Set:
		return nil, errs.InvalidMethod
	default:
		return nil, errs.InvalidDataType
	}
}

func (store *Store) getObject(key string) (*objects.Object, bool) {
	// must be used with a lock
	object, ok := store.kvMap[key]
	if !ok {
		return nil, false
	}

	if object.HasExpired() {
		delete(store.kvMap, key)
		return nil, false
	}

	return object, true
}

func (store *Store) Get(key string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	value, ok := store.getObject(key)
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
		_, exists := store.getObject(key)
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
		_, exists := store.getObject(key)
		if exists {
			count += 1
		}
	}
	return count
}

func (store *Store) Expire(key string, seconds int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)
	if !exists {
		return errs.ErrNotFound
	}

	object.Expire(seconds)
	pq := cleanup.GetPQ()

	pq.HPush(key, object.GetExpiry().At)

	return nil
}

func (store *Store) GetExpiry(key string) (time.Time, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)
	if !exists {
		return time.Time{}, errs.ErrNotFound
	}

	expiry := object.GetExpiry()
	if expiry == nil {
		return time.Time{}, errors.New("No expiry on element")
	}

	return expiry.At, nil
}

func (store *Store) TTL(key string) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)
	
	if !exists {
		return -2
	}

	return object.TTL()
}

type listPushFn func(list *objects.RedisList, items []string) []objects.BlockingPopDisperal
type listPopFn func(list *objects.RedisList, count int) []string

func (store *Store) push(key string, items []string, pushFn listPushFn) ([]objects.BlockingPopDisperal, int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)

	// make a new list
	if !exists {
		redisList := objects.NewList()
		store.kvMap[key] = objects.NewObject(objects.List, redisList)		// place reference
		result := pushFn(redisList, items)
		return result, redisList.GetSize(), nil
	}

	object, err := store.validateActionForDataType(object, actions.LPush)		// same for LPush or RPush (use either)

	if err != nil {
		store.mu.Unlock()
		return nil, 0, err
	}

	redisList, ok := object.Data.(*objects.RedisList);

	if !ok {
		store.mu.Unlock()
		return nil, 0, errors.New("TYPE MISMATCH")
	} 

	result := pushFn(redisList, items)
	return result, redisList.GetSize(), nil
}

func (store *Store) pushWithDispersal(key string, items []string, pushFn listPushFn) (int, error) {
	dispersals, newSize, err := store.push(key, items, pushFn)
	if err != nil {
		return 0, err
	}

	for _, dispersal := range dispersals {
		dispersal.Channel <- dispersal.Value
	}

	return newSize, nil
}

func (store *Store) pop(key string, count int, popFn listPopFn) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	
	object, exists := store.getObject(key)

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
	return store.pushWithDispersal(key, items, func(list *objects.RedisList, items []string) []objects.BlockingPopDisperal {
		return list.LPush(items)
	})
}

func (store *Store) RPush(key string, items []string) (int, error) {
	return store.pushWithDispersal(key, items, func(list *objects.RedisList, items []string) []objects.BlockingPopDisperal {
		return list.RPush(items)
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

func (store *Store) blockingPop(key string, direction actions.BlockingPopDirection) (string, error) {
	if direction != actions.BLEFT && direction != actions.BRIGHT {
		return "", errors.New("INVALID POP")
	}

	var redisList *objects.RedisList = nil

	store.mu.Lock()
	object, exists := store.getObject(key)

	if !exists {
		// acquiring lock for writing

		object := objects.NewList()
		store.kvMap[key] = objects.NewObject(objects.List, object)		// place reference
		redisList = object

		store.mu.Unlock()
	} else {
		store.mu.Unlock()
		object, err := store.validateActionForDataType(object, actions.LPop)
		if err != nil {
			return "", err
		}

		if redisListFromData, ok := object.Data.(*objects.RedisList); !ok {
			return "", errors.New("SERVER TYPE MISMATCH")
		} else {
			redisList = redisListFromData
		}
	}

	// acquiring lock when touching list
	store.mu.Lock()
	isEmpty := redisList.IsEmpty()
	if !isEmpty {
		if direction == "right"	{
			item := redisList.LPop() 
			store.mu.Unlock()
			return item , nil
		} else {
			item := redisList.RPop()
			store.mu.Unlock()
			return item, nil
		}
	} else {
		// redis list is empty
		// make a channel, and add a blocking client			

		channel := make(chan string)
		redisList.AddBlockingPopClient(channel, direction)
		store.mu.Unlock()		// releasing as we now let the redis list handling blocking pop upon a push

		item := <-channel

		return item, nil
	}
}

func (store *Store) BLPop(key string) (string, error) {
	return store.blockingPop(key, actions.BLEFT)
}

func (store *Store) BRPop(key string) (string, error) {
	return store.blockingPop(key, actions.BRIGHT)
}

// ———————————————————————————————————————————————————————————————
// Hash set methods
// ———————————————————————————————————————————————————————————————

func (store *Store) HGet(key, field string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	object, exists := store.getObject(key)
	if !exists {
		return "", errs.ErrNotFound
	}

	object, err := store.validateActionForDataType(object, actions.HGet)

	if err != nil {
		return "", err
	}

	hs, err := objects.ValidateObjectAsHash(object)
	if err != nil {
		return "", err
	}

	value, exists := hs.Get(field)

	if !exists {
		return "", errs.ErrNotFound
	}

	return value, nil
}

func (store *Store) HGetAll(key string) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)

	if !exists {
		return nil, errs.ErrNotFound
	}

	object, err := store.validateActionForDataType(object, actions.HGet)

	if err != nil {
		return nil, err
	}

	hs, err := objects.ValidateObjectAsHash(object)

	if err != nil {
		return nil, err
	}

	response := make([]string, 0, 2 * len(hs))

	for k, v := range hs {
		response = append(response, k)
		response = append(response, v)
	}

	return response, nil
}

func (store *Store) HSet(key string, fieldValueArray []string) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if len(fieldValueArray) & 2 == 1 {
		return 0, errs.IncorrectNumberOfArguments
	}

	object, exists := store.getObject(key)
	
	if !exists {
		object = objects.NewObject(objects.Hash, objects.NewHashSet())
		store.kvMap[key] = object
	}

	object, err := store.validateActionForDataType(object, actions.HGet)

	if err != nil {
		return 0, err
	}

	hs, err := objects.ValidateObjectAsHash(object)

	if err != nil {
		return 0, err
	}

	count := 0
	for i := 0; i < len(fieldValueArray) - 1; i += 2 {
		// check if exists
		field, value := fieldValueArray[i], fieldValueArray[i+1]

		if !hs.Exists(field) {
			count++
		}

		hs.Set(field, value)
	}

	return count, nil
}

func (store *Store) HDel(key string, fields []string) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	object, exists := store.getObject(key)

	if !exists {
		return 0, errs.ErrNotFound
	}

	object, err := store.validateActionForDataType(object, actions.HGet)

	if err != nil {
		return 0, err
	}

	hs, err := objects.ValidateObjectAsHash(object)

	if err != nil {
		return 0, err
	}

	return hs.Delete(fields), nil
}