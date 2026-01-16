package objects

import "server/errs"

type HashSet map[string]string

func NewHashSet() HashSet {
	return make(HashSet)
}

func (hs HashSet) Set(key, value string) {
	hs[key] = value
}

func (hs HashSet) Get(key string) (string, bool) {
	value, exists := hs[key]
	return value, exists
}

func (hs HashSet) Exists(key string) bool {
	_, exists := hs[key]
	return exists
}

func (hs HashSet) Delete(keys []string) int {
	count := 0
	for _, key := range keys {
		_, exists := hs.Get(key)
		if exists {
			count++
		}
		delete(hs, key)
	}

	return count
}

func ValidateObjectAsHash(object *Object) (HashSet, error) {
	hs, ok := object.Data.(HashSet)
	if !ok {
		return nil, errs.TypeMismatch
	}

	return hs, nil
}