package cleanup

import (
	"time"
)

type Store interface {
	Delete(keys []string) int
	GetExpiry(key string) (time.Time, error) 
}

var wakeUpChannel chan struct{} = make(chan struct{})

func RunCleanup(store Store) {
	pq := GetPQ()

	for {
		length := pq.Len()

		if length == 0 {
			<-wakeUpChannel
		}

		nextExpiry := pq.PeekNext()
		
		delta := time.Until(nextExpiry.at)

		if delta > 0 {
			select {
			// wake up after and delete
			case <-time.After(delta):
				;
			case <-wakeUpChannel:
				continue
			}
		}

		nextExpiry = pq.PopNext()

		storeElementExpiryAt, err := store.GetExpiry(nextExpiry.key)

		if err != nil || storeElementExpiryAt != nextExpiry.at {
			continue
		}

		// The GetExpiry call itself does the deletion but keeping it just in case
		store.Delete([]string{nextExpiry.key})
	}
}