package kvstore

import (
	"fmt"
	"sync"
)

// KVStore holds the in-memory key-value data protected by an RWMutex.
// DEADLOCK NOTE: Never call one public method from inside another to avoid re-entrant deadlocks.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, exists := kv.data[key]
	return value, exists
}

func (kv *KVStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[key] = value

	// TODO (teammate – disk I/O): update dirty-flag or snapshot counter here
}

func (kv *KVStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.data, key)

	// TODO (teammate – disk I/O): update dirty-flag or snapshot counter here
}

func (kv *KVStore) Apply(command string) error {
	var op, key, value string
	n, _ := fmt.Sscan(command, &op, &key, &value)

	switch op {
	case "PUT":
		if n < 3 {
			return fmt.Errorf("kvstore.Apply: PUT requires key and value")
		}
		kv.Put(key, value)
	case "DELETE":
		if n < 2 {
			return fmt.Errorf("kvstore.Apply: DELETE requires a key")
		}
		kv.Delete(key)
	default:
		return fmt.Errorf("kvstore.Apply: unknown operation")
	}
	return nil
}
