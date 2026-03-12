package kvstore

import (
	"bytes"
	"encoding/gob"
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

func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	result := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		result[k] = v
	}
	return result
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

// Serialize encodes the entire KV store state into a byte slice using gob.
// The returned bytes can be passed to RaftNode.Snapshot() as the snapshot payload.
func (kv *KVStore) Serialize() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv.data); err != nil {
		return nil, fmt.Errorf("kvstore.Serialize: %w", err)
	}
	return buf.Bytes(), nil
}

// LoadSnapshot replaces the entire KV store state with the data encoded in snapshot.
// It is the inverse of Serialize and must be called before any entries are applied.
func (kv *KVStore) LoadSnapshot(snapshot []byte) error {
	if len(snapshot) == 0 {
		return nil
	}

	var data map[string]string
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&data); err != nil {
		return fmt.Errorf("kvstore.LoadSnapshot: %w", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = data
	return nil
}
