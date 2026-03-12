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
	mu        sync.RWMutex
	data      map[string]string
	clientSeq map[string]int64 // deduplication map: clientID -> last applied seq
}

func NewKVStore() *KVStore {
	return &KVStore{
		data:      make(map[string]string),
		clientSeq: make(map[string]int64),
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

// Apply parses log commands and processes them.
// Format expects: "PUT <key> <value> <clientID> <seq>" or "DELETE <key> <clientID> <seq>"
func (kv *KVStore) Apply(command string) error {
	var op, key, value, clientID string
	var seq int64
	n, _ := fmt.Sscan(command, &op, &key, &value, &clientID, &seq)

	// Older clients/servers without idempotency (or watch events) might just send "PUT key val"
	// We'll treat them as un-deduplicated.
	if n < 4 {
		switch op {
		case "PUT":
			kv.Put(key, value)
		case "DELETE":
			kv.Delete(key)
		}
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Idempotency check: if we've already applied this seq (or higher) for this client, ignore it.
	lastSeq, exists := kv.clientSeq[clientID]
	if exists && seq <= lastSeq {
		// Duplicate detected!
		return nil
	}

	// Apply mutation
	switch op {
	case "PUT":
		kv.data[key] = value
	case "DELETE":
		delete(kv.data, key)
	default:
		return fmt.Errorf("kvstore.Apply: unknown operation %q", op)
	}

	// Update the deduplication table
	kv.clientSeq[clientID] = seq

	return nil
}

// SnapshotState is an internal envelope used by gob to serialize both maps.
type SnapshotState struct {
	Data      map[string]string
	ClientSeq map[string]int64
}

// Serialize encodes the entire KV store state into a byte slice using gob.
// The returned bytes can be passed to RaftNode.Snapshot() as the snapshot payload.
func (kv *KVStore) Serialize() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	state := SnapshotState{
		Data:      kv.data,
		ClientSeq: kv.clientSeq,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
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

	var state SnapshotState
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&state); err != nil {
		// Fallback for backwards compatibility with the old simple map snapshot format
		var oldData map[string]string
		if errOld := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&oldData); errOld == nil {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.data = oldData
			kv.clientSeq = make(map[string]int64)
			return nil
		}
		return fmt.Errorf("kvstore.LoadSnapshot: %w", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = state.Data
	if state.ClientSeq != nil {
		kv.clientSeq = state.ClientSeq
	} else {
		kv.clientSeq = make(map[string]int64)
	}
	return nil
}
