package kvstore

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	kv := NewKVStore()

	kv.Put("name", "PulseKV")
	val, ok := kv.Get("name")
	if !ok || val != "PulseKV" {
		t.Fatalf("expected (PulseKV, true), got (%s, %v)", val, ok)
	}
}

func TestGetMissingKey(t *testing.T) {
	kv := NewKVStore()

	val, ok := kv.Get("nonexistent")
	if ok || val != "" {
		t.Fatalf("expected ('', false), got (%s, %v)", val, ok)
	}
}

func TestPutOverwrite(t *testing.T) {
	kv := NewKVStore()

	kv.Put("key", "v1")
	kv.Put("key", "v2")
	val, _ := kv.Get("key")
	if val != "v2" {
		t.Fatalf("expected v2, got %s", val)
	}
}

func TestDelete(t *testing.T) {
	kv := NewKVStore()

	kv.Put("key", "value")
	kv.Delete("key")
	_, ok := kv.Get("key")
	if ok {
		t.Fatal("key should not exist after delete")
	}
}

func TestDeleteNonexistent(t *testing.T) {
	// Should not panic
	kv := NewKVStore()
	kv.Delete("ghost")
}

func TestApplyPut(t *testing.T) {
	kv := NewKVStore()

	err := kv.Apply("PUT city Bangalore")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, ok := kv.Get("city")
	if !ok || val != "Bangalore" {
		t.Fatalf("expected (Bangalore, true), got (%s, %v)", val, ok)
	}
}

func TestApplyDelete(t *testing.T) {
	kv := NewKVStore()

	kv.Put("city", "Bangalore")
	err := kv.Apply("DELETE city")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, ok := kv.Get("city")
	if ok {
		t.Fatal("key should have been deleted")
	}
}

func TestApplyInvalidCommand(t *testing.T) {
	kv := NewKVStore()

	err := kv.Apply("INVALID foo bar")
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestApplyMalformedPut(t *testing.T) {
	kv := NewKVStore()

	err := kv.Apply("PUT onlykey")
	if err == nil {
		t.Fatal("expected error for PUT without value")
	}
}

// TestConcurrentReadWrite verifies that the RWMutex prevents data races.
// Run with: go test -race ./kvstore/
func TestConcurrentReadWrite(t *testing.T) {
	kv := NewKVStore()
	var wg sync.WaitGroup

	// 50 writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kv.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		}(i)
	}

	// 50 readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kv.Get(fmt.Sprintf("k%d", i))
		}(i)
	}

	// 20 deleters
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kv.Delete(fmt.Sprintf("k%d", i))
		}(i)
	}

	wg.Wait()
}
