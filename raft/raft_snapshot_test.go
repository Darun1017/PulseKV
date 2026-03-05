package raft

import (
	"testing"
)

func TestSnapshot(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, NewMemoryPersister())

	// Populate the log with some entries
	rn.mu.Lock()
	rn.currentTerm = 1
	rn.commitIndex = 5
	rn.log = []LogEntry{
		{Term: 0, Index: 0, Command: ""}, // Sentinel
		{Term: 1, Index: 1, Command: "A"},
		{Term: 1, Index: 2, Command: "B"},
		{Term: 1, Index: 3, Command: "C"},
		{Term: 1, Index: 4, Command: "D"},
		{Term: 1, Index: 5, Command: "E"},
		{Term: 1, Index: 6, Command: "F"}, // Uncommitted
	}
	rn.mu.Unlock()

	// Perform snapshot up to index 3
	snapshotData := []byte("mock_kv_state_up_to_index_3")
	rn.Snapshot(3, snapshotData)

	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// 1. Check Log array length and contents
	// Expected log after snap: [Dummy(Idx:3, Term:1), {Term:1, Index:4, Command:"D"}, {Term:1, Index:5, Command:"E"}, {Term:1, Index:6, Command:"F"}]
	if len(rn.log) != 4 {
		t.Fatalf("Expected log length 4, got %d", len(rn.log))
	}

	if rn.log[0].Index != 3 || rn.log[0].Term != 1 {
		t.Fatalf("Expected dummy entry at index 0 to have Index 3, Term 1. Got: %v", rn.log[0])
	}
	if rn.log[1].Command != "D" {
		t.Fatalf("Expected first real entry to be 'D', got %s", rn.log[1].Command)
	}

	// 2. Check Persister
	persistedSnap := rn.persister.ReadSnapshot()
	if string(persistedSnap) != "mock_kv_state_up_to_index_3" {
		t.Fatalf("Snapshot data was not correctly saved to persister")
	}
}

func TestSnapshot_InvalidIndices(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, NewMemoryPersister())

	rn.mu.Lock()
	rn.currentTerm = 1
	rn.commitIndex = 2
	rn.log = []LogEntry{
		{Term: 0, Index: 0, Command: ""},
		{Term: 1, Index: 1, Command: "A"},
		{Term: 1, Index: 2, Command: "B"},
		{Term: 1, Index: 3, Command: "C"}, // Uncommitted
	}
	rn.mu.Unlock()

	// Try to snapshot uncommitted index (should be ignored)
	rn.Snapshot(3, []byte("snap"))
	rn.mu.RLock()
	if len(rn.log) != 4 {
		t.Fatalf("Snapshot of uncommitted index should be ignored")
	}
	rn.mu.RUnlock()

	// Try to snapshot already snapshotted index (should be ignored)
	rn.Snapshot(-1, []byte("snap"))
	rn.mu.RLock()
	if len(rn.log) != 4 {
		t.Fatalf("Snapshot of past index should be ignored")
	}
	rn.mu.RUnlock()
}
