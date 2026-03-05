package raft

import (
	"os"
	"testing"
)

func TestPersister(t *testing.T) {
	filename := "test_raft_state.json"
	defer os.Remove(filename)
	defer os.Remove(filename + ".tmp")

	persister := NewFilePersister(filename)

	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, persister)

	// Simulate some state changes
	rn.mu.Lock()
	rn.currentTerm = 10
	rn.votedFor = 2
	rn.log = append(rn.log, LogEntry{Term: 1, Index: 1, Command: "SET A 1"})
	rn.log = append(rn.log, LogEntry{Term: 2, Index: 2, Command: "SET B 2"})
	rn.persist()
	rn.mu.Unlock()

	// Stop node
	rn.Stop()

	// Create a new node with the same persister file, simulating a crash recovery
	persister2 := NewFilePersister(filename)
	rn2 := NewRaftNode(1, []int{2, 3}, applyCh, persister2)

	rn2.mu.RLock()
	defer rn2.mu.RUnlock()

	if rn2.currentTerm != 10 {
		t.Fatalf("Expected term 10 recovered, got %d", rn2.currentTerm)
	}

	if rn2.votedFor != 2 {
		t.Fatalf("Expected votedFor 2 recovered, got %d", rn2.votedFor)
	}

	if len(rn2.log) != 3 { // dummy + 2 entries
		t.Fatalf("Expected log length 3 recovered, got %d", len(rn2.log))
	}

	if rn2.log[2].Command != "SET B 2" {
		t.Fatalf("Expected log[2] to be 'SET B 2', got %s", rn2.log[2].Command)
	}
}

func TestAppendEntriesRPC(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	persister := NewMemoryPersister()
	rn := NewRaftNode(1, []int{2, 3}, applyCh, persister)

	// Node starts at term 0, Follower
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Index: 1, Command: "CMD_1"}},
		LeaderCommit: 1,
	}

	reply := &AppendEntriesReply{}
	rn.AppendEntries(args, reply)

	if !reply.Success {
		t.Fatalf("AppendEntries should have succeeded")
	}

	rn.mu.RLock()
	if rn.currentTerm != 1 {
		t.Fatalf("Node should have advanced to term 1, got %d", rn.currentTerm)
	}
	if len(rn.log) != 2 {
		t.Fatalf("Node log should have length 2 (1 dummy + 1 actual), got %d", len(rn.log))
	}
	rn.mu.RUnlock()
}
