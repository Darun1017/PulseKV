package raft

import (
	"testing"
)

func TestRequestVote_TermCheck(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, NewMemoryPersister())
	rn.currentTerm = 2

	// Scenario 1: Older term
	argsOlder := &RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
	replyOlder := &RequestVoteReply{}
	rn.RequestVote(argsOlder, replyOlder)

	if replyOlder.VoteGranted {
		t.Errorf("Should reject vote for older term")
	}

	// Scenario 2: Newer term
	argsNewer := &RequestVoteArgs{Term: 3, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
	replyNewer := &RequestVoteReply{}
	rn.RequestVote(argsNewer, replyNewer)

	if !replyNewer.VoteGranted {
		t.Errorf("Should grant vote for newer term if log is up-to-date")
	}
	if rn.currentTerm != 3 || rn.state != Follower {
		t.Errorf("Node should advanced term and become follower")
	}
}

func TestRequestVote_LogUpToDateCheck(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, NewMemoryPersister())

	// Set up our node's log
	rn.mu.Lock()
	rn.currentTerm = 1
	rn.log = []LogEntry{
		{Term: 0, Index: 0, Command: ""}, // Sentinel
		{Term: 1, Index: 1, Command: "CMD_1"},
		{Term: 1, Index: 2, Command: "CMD_2"},
	}
	rn.mu.Unlock()

	// Scenario 1: Candidate log is shorter and term is same -> Reject
	argsShort := &RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 1, LastLogTerm: 1}
	replyShort := &RequestVoteReply{}
	rn.RequestVote(argsShort, replyShort)
	if replyShort.VoteGranted {
		t.Errorf("Should reject vote if candidate log is not up-to-date (shorter index)")
	}

	// Scenario 2: Candidate log same length but term is older -> Reject
	argsOldTerm := &RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 2, LastLogTerm: 0}
	replyOldTerm := &RequestVoteReply{}
	rn.RequestVote(argsOldTerm, replyOldTerm)
	if replyOldTerm.VoteGranted {
		t.Errorf("Should reject vote if candidate log is not up-to-date (older term)")
	}

	// Scenario 3: Candidate log is longer -> Accept
	argsLong := &RequestVoteArgs{Term: 2, CandidateId: 3, LastLogIndex: 3, LastLogTerm: 1}
	replyLong := &RequestVoteReply{}
	rn.RequestVote(argsLong, replyLong)
	if !replyLong.VoteGranted {
		t.Errorf("Should grant vote if candidate log is up-to-date")
	}
}

func TestAppendEntries_ConflictAndTruncation(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3}, applyCh, NewMemoryPersister())

	// Set up node with conflicting log
	rn.mu.Lock()
	rn.currentTerm = 2
	rn.log = []LogEntry{
		{Term: 0, Index: 0, Command: ""}, // Sentinel
		{Term: 1, Index: 1, Command: "A"}, // OK
		{Term: 2, Index: 2, Command: "B"}, // CONFLICT (Leader will have Term 3 here)
		{Term: 2, Index: 3, Command: "C"}, // Will be truncated
	}
	rn.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1, // Matches our OK entry
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 3, Index: 2, Command: "NEW_B"},
			{Term: 3, Index: 3, Command: "NEW_C"},
		},
		LeaderCommit: 3,
	}

	reply := &AppendEntriesReply{}
	rn.AppendEntries(args, reply)

	if !reply.Success {
		t.Fatalf("AppendEntries should have succeeded")
	}

	rn.mu.RLock()
	defer rn.mu.RUnlock()
	
	if len(rn.log) != 4 {
		t.Fatalf("Expected log length 4, got %d", len(rn.log))
	}

	if rn.log[2].Command != "NEW_B" || rn.log[3].Command != "NEW_C" {
		t.Errorf("Log was not correctly truncated and appended. Log: %v", rn.log)
	}
}
