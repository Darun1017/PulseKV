package raft

import (
	"sync"
	"testing"
	"time"
)

func TestNewRaftNodeDefaults(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	term, role, isLeader := rn.GetState()
	if term != 0 {
		t.Fatalf("expected initial term 0, got %d", term)
	}
	if role != Follower {
		t.Fatalf("expected Follower, got %s", role)
	}
	if isLeader {
		t.Fatal("new node should not be leader")
	}
	if rn.GetID() != 1 {
		t.Fatalf("expected id 1, got %d", rn.GetID())
	}
}

func TestRoleString(t *testing.T) {
	cases := []struct {
		role Role
		want string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{Role(99), "Unknown(99)"},
	}
	for _, tc := range cases {
		if got := tc.role.String(); got != tc.want {
			t.Errorf("Role(%d).String() = %q, want %q", int(tc.role), got, tc.want)
		}
	}
}

func TestElectionTimeoutTransitionToCandidate(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	rn.Start()
	time.Sleep(500 * time.Millisecond)

	term, role, _ := rn.GetState()
	if role != Candidate {
		t.Fatalf("expected Candidate after timeout, got %s", role)
	}
	if term < 1 {
		t.Fatalf("expected term >= 1 after election, got %d", term)
	}

	rn.Stop()
}

func TestBecomeLeader(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	rn.mu.Lock()
	rn.currentTerm = 1
	rn.state = Candidate
	rn.mu.Unlock()

	rn.becomeLeader()

	term, role, isLeader := rn.GetState()
	if !isLeader || role != Leader {
		t.Fatalf("expected Leader, got %s (isLeader=%v)", role, isLeader)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}
}

func TestBecomeFollower(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	rn.mu.Lock()
	rn.currentTerm = 3
	rn.state = Leader
	rn.becomeFollower(5)
	rn.mu.Unlock()

	term, role, isLeader := rn.GetState()
	if role != Follower {
		t.Fatalf("expected Follower, got %s", role)
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
	if isLeader {
		t.Fatal("should not be leader after stepping down")
	}
}

func TestApplyCommittedEntries(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	rn.mu.Lock()
	rn.log = append(rn.log, LogEntry{Term: 1, Index: 1, Command: "PUT x 1"})
	rn.log = append(rn.log, LogEntry{Term: 1, Index: 2, Command: "PUT y 2"})
	rn.commitIndex = 2
	rn.mu.Unlock()

	rn.applyCommittedEntries()

	for i := 1; i <= 2; i++ {
		select {
		case entry := <-applyCh:
			if entry.Index != i {
				t.Fatalf("expected entry index %d, got %d", i, entry.Index)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for entry %d on applyCh", i)
		}
	}

	rn.mu.RLock()
	if rn.lastApplied != 2 {
		t.Fatalf("expected lastApplied=2, got %d", rn.lastApplied)
	}
	rn.mu.RUnlock()
}

func TestStartStop(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)

	rn.Start()
	time.Sleep(50 * time.Millisecond)
	rn.Stop()
}

func TestConcurrentGetState(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)
	rn.Start()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rn.GetState()
		}()
	}
	wg.Wait()
	rn.Stop()
}

func TestResetElectionTimerPostponesElection(t *testing.T) {
	applyCh := make(chan LogEntry, 16)
	rn := NewRaftNode(1, []int{2, 3, 4}, applyCh)
	rn.Start()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			select {
			case rn.resetElectionTimer <- struct{}{}:
			default:
			}
			time.Sleep(50 * time.Millisecond)
		}
		close(done)
	}()

	<-done
	term, role, _ := rn.GetState()
	if role != Follower || term != 0 {
		t.Fatalf("expected Follower/term 0, got %s/term %d", role, term)
	}
	rn.Stop()
}
