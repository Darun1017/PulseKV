package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(r))
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command string
}

// RaftNode holds all the state required by the Raft algorithm for a single node.
// DEADLOCK NOTE:
// 1. Never hold mu while performing a blocking I/O operation (network send, disk write).
// 2. Never call an exported RaftNode method from inside another method that already holds mu.
// 3. Channel sends on applyCh are done after releasing mu to avoid blocking.
type RaftNode struct {
	mu sync.RWMutex
	id int

	// TODO (teammate – networking): populate this from the cluster config.
	peerIDs []int

	// Persistent state (on all servers)
	// TODO (teammate – disk I/O): hook persistence here.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state (on all servers)
	commitIndex int
	lastApplied int
	state       Role

	// Volatile state (on leaders only)
	// TODO (teammate – log replication): populate nextIndex and matchIndex when this node becomes Leader.
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh            chan LogEntry
	resetElectionTimer chan struct{}
	stopCh             chan struct{}
}

func NewRaftNode(id int, peerIDs []int, applyCh chan LogEntry) *RaftNode {
	rn := &RaftNode{
		id:      id,
		peerIDs: peerIDs,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // index 0 is a sentinel

		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,

		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),

		applyCh:            applyCh,
		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
	}
	return rn
}

func (rn *RaftNode) Start() {
	go rn.run()
	log.Printf("[Node %d] Raft engine started", rn.id)
}

func (rn *RaftNode) Stop() {
	close(rn.stopCh)
	log.Printf("[Node %d] Raft engine stopped", rn.id)
}

func (rn *RaftNode) run() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rn.id)))
	newElectionTimeout := func() time.Duration {
		return time.Duration(150+rng.Intn(151)) * time.Millisecond
	}

	electionTimer := time.NewTimer(newElectionTimeout())
	defer electionTimer.Stop()

	for {
		select {
		case <-electionTimer.C:
			rn.handleElectionTimeout(rng)
			electionTimer.Reset(newElectionTimeout())
		case <-rn.resetElectionTimer:
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(newElectionTimeout())
		case <-rn.stopCh:
			log.Printf("[Node %d] Event loop exiting", rn.id)
			return
		}

		rn.applyCommittedEntries()
	}
}

func (rn *RaftNode) handleElectionTimeout(rng *rand.Rand) {
	rn.mu.Lock()
	if rn.state == Leader {
		// TODO (teammate – networking): implement heartbeat sending
		rn.mu.Unlock()
		return
	}

	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	currentTerm := rn.currentTerm
	id := rn.id
	peerIDs := make([]int, len(rn.peerIDs))
	copy(peerIDs, rn.peerIDs)

	// TODO (teammate – disk I/O): persist currentTerm and votedFor to stable storage before sending any RPCs.

	log.Printf("[Node %d] Election timeout — transitioning to Candidate for term %d", id, currentTerm)
	rn.mu.Unlock()

	// TODO (teammate – networking): implement sendRequestVote RPC to each peer.
	_ = peerIDs
}

func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.state = Leader
	lastLogIndex := len(rn.log) - 1
	for _, peerID := range rn.peerIDs {
		rn.nextIndex[peerID] = lastLogIndex + 1
		rn.matchIndex[peerID] = 0
	}

	log.Printf("[Node %d] Became Leader for term %d", rn.id, rn.currentTerm)
	// TODO (teammate – networking): start sending periodic heartbeats
}

func (rn *RaftNode) becomeFollower(newTerm int) {
	log.Printf("[Node %d] Stepping down to Follower (term %d → %d)", rn.id, rn.currentTerm, newTerm)

	rn.state = Follower
	rn.currentTerm = newTerm
	rn.votedFor = -1

	// TODO (teammate – disk I/O): persist currentTerm and votedFor.

	select {
	case rn.resetElectionTimer <- struct{}{}:
	default:
	}
}

func (rn *RaftNode) applyCommittedEntries() {
	rn.mu.Lock()
	var entriesToApply []LogEntry
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		if rn.lastApplied < len(rn.log) {
			entriesToApply = append(entriesToApply, rn.log[rn.lastApplied])
		}
	}
	rn.mu.Unlock()

	for _, entry := range entriesToApply {
		rn.applyCh <- entry
	}
}

func (rn *RaftNode) GetState() (term int, role Role, isLeader bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm, rn.state, rn.state == Leader
}

func (rn *RaftNode) GetID() int {
	return rn.id
}

// TODO (teammate – networking): Implement RequestVote and AppendEntries RPC handlers.
