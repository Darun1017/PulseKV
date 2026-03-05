package raft

import (
	"bytes"
	"encoding/gob"
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
	persister   Persister
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state (on all servers)
	commitIndex int
	lastApplied int
	state       Role

	// Volatile state (on leaders only)
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh            chan LogEntry
	resetElectionTimer chan struct{}
	stopCh             chan struct{}
}

func NewRaftNode(id int, peerIDs []int, applyCh chan LogEntry, persister Persister) *RaftNode {
	rn := &RaftNode{
		id:      id,
		peerIDs: peerIDs,

		persister:   persister,
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

	// initialize from state persisted before a crash
	rn.readPersist(persister.ReadRaftState())

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

	rn.persist()

	log.Printf("[Node %d] Election timeout — transitioning to Candidate for term %d", id, currentTerm)
	rn.mu.Unlock()

	// Send RequestVote RPCs
	for _, peer := range peerIDs {
		if peer != id {
			go func(server int) {
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  id,
					LastLogIndex: len(rn.log) - 1,
					LastLogTerm:  rn.log[len(rn.log)-1].Term,
				}
				reply := &RequestVoteReply{}

				// TODO (teammate – networking): use actual RPC
				if rn.sendRequestVote(server, args, reply) {
					// Handle reply
				}
			}(peer)
		}
	}
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

	rn.persist()

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

// -- Persistence Engine --

// persist saves the node's critical state to stable storage (WAL).
// The caller MUST hold rn.mu
func (rn *RaftNode) persist() {
	if rn.persister == nil {
		return
	}

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(rn.currentTerm); err != nil {
		log.Printf("[Node %d] persist error currentTerm: %v", rn.id, err)
		return
	}
	if err := e.Encode(rn.votedFor); err != nil {
		log.Printf("[Node %d] persist error votedFor: %v", rn.id, err)
		return
	}
	if err := e.Encode(rn.log); err != nil {
		log.Printf("[Node %d] persist error log: %v", rn.id, err)
		return
	}
	state := w.Bytes()
	rn.persister.SaveRaftState(state)
}

// readPersist restores previously persisted state.
func (rn *RaftNode) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logArr []LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		log.Printf("[Node %d] readPersist error currentTerm: %v", rn.id, err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Printf("[Node %d] readPersist error votedFor: %v", rn.id, err)
		return
	}
	if err := d.Decode(&logArr); err != nil {
		log.Printf("[Node %d] readPersist error log: %v", rn.id, err)
		return
	}

	rn.currentTerm = currentTerm
	rn.votedFor = votedFor
	rn.log = logArr
}

// Snapshot allows the service (KV store) to tell Raft that it has snapshotted
// up to index. Raft can then discard the log before that index.
func (rn *RaftNode) Snapshot(index int, snapshot []byte) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	lastIncludedIndex := rn.log[0].Index
	// If the snapshot index is behind our current log start, or ahead of our commit, ignore
	if index <= lastIncludedIndex || index > rn.commitIndex {
		return
	}

	// Find the snapshot index in our log array
	var shift int
	for i, entry := range rn.log {
		if entry.Index == index {
			shift = i
			break
		}
	}

	if shift == 0 {
		return // Index not found in log
	}

	lastIncludedTerm := rn.log[shift].Term

	// Create a new log starting with a dummy entry
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Index: index, Term: lastIncludedTerm}
	newLog = append(newLog, rn.log[shift+1:]...)

	rn.log = newLog

	// Persist the state AND the snapshot
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rn.currentTerm)
	_ = e.Encode(rn.votedFor)
	_ = e.Encode(rn.log)
	state := w.Bytes()

	rn.persister.SaveStateAndSnapshot(state, snapshot)

	log.Printf("[Node %d] Snapshotted through index %d (term %d)", rn.id, index, lastIncludedTerm)
}
