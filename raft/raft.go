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

	peerIDs   []int
	peerAddrs map[int]string // peerID → "host:rpcPort"
	transport *Transport

	// Persistent state (on all servers)
	persister   Persister
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state (on all servers)
	commitIndex int
	lastApplied int
	state       Role
	leaderID    int // -1 means unknown

	// Volatile state (on leaders only)
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh            chan LogEntry
	resetElectionTimer chan struct{}
	stopCh             chan struct{}
	replicateNow       chan struct{} // signals immediate replication after Propose
	snapshotCh         chan []byte   // delivers remote snapshots to the service layer (main.go)

	// Tunable timings (set before Start, or leave defaults)
	ElectionTimeoutBase   time.Duration // minimum election timeout (default 150ms)
	ElectionTimeoutSpread int           // random spread in ms added to base (default 150)
	HeartbeatInterval     time.Duration // leader heartbeat period (default 50ms)
}

func NewRaftNode(id int, peerIDs []int, applyCh chan LogEntry, persister Persister) *RaftNode {
	rn := &RaftNode{
		id:        id,
		peerIDs:   peerIDs,
		peerAddrs: make(map[int]string),

		persister:   persister,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // index 0 is a sentinel

		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		leaderID:    -1,

		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),

		applyCh:            applyCh,
		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		replicateNow:       make(chan struct{}, 1),
		snapshotCh:         make(chan []byte, 1),

		ElectionTimeoutBase:   150 * time.Millisecond,
		ElectionTimeoutSpread: 150,
		HeartbeatInterval:     50 * time.Millisecond,
	}

	// initialize from state persisted before a crash
	rn.readPersist(persister.ReadRaftState())

	// After restoring a compacted log, commitIndex and lastApplied must point
	// to array index 0 (the sentinel). The snapshot payload itself will be
	// loaded into the KV store by the caller (main.go), which covers everything
	// up through log[0].Index. Entries that follow the sentinel in the log will
	// be applied normally by applyCommittedEntries.
	rn.commitIndex = 0
	rn.lastApplied = 0

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
		return rn.ElectionTimeoutBase + time.Duration(rng.Intn(rn.ElectionTimeoutSpread+1))*time.Millisecond
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
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	rn.persist()

	log.Printf("[Node %d] Election timeout — transitioning to Candidate for term %d | leader=unknown", id, currentTerm)

	// Single-node cluster: we are the only voter → become leader immediately.
	if len(peerIDs) == 0 {
		rn.mu.Unlock()
		rn.becomeLeader()
		return
	}

	rn.mu.Unlock()

	// Vote counting
	var mu sync.Mutex
	votesGranted := 1 // vote for self
	majority := (len(peerIDs)+1)/2 + 1

	for _, peer := range peerIDs {
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			ok := rn.sendRequestVote(server, args, reply)
			if !ok {
				return
			}

			rn.mu.Lock()
			// If our term changed while RPCs were in flight, discard the result.
			if rn.currentTerm != currentTerm || rn.state != Candidate {
				rn.mu.Unlock()
				return
			}

			if reply.Term > rn.currentTerm {
				rn.becomeFollower(reply.Term)
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			if reply.VoteGranted {
				mu.Lock()
				votesGranted++
				got := votesGranted
				mu.Unlock()

				if got >= majority {
					rn.mu.Lock()
					// Double-check we're still a candidate for this term.
					if rn.state == Candidate && rn.currentTerm == currentTerm {
						rn.mu.Unlock()
						rn.becomeLeader()
					} else {
						rn.mu.Unlock()
					}
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	rn.state = Leader
	rn.leaderID = rn.id
	lastLogIndex := len(rn.log) - 1
	for _, peerID := range rn.peerIDs {
		rn.nextIndex[peerID] = lastLogIndex + 1
		rn.matchIndex[peerID] = 0
	}
	log.Printf("[Node %d] ★ Became Leader for term %d | leader=%d", rn.id, rn.currentTerm, rn.id)
	rn.mu.Unlock()

	// Start the heartbeat + replication loop (runs until no longer leader or stopped).
	go rn.leaderLoop()
}

// leaderLoop sends periodic heartbeats and handles on-demand replication.
func (rn *RaftNode) leaderLoop() {
	heartbeatTicker := time.NewTicker(rn.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			rn.mu.RLock()
			isLeader := rn.state == Leader
			rn.mu.RUnlock()
			if !isLeader {
				return
			}
			rn.replicateToAll()
		case <-rn.replicateNow:
			rn.mu.RLock()
			isLeader := rn.state == Leader
			rn.mu.RUnlock()
			if !isLeader {
				return
			}
			rn.replicateToAll()
		case <-rn.stopCh:
			return
		}
	}
}

// replicateToAll sends AppendEntries RPCs to every peer in parallel.
func (rn *RaftNode) replicateToAll() {
	rn.mu.RLock()
	peers := make([]int, len(rn.peerIDs))
	copy(peers, rn.peerIDs)
	rn.mu.RUnlock()

	for _, peer := range peers {
		go rn.replicateTo(peer)
	}
}

// replicateTo sends an AppendEntries RPC to a single peer and handles the response.
func (rn *RaftNode) replicateTo(peer int) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}

	next := rn.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	// If the peer is so far behind that the entries it needs were already
	// compacted into a snapshot, send the snapshot instead of AppendEntries.
	if next-1 >= len(rn.log) {
		snapData := rn.persister.ReadSnapshot()
		args := &InstallSnapshotArgs{
			Term:              rn.currentTerm,
			LeaderId:          rn.id,
			LastIncludedIndex: rn.log[0].Index,
			LastIncludedTerm:  rn.log[0].Term,
			Data:              snapData,
		}
		currentTerm := rn.currentTerm
		rn.mu.Unlock()

		log.Printf("[Node %d] Sending InstallSnapshot to peer %d (sentinel=%d)", rn.id, peer, args.LastIncludedIndex)
		reply := &InstallSnapshotReply{}
		if !rn.sendInstallSnapshot(peer, args, reply) {
			return
		}

		rn.mu.Lock()
		defer rn.mu.Unlock()

		if rn.currentTerm != currentTerm || rn.state != Leader {
			return
		}
		if reply.Term > rn.currentTerm {
			rn.becomeFollower(reply.Term)
			return
		}
		// Snapshot accepted: peer now has the sentinel; start sending
		// log entries from array index 1 on the next heartbeat.
		rn.nextIndex[peer] = 1
		rn.matchIndex[peer] = 0
		return
	}

	prevLogIndex := next - 1
	prevLogTerm := 0
	if prevLogIndex < len(rn.log) {
		prevLogTerm = rn.log[prevLogIndex].Term
	}

	// Collect entries to send
	var entries []LogEntry
	if next < len(rn.log) {
		entries = make([]LogEntry, len(rn.log)-next)
		copy(entries, rn.log[next:])
	}

	args := &AppendEntriesArgs{
		Term:         rn.currentTerm,
		LeaderId:     rn.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	currentTerm := rn.currentTerm
	rn.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rn.sendAppendEntries(peer, args, reply)
	if !ok {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.currentTerm != currentTerm || rn.state != Leader {
		return
	}

	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		rn.nextIndex[peer] = prevLogIndex + len(entries) + 1
		rn.matchIndex[peer] = prevLogIndex + len(entries)
		rn.advanceCommitIndex()
	} else {
		// Decrement nextIndex and retry on next heartbeat
		if rn.nextIndex[peer] > 1 {
			rn.nextIndex[peer]--
		}
	}
}

// advanceCommitIndex checks whether a majority of nodes have replicated
// entries and advances commitIndex accordingly. Caller MUST hold rn.mu.
func (rn *RaftNode) advanceCommitIndex() {
	for n := rn.commitIndex + 1; n < len(rn.log); n++ {
		if rn.log[n].Term != rn.currentTerm {
			continue // Raft only commits entries from current term
		}

		// Count how many peers have replicated index n (leader counts itself)
		matches := 1
		for _, peer := range rn.peerIDs {
			if rn.matchIndex[peer] >= n {
				matches++
			}
		}

		majority := (len(rn.peerIDs)+1)/2 + 1
		if matches >= majority {
			rn.commitIndex = n
		} else {
			break // No point checking higher indices
		}
	}
}

// triggerReplication signals the leader loop to send entries immediately.
func (rn *RaftNode) triggerReplication() {
	select {
	case rn.replicateNow <- struct{}{}:
	default:
	}
}

func (rn *RaftNode) becomeFollower(newTerm int) {
	// Always reset the election timer when we hear from a valid leader/candidate.
	defer func() {
		select {
		case rn.resetElectionTimer <- struct{}{}:
		default:
		}
	}()

	// If we're already a follower at this term, nothing to change.
	if rn.state == Follower && rn.currentTerm == newTerm {
		return
	}

	log.Printf("[Node %d] Stepping down to Follower (term %d → %d) | leader=%d", rn.id, rn.currentTerm, newTerm, rn.leaderID)

	rn.state = Follower
	if newTerm > rn.currentTerm {
		rn.currentTerm = newTerm
		rn.votedFor = -1
		rn.leaderID = -1 // unknown until we hear from the new leader
	}

	rn.persist()
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

// GetLeaderID returns the ID of the known leader (-1 if unknown).
func (rn *RaftNode) GetLeaderID() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

func (rn *RaftNode) GetID() int {
	return rn.id
}

// SetTransport assigns the network transport used for RPC calls.
func (rn *RaftNode) SetTransport(t *Transport) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.transport = t
}

// SetPeerAddrs sets the address map (peerID → "host:rpcPort") for all peers.
func (rn *RaftNode) SetPeerAddrs(addrs map[int]string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peerAddrs = addrs
}

// GetSnapshotCh returns the channel on which remotely-installed snapshots are
// delivered. main.go should drain this in a goroutine and call
// kvstore.LoadSnapshot on each received payload.
func (rn *RaftNode) GetSnapshotCh() <-chan []byte {
	return rn.snapshotCh
}

// saveSnapshotLocked atomically persists the current Raft state together with
// a snapshot payload. Caller must hold rn.mu (write lock).
func (rn *RaftNode) saveSnapshotLocked(snapshot []byte) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rn.currentTerm)
	_ = e.Encode(rn.votedFor)
	_ = e.Encode(rn.log)
	rn.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
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

// Propose submits a new command to the Raft log.
// Returns true if this node is the leader and the entry was appended.
// Returns false if this node is NOT the leader (caller should redirect).
func (rn *RaftNode) Propose(command string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return false
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   len(rn.log),
		Command: command,
	}
	rn.log = append(rn.log, entry)

	// Single-node cluster: commit immediately (we are the only voter).
	if len(rn.peerIDs) == 0 {
		rn.commitIndex = entry.Index
	} else {
		// Multi-node: trigger replication, commitIndex advances via advanceCommitIndex.
		go rn.triggerReplication()
	}

	rn.persist()

	log.Printf("[Node %d] Proposed entry %d (term %d): %s | leader=%d", rn.id, entry.Index, rn.currentTerm, command, rn.id)
	return true
}
