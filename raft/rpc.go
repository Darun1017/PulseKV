package raft

import (
	"log"
)

// -- RPC Structs --

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int // sentinel index stored in log[0].Index on the leader
	LastIncludedTerm  int // term of that sentinel entry
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// -- Handlers --

// RequestVote handles an incoming vote request from a Candidate.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	defer rn.persist() // Ensure any state changes (like term or vote) are persisted

	// 1. Reply false if term < currentTerm
	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. If term > currentTerm, become follower and update term
	if args.Term > rn.currentTerm {
		log.Printf("[Node %d] RequestVote: stepping down to term %d, previously %d", rn.id, args.Term, rn.currentTerm)
		rn.becomeFollower(args.Term) // Note: becomeFollower resets votedFor to -1
	}

	reply.Term = rn.currentTerm

	// 3. Check log up-to-date
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	// 4. Grant vote if we haven't voted for someone else in this term, and candidate log is up to date
	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && logIsUpToDate {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true

		// Reset election timer since we granted a vote (meaning this candidate is viable)
		select {
		case rn.resetElectionTimer <- struct{}{}:
		default:
		}

		log.Printf("[Node %d] Granted vote to Candidate %d in term %d", rn.id, args.CandidateId, rn.currentTerm)
	} else {
		reply.VoteGranted = false
		log.Printf("[Node %d] Denied vote to Candidate %d in term %d (votedFor: %d, logUpToDate: %v)", rn.id, args.CandidateId, rn.currentTerm, rn.votedFor, logIsUpToDate)
	}
}

// AppendEntries handles log replication and heartbeats from the Leader.
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	defer rn.persist() // Ensure log updates are atomic with currentTerm/votedFor

	// 1. Reply false if term < currentTerm
	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.Success = false
		return
	}

	// 2. Either way, this is a valid leader. Become follower if we aren't already or term advanced.
	rn.leaderID = args.LeaderId // track who the leader is
	rn.becomeFollower(args.Term)
	reply.Term = rn.currentTerm

	// 3. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rn.log) {
		reply.Success = false
		return
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Truncate the log here because it conflicts
		rn.log = rn.log[:args.PrevLogIndex]
		reply.Success = false
		return
	}

	// 4. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rn.log) {
			if rn.log[index].Term != entry.Term {
				rn.log = rn.log[:index]
				rn.log = append(rn.log, args.Entries[i:]...)
				break // log has been replaced with args.Entries
			}
		} else {
			// 5. Append any new entries not already in the log
			rn.log = append(rn.log, entry)
		}
	}

	reply.Success = true

	// 6. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
	}
}

// -- Callers --

// sendRequestVote sends a RequestVote RPC to a peer via the transport.
func (rn *RaftNode) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rn.transport != nil {
		return rn.transport.CallRequestVote(server, args, reply)
	}
	// Fallback for tests without a transport.
	log.Printf("[Node %d] sendRequestVote: no transport configured (peer %d)", rn.id, server)
	return false
}

// sendAppendEntries sends an AppendEntries RPC to a peer via the transport.
func (rn *RaftNode) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rn.transport != nil {
		return rn.transport.CallAppendEntries(server, args, reply)
	}
	// Fallback for tests without a transport.
	log.Printf("[Node %d] sendAppendEntries: no transport configured (peer %d)", rn.id, server)
	return false
}

// InstallSnapshot handles an incoming snapshot sent by the leader when a
// follower is so far behind that the needed log entries have been compacted.
func (rn *RaftNode) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm

	// 1. Reject stale leaders.
	if args.Term < rn.currentTerm {
		return
	}

	rn.leaderID = args.LeaderId
	rn.becomeFollower(args.Term)
	reply.Term = rn.currentTerm

	// 2. Ignore if we already have this snapshot or a newer one.
	if args.LastIncludedIndex <= rn.log[0].Index {
		return
	}

	// 3. Replace the entire log with a single sentinel entry covering the snapshot.
	rn.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}

	// 4. Reset array-index pointers to the sentinel.
	//    Entries after the sentinel will arrive via normal AppendEntries.
	rn.commitIndex = 0
	rn.lastApplied = 0

	// 5. Atomically persist Raft state + snapshot to disk.
	rn.saveSnapshotLocked(args.Data)

	// 6. Notify main.go to reload the KV store from this snapshot.
	//    Non-blocking replace: if a snapshot is already pending, evict and enqueue the newer one.
	select {
	case rn.snapshotCh <- args.Data:
	default:
		<-rn.snapshotCh
		rn.snapshotCh <- args.Data
	}

	log.Printf("[Node %d] InstallSnapshot: accepted snapshot from leader %d (sentinel=%d, term=%d)",
		rn.id, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a peer via the transport.
func (rn *RaftNode) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rn.transport != nil {
		return rn.transport.CallInstallSnapshot(server, args, reply)
	}
	log.Printf("[Node %d] sendInstallSnapshot: no transport configured (peer %d)", rn.id, server)
	return false
}
