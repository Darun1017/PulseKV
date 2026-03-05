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

// -- Callers (Stubs) --

// sendRequestVote is a stub for calling the RequestVote RPC on a peer.
// In a real network implementation, this uses an RPC client (e.g., net/rpc or gRPC).
func (rn *RaftNode) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// TODO (teammate - networking): Implement actual network call
	log.Printf("[Node %d] Sending RequestVote to Node %d (CandidateId: %d, Term: %d, LastLogIndex: %d)", rn.id, server, args.CandidateId, args.Term, args.LastLogIndex)
	return false
}

// sendAppendEntries is a stub for calling the AppendEntries RPC on a peer.
// In a real network implementation, this uses an RPC client (e.g., net/rpc or gRPC).
func (rn *RaftNode) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// TODO (teammate - networking): Implement actual network call
	log.Printf("[Node %d] Sending AppendEntries to Node %d (LeaderId: %d, Term: %d, PrevLogIndex: %d, Entries: %d)", rn.id, server, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries))
	return false
}
