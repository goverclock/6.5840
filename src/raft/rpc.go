package raft

import (
	"sync"
)

var rpcLock sync.Mutex // should be acquired whenever invoke a RPC handler

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntryArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entry to replicate(empty for heart beat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rpcLock.Lock()
	defer rpcLock.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reveiver implementation:
	// 1. reply false if term < currentTerm
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm < args.Term { // step down to follower
		// rf.ResetLastHeartBeat()	// should only reset after granting vote to the candidate
		rf.toFollower(args.Term)
		Debug(dTerm, "S%d term=%d", rf.me, rf.currentTerm)
		Debug(dClient, "S%d become follower(vote)", rf.me)
	}

	// 2. if votedFor is null or candidateId, and candidate's log is at least as up-to-date
	// 	as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if the candidate's log is up to date
		// determine which of two logs is more up-to-date by comparing the index and
		// term of the last entries in the logs
		// if terms differ, higher term = more up-to-date
		// else longer log = more up-to-date
		logLen := rf.LogLen()
		lastLog := rf.LogAt(logLen - 1)
		if args.LastLogTerm < lastLog.Term {
			Debug(dVote, "S%d deny S%d(term %d<%d)", rf.me, args.CandidateId, args.LastLogTerm, lastLog.Term)
			return
		} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex < logLen-1 {
			Debug(dVote, "S%d deny S%d(log len)", rf.me, args.CandidateId)
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		Debug(dVote, "S%d voted S%d", rf.me, args.CandidateId)
		rf.ResetLastHeartBeat()
	}
}

func (rf *Raft) AppendEntryHandler(args *AppendEntryArgs, reply *AppendEntryReply) {
	rpcLock.Lock()
	defer rpcLock.Unlock()

	defer func() {
		if len(args.Entries) != 0 {
			Debug(dLog, "S%d reply %v", rf.me, reply)
		} else {
			Debug(dTimer, "S%d got hb", rf.me)
		}
	}()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 1 {
		panic("should only append at most 1 entry at one time")
	}
	// reset election timeout
	rf.ResetLastHeartBeat()

	// if find higher term, step down
	if rf.currentTerm < args.Term {
		rf.ResetLastHeartBeat()
		rf.toFollower(args.Term)
		Debug(dClient, "S%d become follower(append)", rf.me)
		Debug(dTerm, "S%d term=%d", rf.me, rf.currentTerm)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	// receiver implementation for replicaing log entries
	// 1. reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	prevInd := args.PrevLogIndex
	if len(args.Entries) != 0 { // if not heart beat
		// 2. reply false if log doesn't contain an entry at preLogIndex whose
		// 	term matches prevLogTerm
		logLen := rf.LogLen()
		if logLen <= prevInd {
			reply.Success = false
			return
		}
		if rf.LogAt(prevInd).Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		// 3. if an existing entry conflicts with a new one(same index, different term),
		//  delete the existing entry and all that follow it
		entry := args.Entries[0]
		if logLen > prevInd+1 && rf.LogAt(prevInd+1).Term != entry.Term {
			rf.LogRemoveFrom(prevInd + 1)
		}
		// 4. append any new entries *not already* in the log
		if rf.LogLen() == prevInd+1 {
			rf.LogAppend(entry)
		}
	}
	// 5. if leaderCommit > commitIndex, set commitIndex =
	//  min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.SetCommitIndex(min(args.LeaderCommit, rf.LogLen()-1))
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// tldr: may delay return, do not lock rf on this
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)
	return ok
}
