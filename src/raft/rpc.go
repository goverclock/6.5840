package raft

import (
	"sync"
)

var rpcLock sync.Mutex // should be acquired whenever invoke a RPC handler

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
	// lastLogIndex	int
	// lastLogTerm		int
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
	Success bool // true if follower contained entry matching
	// prevLogIndex and prevLogTerm
}

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rpcLock.Lock()
	defer rpcLock.Unlock()

	// reveiver implementation:
	// 1. reply false if term < currentTerm
	// 2. if votedFor is null of candidateId, and candidate's log is at least as up-to-date
	// 	as receiver's log, grant vote
	reply.Term = rf.CurrentTerm()
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm() {
		return
	}
	if rf.CurrentTerm() < args.Term { // step down to follower
		rf.toFollower(args.Term)
		Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
		Debug(dClient, "S%d become follower(vote)", rf.Me())
		rf.ResetLastHeartBeat()
	}
	if rf.VotedFor() == -1 || rf.VotedFor() == args.CandidateId {
		// TODO: check if log is up to date
		rf.SetVotedFor(args.CandidateId)
		reply.VoteGranted = true
		Debug(dVote, "S%d voted S%d", rf.Me(), args.CandidateId)
		rf.ResetLastHeartBeat()
	}
}

func (rf *Raft) AppendEntryHandler(args *AppendEntryArgs, reply *AppendEntryReply) {
	rpcLock.Lock()
	defer rpcLock.Unlock()

	if len(args.Entries) > 1 {
		panic("should only append at most 1 entry at one time")
	}
	// reset election timeout
	rf.ResetLastHeartBeat()

	// if find higher term, step down
	if rf.CurrentTerm() < args.Term {
		rf.toFollower(args.Term)
		Debug(dClient, "S%d become follower(append)", rf.Me())
		Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
		rf.ResetLastHeartBeat()
	}
	defer func() {
		if len(args.Entries) != 0 {
			Debug(dLog, "S%d reply %v", rf.me, reply)
		} else {
			Debug(dLeader, "S%d got hb", rf.me)
		}
	}()

	reply.Term = rf.CurrentTerm()
	reply.Success = true
	// receiver implementation for replicaing log entries
	// 1. reply false if term < currentTerm
	if rf.CurrentTerm() > args.Term {
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
	if args.LeaderCommit > rf.CommitIndex() {
		rf.SetCommitIndex(min(args.LeaderCommit, prevInd+1))
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
