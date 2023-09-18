package raft

import (
	"sync"
	"time"
)

var rpcLock sync.Mutex	// should be acquired whenever invoke a RPC handler

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
	Term     int // leader's term
	LeaderId int
	// prevLogIndex	int
	// Entries		[]Entry
	// LeaderCommit	int
}

type AppendEntryReply struct {
	Term int // currentTerm, for leader to update itself
	// Success bool
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
	if  rf.CurrentTerm() < args.Term { // step down to follower
		rf.incrementTermTo(args.Term)
		Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
		rf.SetState(Follower)
		Debug(dLeader, "S%d become follower(vote)", rf.Me())
	}
	if rf.VotedFor() == -1 || rf.VotedFor() == args.CandidateId {
		// TODO: check if log is up to date
		rf.SetVotedFor(args.CandidateId)
		reply.VoteGranted = true
		Debug(dLeader, "S%d voted S%d", rf.Me(), args.CandidateId)
	}
}

func (rf *Raft) AppendEntryHandler(args *AppendEntryArgs, reply *AppendEntryReply) {
	rpcLock.Lock()
	defer rpcLock.Unlock()
	// receiver implementation
	// 1. reply false if term < currentTerm
	// 2. reply false if log doesn't contain an entry at preLogIndex whose
	// 	term matches prevLogTerm
	// 3. ...
	// 4. ...

	// reset election timeout
	Debug(dLeader, "S%d reset elec timeout", rf.Me())
	rf.SetLastAppendEntryTime(time.Now())
	if rf.CurrentTerm() < args.Term {
		rf.incrementTermTo(args.Term)
		Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
		rf.SetState(Follower)
		Debug(dLeader, "S%d become follower(append)", rf.Me())
	}
	reply.Term = rf.CurrentTerm()
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
