package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

//	"bytes"

//	"6.5840/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// persistent state on all servers
	// updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int // -1 for null
	logs        []LogEntry
	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int
	// volatile state on leaders, reinitialized after election
	nextIndex  []int // index of next log entry to send for each followers
	matchIndex []int // index of highest log entry known to be replicated on a server

	lastHeartBeat time.Time // time of receiving last heart beat(AppendEntry)
	state         State     // Leader, Candidate, Follower
	logStartIndex int

	// snapshot
	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int

	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logStartIndex)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ct int
	var vf int
	var lsi int
	var lg []LogEntry
	var lii int
	var lit int
	if d.Decode(&ct) != nil || d.Decode(&vf) != nil || d.Decode(&lsi) != nil ||
		d.Decode(&lg) != nil || d.Decode(&lii) != nil || d.Decode(&lit) != nil {
		Debug(dError, "S%d readPersist(): fail to decode", rf.me)
		panic("readPersist() failed")
	} else {
		rf.currentTerm = ct
		rf.votedFor = vf
		rf.logStartIndex = lsi
		rf.logs = lg
		rf.lastIncludedIndex = lii
		rf.lastIncludedTerm = lit
	}
}

// must be called after readPersist() in rf.Make()
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	// apply the snapshot
	lii := rf.lastIncludedIndex
	lit := rf.lastIncludedTerm
	rf.snapShot = data
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotIndex: lii,
		SnapshotTerm:  lit,
	}
	rf.mu.Lock()
	go func() {
		rf.applyChan <- msg
		rf.mu.Unlock()
	}()
	rf.commitIndex = lii
	rf.lastApplied = lii
	rf.ResetLastHeartBeat()
	rf.logStartIndex = lii + 1
	rf.lastIncludedIndex = lii
	rf.lastIncludedTerm = lit
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapShot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.LogAt(index).Term
	rf.LogTrimHead(index)
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader { // not a leader, just return false
		return -1, -1, false
	}

	log := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	// 1. leader stores command in its own logs
	rf.LogAppend(log)
	rf.matchIndex[rf.me] = rf.LogLen()
	Debug(dLog2, "S%d start (%d,%d)=%v", rf.me, log.Term, rf.LogLen(), log.Command)
	Debug(dLog, "S%d ni=%v", rf.me, rf.nextIndex)
	index := rf.LogLen()
	// 2. leader issue AppendEntries RPC in parallel to each of
	// 	the other servers to replicate the entry
	// (this is done by AppendEntryTicker)

	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	numPeers := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logStartIndex = 1
	rf.snapShot = nil
	rf.commitIndex = 0 // no entry committed
	rf.lastApplied = 0
	rf.nextIndex = make([]int, numPeers)
	rf.matchIndex = make([]int, numPeers)
	rf.lastHeartBeat = time.Now()
	rf.state = Follower
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	Debug(dClient, "S%d startup", rf.me)

	go rf.electTicker()
	go rf.applyTicker()
	go rf.appendEntryTicker()
	go rf.commitTicker()

	return rf
}

// convert to leader, reset nextIndex, matchIndex
func (rf *Raft) toLeader() {
	rf.state = Leader
	numPeers := len(rf.peers)
	rf.nextIndex = make([]int, numPeers) // initialized to leader last log index + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.LogLen() + 1
	}
	rf.matchIndex = make([]int, numPeers) // initialized to 0, increases monotonically
}

// convert to candidate, increment term, vote for self
func (rf *Raft) toCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
}

// convert to follower, set term to t, reset voteFor
func (rf *Raft) toFollower(t int) {
	rf.currentTerm = t
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) ResetLastHeartBeat() {
	Debug(dTimer, "S%d reset elec timeout", rf.me)
	rf.lastHeartBeat = time.Now()
}
