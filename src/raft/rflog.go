package raft

import "fmt"

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) LogLen() int {
	return len(rf.logs) + rf.logStartIndex - 1
}

func (rf *Raft) LogAt(i int) LogEntry {
	if i == 0 { // dummy log
		return LogEntry{}
	}
	// if i < logStartIndex, should install snapshot,
	// *but should invoke before reaching this point*
	return rf.logs[i-rf.logStartIndex]
}

func (rf *Raft) LogTermAt(i int) int {
	if i >= rf.logStartIndex {
		return rf.LogAt(i).Term
	}
	if i == rf.logStartIndex - 1 {
		return rf.lastIncludedTerm
	}
	fmt.Println("LogTermAt()", i)
	panic("LogTermAt()")
}

// return index of first entry with term t
// find in index range [1, bound)
func (rf *Raft) FirstWithTerm(t int, bound int) int {
	if rf.LogTermAt(bound)!= t {
		panic("bad use of FirstWithTerm()")
	}
	for rf.LogTermAt(bound)== t {
		bound--
		if bound < rf.logStartIndex - 1 {
			break
		}
	}
	return bound + 1
}

// return if rf.logs contain entry with term,
// if true, also return the index of last such entry
func (rf *Raft) HasTerm(t int) (bool, int) {
	for i := rf.LogLen(); i >= rf.logStartIndex - 1; i-- {
		iTerm := rf.LogTermAt(i)
		if iTerm == t {
			return true, i
		}
		if iTerm < t {
			break
		}
	}
	return false, -1
}

func (rf *Raft) LogFrom(i int) []LogEntry {
	return rf.logs[i-rf.logStartIndex:]
}

func (rf *Raft) LogRemoveFrom(i int) {
	rf.logs = rf.logs[:i-rf.logStartIndex]
	rf.persist()
	Debug(dLog, "S%d remove log [%d,]", rf.me, i)
}

func (rf *Raft) LogTrimHead(i int) {
	rf.logs = rf.logs[i-rf.logStartIndex+1:]
	rf.logStartIndex = i + 1
	rf.persist()
	Debug(dSnap, "S%d drop head log [,%d]", rf.me, i)
}

func (rf *Raft) LogAppend(log LogEntry) {
	rf.logs = append(rf.logs, log)
	rf.persist()
	Debug(dLog, "S%d append (%d,%d)=%v", rf.me, log.Term, rf.LogLen(), log.Command)
}

func (rf *Raft) LogAppends(log []LogEntry) {
	rf.logs = append(rf.logs, log...)
	rf.persist()
	Debug(dLog, "S%d append (%d,%d-%d)=%v (x%d)", rf.me, log[0].Term, rf.LogLen()-len(log)+1, rf.LogLen(), log[0].Command, len(log))
}

func (rf *Raft) SetCommitIndex(ci int) {
	if rf.commitIndex >= ci {
		return
	}
	Debug(dLog, "S%d set ci to %d", rf.me, ci)
	rf.commitIndex = ci
}
