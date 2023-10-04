package raft

func (rf *Raft) LogLen() int {
	return len(rf.logs)
}

func (rf *Raft) LogAt(i int) LogEntry {
	return rf.logs[i]
}

// return index of first entry with term t
// find in index range (0, bound)
func (rf *Raft) FirstWithTerm(t int, bound int) int {
	for rf.logs[bound].Term == t {
		bound--
	}
	return bound + 1
}

// return if rf.logs contain entry with term,
// if true, also return the index of last such entry
func (rf *Raft) HasTerm(t int) (bool, int) {
	for i := len(rf.logs) - 1; i > 0; i-- {
		iTerm := rf.logs[i].Term
		if iTerm == t {
			return true, i
		}
		if iTerm < t {
			break
		}
	}
	return false, -1
}

func (rf *Raft) LogRemoveFrom(i int) {
	rf.logs = rf.logs[:i]
	rf.persist()
	Debug(dLog, "S%d remove log from %d", rf.me, i)
}

func (rf *Raft) LogAppend(log LogEntry) {
	rf.logs = append(rf.logs, log)
	rf.persist()
	Debug(dLog, "S%d append (%d,%d)=%v", rf.me, log.Term, len(rf.logs)-1, log.Command)
}

func (rf *Raft) LogAppends(log []LogEntry) {
	rf.logs = append(rf.logs, log...)
	rf.persist()
	Debug(dLog, "S%d append (%d,%d)=%v (x%d)", rf.me, log[0].Term, len(rf.logs)-1, log[0].Command, len(log))
}

func (rf *Raft) SetCommitIndex(ci int) {
	if rf.commitIndex >= ci {
		return
	}
	Debug(dLog, "S%d set ci to %d", rf.me, ci)
	rf.commitIndex = ci
}
