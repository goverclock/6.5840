package raft

func (rf *Raft) LogLen() int {
	return len(rf.logs)
}

func (rf *Raft) LogAt(i int) LogEntry {
	return rf.logs[i]
}

func (rf *Raft) LogRemoveFrom(i int) {
	rf.logs = rf.logs[:i]
	Debug(dLog, "S%d remove log from %d", rf.me, i)
}

func (rf *Raft) LogAppend(log LogEntry) {
	rf.logs = append(rf.logs, log)
	Debug(dLog, "S%d append (%d,%d)=%v", rf.me, log.Term, len(rf.logs) - 1, log.Command)
}

func (rf *Raft) SetCommitIndex(ci int) {
	if rf.commitIndex >= ci {
		return
	}
	Debug(dLog, "S%d set ci to %d", rf.me, ci)
	rf.commitIndex = ci
}
