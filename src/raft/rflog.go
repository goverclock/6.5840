package raft

func (rf *Raft) LogLen() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.logs)
}

func (rf *Raft) LogAt(i int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logs[i]
}

func (rf *Raft) LogRemoveFrom(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = rf.logs[:i]
}

func (rf *Raft) LogAppend(log LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = append(rf.logs, log)
	Debug(dLog, "S%d append log(%d,%d)", rf.me, log.Term, len(rf.logs) - 1)
}

func (rf *Raft) CommitOne() {
	Debug(dLog, "S%d commit one", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex++
}

func (rf *Raft) SetCommitIndex(ci int) {
	Debug(dLog, "S%d set ci to %d", rf.me, ci)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex >= ci {
		panic("SetCommitIndex: rf.commitIndex >= ci")
	}
	rf.commitIndex = ci
}
