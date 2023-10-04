package raft

import (
	"math/rand"
	"time"
)

// send heartbeat(empty AppendEntries) when the server is leader
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(pi int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				fni := rf.nextIndex[pi] // fni - follower's next index
				args := AppendEntryArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = fni - 1
				args.PrevLogTerm = rf.logs[fni-1].Term
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()

				reply := AppendEntryReply{}
				ok := rf.sendAppendEntry(pi, &args, &reply)
				if ok {
					Debug(dTimer, "S%d hb to S%d(GOOD)", rf.me, pi)
				} else {
					Debug(dTimer, "S%d hb to S%d(BAD)", rf.me, pi)
				}
				// if leader(or candidate) discovers higher term, become follower
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					Debug(dLeader, "S%d become follower(term %d)", rf.me, reply.Term)
					rf.ResetLastHeartBeat()
					rf.toFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(i)
		}
		time.Sleep(120 * time.Millisecond) // heart beat interval
	}
}

func (rf *Raft) electTicker() {
	randTimeout := time.Duration(600+(rand.Int()%300)) * time.Millisecond
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		diff := time.Since(rf.lastHeartBeat)
		if rf.state == Leader || diff < randTimeout {
			rf.mu.Unlock()
			continue
		}

		// 600 ~ 900ms election timeout
		randTimeout = time.Duration(600+(rand.Int()%300)) * time.Millisecond

		// election timeout
		rf.ResetLastHeartBeat() // reset election timeout
		// become candidate, init election
		rf.toCandidate()
		Debug(dClient, "S%d become candidate", rf.me)
		Debug(dLeader, "S%d init election", rf.me)
		Debug(dTerm, "S%d term=%d", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		// when election timeout again during election,
		// quit current and start new election
		quit := make(chan int)
		go func() {
			// send RequestVoteRPCs to all other servers
			voteCh := make(chan int)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(pi int) {
					rf.mu.Lock()
					args := RequestVoteArgs{}
					args.CandidateId = rf.me
					args.Term = rf.currentTerm
					args.LastLogIndex = rf.LogLen() - 1
					lastLog := rf.LogAt(args.LastLogIndex)
					args.LastLogTerm = lastLog.Term
					rf.mu.Unlock()

					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(pi, &args, &reply)
					if !ok {
						voteCh <- 0
						return
					}
					// if a candidate or leader discovers that its term is out of date,
					// it immediately reverts to follower state
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						rf.ResetLastHeartBeat()
						rf.toFollower(reply.Term)
						Debug(dTerm, "S%d term=%d", rf.me, rf.currentTerm)
						Debug(dClient, "S%d become follower", rf.me)
						rf.mu.Unlock()
						voteCh <- 0
					} else if reply.VoteGranted {
						rf.mu.Unlock()
						Debug(dVote, "S%d got vote from S%d", rf.me, pi)
						voteCh <- 1
					} else {
						rf.mu.Unlock()
						voteCh <- 0
					}
				}(i)
			}

			votes := 1
			replyCount := 0
			numPeers := len(rf.peers)
			for {
				select {
				case v := <-voteCh:
					votes += v
					replyCount++
					rf.mu.Lock()
					if rf.state == Candidate && votes >= len(rf.peers)/2+1 {
						rf.toLeader()
						Debug(dClient, "S%d become leader", rf.me)
						rf.ResetLastHeartBeat() // reset election timeout
					}
					rf.mu.Unlock()
				case <-quit:
					for replyCount < numPeers-1 {
						<-voteCh
					}
					return
				}
			}
		}()
		rf.mu.Lock()
		diff = time.Since(rf.lastHeartBeat)
		rf.mu.Unlock()
		if diff < randTimeout {
			time.Sleep(randTimeout - diff)
		}
		quit <- 1
	}
}

// All: if commitIndex > lastApplied, increment lastApplied, apply
// log[lastApplied] to state machine
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			t := rf.logs[rf.lastApplied].Term
			ind := rf.lastApplied
			rf.mu.Unlock()
			rf.applyChan <- msg
			Debug(dLog, "S%d apply (%d,%d)=%v", rf.me, t, ind, msg.Command)
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// Leader: if last log index > nextIndex for a follower: send
// AppendEntry RPC with log entries starting at nextIndex
// TODO: now support only 1 entry at a time, should support multiple
func (rf *Raft) appendEntryTicker() {
	numPeers := len(rf.peers)
	doneCh := []chan int{} // make sure for each follower there is at most 1 ongoing AppendEntry RPC
	for i := 0; i < numPeers; i++ {
		doneCh = append(doneCh, make(chan int, 1))
		doneCh[i] <- 0
	}
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		lastLogIndex := len(rf.logs) - 1
		for i, fni := range rf.nextIndex {
			select {
			case <-doneCh[i]:
			default:
				continue
			}
			if i == rf.me || lastLogIndex < fni { // fni - follower's next index
				doneCh[i] <- 0
				continue
			}
			// else lastLogIndex >= fni
			args := AppendEntryArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = fni - 1
			args.PrevLogTerm = rf.logs[fni-1].Term
			args.Entries = append(args.Entries, rf.logs[fni:]...)
			args.LeaderCommit = rf.commitIndex
			reply := AppendEntryReply{}
			// send in parallel
			go func(pi int) {
				defer func() {
					doneCh[pi] <- 0
				}()
				Debug(dLog, "S%d sending ae(%d,%d) to S%d", rf.me, args.Entries[0].Term, args.PrevLogIndex+1, pi)
				ok := rf.sendAppendEntry(pi, &args, &reply)
				if !ok {
					Debug(dLog, "S%d ae(%d,%d) to S%d(BAD)", rf.me, args.Entries[0].Term, args.PrevLogIndex+1, pi)
					return
				} else {
					Debug(dLog, "S%d ae(%d,%d) to S%d(GOOD)", rf.me, args.Entries[0].Term, args.PrevLogIndex+1, pi)
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Leader {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.ResetLastHeartBeat()
					rf.toFollower(args.Term)
					Debug(dClient, "S%d become follower(ae reply)", rf.me)
					Debug(dTerm, "S%d term=%d", rf.me, rf.currentTerm)
					return
				}
				if reply.Success { // Leader: if successful, update nextIndex and matchIndex for follower
					rf.nextIndex[pi] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[pi] = max(rf.matchIndex[pi], args.PrevLogIndex+len(args.Entries))
				} else { // Leader: if fails because of log inconsistency, decrement nextIndex and retry
					if reply.XTerm == -1 {	// follower's log is too short
						rf.nextIndex[pi] = reply.XLen
					} else {
						ok, ind := rf.HasTerm(reply.XTerm)
						if !ok {	// leader doesn't have XTerm
							rf.nextIndex[pi] = reply.XIndex
						} else {	// leader has XTerm
							rf.nextIndex[pi] = ind
						}
					}
				}
				Debug(dLog, "S%d update ni=%v", rf.me, rf.nextIndex)
			}(i)
		}
		rf.mu.Unlock()
	}
}

// Leader: if there exists an N such that N > commitIndex, and a majority of matchIndex[i] >= N.
// and log[N].term == currentTerm, set commitIndex = N
func (rf *Raft) commitTicker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}

		// from the Paper 5.4.2:
		// To eliminate problems like the one in Figure 8, Raft
		// never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
		// term are committed by counting replicas; once an entry
		// from the current term has been committed in this way,
		// then all prior entries are committed indirectly because
		// of the Log Matching Property.

		// find minimum n where log[n].term == currentTerm && n > rf.commitIndex
		n := rf.commitIndex + 1
		for n < len(rf.logs) {
			if rf.logs[n].Term == rf.currentTerm {
				break
			}
			n++
		}
		if n == len(rf.logs) {
			rf.mu.Unlock()
			continue
		}
		cnt := 1 // the leader itself
		for i, mi := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if mi >= n {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			rf.commitIndex = n
			Debug(dLog, "S%d commit (%d,%d)=%v", rf.me, rf.logs[rf.commitIndex].Term, rf.commitIndex, rf.logs[rf.commitIndex].Command)
		}

		rf.mu.Unlock()
	}
}
