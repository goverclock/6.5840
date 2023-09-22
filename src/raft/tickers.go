package raft

import (
	"math/rand"
	"time"
)

// send heartbeat(empty AppendEntries) when the server is leader
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		if rf.State() != Leader {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(pi int) {
				if rf.State() != Leader {
					return
				}
				args := AppendEntryArgs{}
				args.Term = rf.CurrentTerm()
				args.LeaderId = rf.Me()
				args.LeaderCommit = rf.CommitIndex()
				args.PrevLogIndex = args.LeaderCommit
				reply := AppendEntryReply{}
				ok := rf.sendAppendEntry(pi, &args, &reply)
				if ok {
					Debug(dLeader, "S%d hb to S%d(OK)", rf.Me(), pi)
				} else {
					Debug(dLeader, "S%d hb to S%d(FAIL)", rf.Me(), pi)
				}
				// if leader(or candidate) discovers higher term, become follower
				if reply.Term > rf.CurrentTerm() {
					Debug(dLeader, "S%d found term %d, step down", rf.Me(), reply.Term)
					rf.toFollower(reply.Term)
					rf.ResetLastHeartBeat()
				}
			}(i)
		}
		time.Sleep(120 * time.Millisecond) // heart beat interval
	}
}

func (rf *Raft) electTicker() {
	randTimeout := time.Duration(700+(rand.Int()%300)) * time.Millisecond
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// 600 ~ 900ms election timeout
		diff := time.Since(rf.LastHeartBeat())
		if diff < randTimeout {
			time.Sleep(randTimeout - diff)
			continue
		}
		randTimeout = time.Duration(600+(rand.Int()%300)) * time.Millisecond

		if rf.State() == Leader {
			continue
		}

		// election timeout
		rf.ResetLastHeartBeat() // reset election timeout
		// become candidate, init election
		rf.toCandidate()
		Debug(dClient, "S%d become candidate", rf.Me())
		Debug(dLeader, "S%d init election", rf.Me())
		Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
		// when election timeout again during election,
		// quit current and start new election
		quit := make(chan int)
		go func() {
			// send RequestVoteRPCs to all other servers
			voteCh := make(chan int)
			for i := range rf.peers {
				if i == rf.Me() {
					continue
				}
				go func(pi int) {
					args := RequestVoteArgs{}
					args.CandidateId = rf.Me()
					args.Term = rf.CurrentTerm()
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(pi, &args, &reply)
					if !ok {
						return
					}
					// if a candidate or leader discovers that its term is out of date,
					// it immediately reverts to follower state
					if rf.CurrentTerm() < reply.Term {
						rf.toFollower(reply.Term)
						Debug(dTerm, "S%d term=%d", rf.Me(), rf.CurrentTerm())
						Debug(dClient, "S%d become follower", rf.Me())
						rf.ResetLastHeartBeat()
					} else if reply.VoteGranted {
						Debug(dVote, "S%d got vote from S%d", rf.Me(), pi)
						voteCh <- 1
					}
				}(i)
			}

			votes := 1
			for {
				select {
				case <-voteCh:
					votes++
					if rf.State() == Candidate && votes >= len(rf.peers)/2+1 {
						rf.SetState(Leader)
						Debug(dClient, "S%d become leader", rf.Me())
						rf.ResetLastHeartBeat() // reset election timeout
					}
				case <-quit:
					return
				}
			}
		}()
		diff = time.Since(rf.LastHeartBeat())
		if diff < randTimeout {
			time.Sleep(randTimeout - diff)
		}
		quit <- 1
	}
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.commitIndex,
			}
			rf.mu.Unlock()
			rf.applyChan <- msg
			Debug(dLog, "S%d sent %v to applyCh", rf.me, msg)
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}
