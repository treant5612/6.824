package raft

import (
	"log"
	"math/rand"
	"time"
)

type state int

const (
	Follower state = iota
	Candidate
	Leader
)
const (
	ElectionTimeout = 400
	HeartBeatCycle  = 150 * time.Millisecond
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime)
}

//if RPC request or response contains Term T > currentTerm ,set currentTerm = T ,convert to follower
func (rf *Raft) updateTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = Follower
		rf.resetTimer()
		rf.persist()

		return true
	}
	return false
}

func (rf *Raft) getLastLogInfo() (index int, term int) {
	index = rf.logLength() - 1
	term = rf.getLogEntry(index).Term // rf.log[index].Term
	return
}

func (rf *Raft) addEntries(prevlogIndex int, entries []*Entry) {
	start := prevlogIndex + 1
	for i := range entries {
		entry := rf.getLogEntry(i + start)
		if entry == nil || entry.Term != entries[i].Term {
			//rf.log = rf.log[:start+i]
			rf.cutLog(start + i)
			//rf.log = append(rf.log, entries[i:]...)
			rf.appendLogs(entries[i:])
			return
		}
	}

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// return a time.duration between [T,2T)
func randomTimeout() time.Duration {
	T := ElectionTimeout
	return time.Duration(T+rand.Intn(T)) * time.Millisecond

}
func (rf *Raft) resetTimer() {
	rf.timer.Reset(randomTimeout())
}
func (rf *Raft) firstIndexOfTerm(term int) (index int) {
	for i := rf.lastIncludedIndex ; i < rf.logLength(); i++ {
		if rf.getLogEntry(i).Term == term {
			return i
		}
	}
	return 1
}
func (rf *Raft) lastIndexOfTerm(term int) (index int) {
	for i := rf.logLength() - 1; i >= rf.lastIncludedIndex; i-- {
		if rf.getLogEntry(i).Term == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) logLength() int {
	return len(rf.logs) + rf.lastIncludedIndex
}
func (rf *Raft) logTail(start int) []*Entry {
	return rf.logs[rf.convertIndex(start):]
}
func (rf *Raft) appendLog(e *Entry) {
	rf.logs = append(rf.logs, e)
}
func (rf *Raft) appendLogs(entries []*Entry) {
	rf.logs = append(rf.logs, entries...)
}
func (rf *Raft) getLogEntry(index int) *Entry {
	if rf.logLength() <= index || rf.lastIncludedIndex>index {
		return nil
	}
	entry := rf.logs[rf.convertIndex(index)]
	return entry
}
func (rf *Raft) cutLog(end int) {
	realEnd := rf.convertIndex(end)
	rf.logs = rf.logs[:realEnd]
}

func (rf *Raft) convertIndex(index int) (realIndex int) {
	return index - rf.lastIncludedIndex
}
