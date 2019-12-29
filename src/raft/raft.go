package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type Entry struct {
	Term    int
	Command interface{}
}

func (e Entry) String() string {
	return fmt.Sprintf("%d:%v", e.Term, e.Command)
}
func (rf *Raft) Lock() {
	rf.mu.Lock()
	rf.lockLocation = fmt.Sprint(runtime.Caller(1)) + strconv.FormatInt(rand.Int63n(1<<31), 16)
	//log.Println("Lock", rf.lockLocation)

}
func (rf *Raft) Unlock() {
	rf.lockLocation = "unlock" + fmt.Sprint(runtime.Caller(1))

	//log.Println("unlock", rf.lockLocation)
	rf.mu.Unlock()
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       state
	currentTerm int
	votedFor    int
	logs        []*Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh         chan ApplyMsg
	timer           *time.Timer
	heartBeatsTimer *time.Timer
	lockLocation    string

	lastIncludedIndex int
	lastIncludedTerm  int
	snapShot          interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()

	var term int = rf.currentTerm
	var isleader bool = rf.state == Leader
	// Your code here (2A).

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
	//	rf.persister.SaveRaftState(data)
}
func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	data := w.Bytes()
	return data
}
func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}

//
// restore previously persisted state.
//
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
	var currentTerm int
	var votedFor int
	var log []*Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("decode error")
	}

	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		panic("decode lastIncludeIndex/Term error")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

}

func (rf *Raft) electionWatcher() {

	for range rf.timer.C {
		if rf.killed() {
			return
		}
		rf.Lock()
		switch rf.state {
		case Leader:
			rf.resetTimer()
		case Follower, Candidate:
			rf.startElection()
		}
		rf.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++ //safe <- under Lock
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	rf.resetTimer()
	replyCh := make(chan *RequestVoteReply, len(rf.peers))

	rf.broadcastRequestVotes(replyCh)
	rf.handleReqeustVoteReply(replyCh)

}

func (rf *Raft) broadcastRequestVotes(replyCh chan *RequestVoteReply) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := new(RequestVoteArgs)
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex, args.LastLogTerm = rf.getLastLogInfo()

		go func(id int) {
			for i := 0; i < 1; i++ {
				reply := new(RequestVoteReply)
				ok := rf.sendRequestVote(id, args, reply)
				if ok {
					timeout := time.After(time.Second/10)
					select {
					case replyCh <- reply:
					case <-timeout:
					}
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) handleReqeustVoteReply(replyChan chan *RequestVoteReply) {
	votes := 1
	timeout := time.After(time.Second)
	go func() {
		for i := 0; i < len(rf.peers)-1; i++ {
			var reply *RequestVoteReply
			select {
			case reply = <-replyChan:
			case <-timeout:
				return
			}
			rf.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			//  stale candidate'Term
			if rf.updateTerm(reply.Term) {
				rf.Unlock()
				return
			}

			// stale reply
			if rf.currentTerm != reply.Term {
				rf.Unlock()
				return
			}

			// correct
			if rf.currentTerm == reply.Term && reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					go rf.beLeader(reply.Term)
					rf.Unlock()
					return
				}
			}
			rf.Unlock()

		}
	}()
}

func (rf *Raft) beLeader(term int) {
	rf.Lock()
	defer rf.Unlock()
	if rf.currentTerm != term {
		return
	}
	rf.state = Leader
	rf.initNextIndex()
	go rf.heartBeats()
}
func (rf *Raft) initNextIndex() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logLength()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) heartBeats() {
	for !rf.killed() {
		rf.Lock()
		rf.heartBeatsTimer.Reset(HeartBeatCycle)
		isLeader := rf.state == Leader
		rf.Unlock()
		if !isLeader {
			return
		}
		rf.broadcastAppendEntries()
		<-rf.heartBeatsTimer.C
	}
}

func (rf *Raft) broadcastAppendEntries() {
	atomic.AddInt32(&BroadcastAppendCounts, 1)
	rf.Lock()
	if rf.state != Leader {
		rf.Unlock()
		return
	}
	//rf.heartBeatsTimer.Reset(HeartBeatCycle * time.Millisecond)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesTo(i, rf.currentTerm)
	}

	rf.Unlock()

}

func (rf *Raft) sendAppendEntriesTo(id int, term int) {
	start := time.Now()
	for time.Since(start) < time.Second/2 && !rf.killed() {
		rf.Lock()
		if rf.currentTerm != term || rf.state != Leader {
			rf.Unlock()
			return
		}
		if rf.nextIndex[id] <= rf.lastIncludedIndex {
			rf.sendSnapshotTo(id)
			rf.Unlock()
			return
		}

		args := new(AppendEntriesArgs)
		args.Term = term
		args.PrevLogIndex = rf.nextIndex[id] - 1
		args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
		args.Entries = append(args.Entries, rf.logTail(args.PrevLogIndex+1)...)

		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me

		rf.Unlock()
		reply := new(AppendEntriesReply)
		ok := rf.sendAppendEntries(id, args, reply)
		if ok {
			if rf.handleAppendReply(id, args, reply) {
				return
			}
		} else {
			<-time.After(300 * time.Millisecond)
		}
	}
}

func (rf *Raft) handleAppendReply(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) (done bool) {
	rf.Lock()
	defer rf.Unlock()
	// stale leader
	if rf.updateTerm(reply.Term) {
		return true
	}
	// delayed reply
	if reply.Term < rf.currentTerm {
		return true
	}
	if reply.Success && reply.Term == rf.currentTerm {
		rf.matchIndex[id] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[id])
		rf.nextIndex[id] = rf.matchIndex[id] + 1
		rf.updateCommitIndex()
		return true
	}

	if !reply.Success {
		if rollBackIndex := rf.lastIndexOfTerm(reply.ConflictTerm); rollBackIndex > 0 {
			rf.nextIndex[id] = rollBackIndex
		} else {
			rf.nextIndex[id] = max(1, reply.ConflictIndex)
		}
		return false
	}
	return false
}

func (rf *Raft) apply() {
	go func() {
		rf.Lock()
		defer rf.Unlock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				continue
			}
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.getLogEntry(rf.lastApplied).Command,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}
	var matches = make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)

	//	if there exists an N such that N > commitIndex,
	//	a majority of matchIndex[i] >= N,
	// 	and log[N].term == currentTerm:
	//	set commitIndex = N  => Figure8
	N := matches[len(matches)/2+1]
	if N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.apply()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()
	//log.Println("start ", command)
	//defer log.Println("start return", command)
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		rf.Unlock()
		return index, term, isLeader
	}
	index = rf.logLength()
	term = rf.currentTerm
	newEntry := &Entry{Term: term, Command: command}
	//rf.log = append(rf.log, newEntry)
	rf.appendLog(newEntry)
	rf.persist()
	rf.heartBeatsTimer.Reset(0)
	// Your code here (2B).
	rf.Unlock()
	//go rf.broadcastAppendEntries()
	atomic.AddInt32(&StartsCounts, 1)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a Lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//Once.Do(func() {
	//	log.Println(
	//		atomic.LoadInt32(&AppendEntriesCounts),
	//		atomic.LoadInt32(&AppendEntriesFailed),
	//		atomic.LoadInt32(&StartsCounts),
	//		atomic.LoadInt32(&BroadcastAppendCounts),
	//
	//	)
	//})
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.logs = []*Entry{{0, nil}}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timer = time.NewTimer(randomTimeout())
	rf.heartBeatsTimer = time.NewTimer(0)
	go rf.electionWatcher()
	go func() {
		for range time.Tick(time.Second / 2) {
			if rf.killed() {
				return
			}
			//log.Println(rf, rf.lockLocation, "\t", rf.persister.RaftStateSize())
		}
	}()
	return rf
}

func (rf *Raft) String() string {
	str := fmt.Sprintf("me:%v, state%v, term%v ,logLen%v,commited%v,applied%v, lastIncluded%v", rf.me, rf.state, rf.currentTerm, rf.logLength(), rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex)
	return str
}
