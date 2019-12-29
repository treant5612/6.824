package raft

import (
	"sync"
	"sync/atomic"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

var (
	RequestVoteCounts     int32 = 0
	AppendEntriesCounts   int32
	AppendEntriesFailed   int32
	Once                  sync.Once
	StartsCounts          int32
	BroadcastAppendCounts int32
)
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	//defer log.Printf("%+v,%+v,rf.me:%v",args,reply,rf.me)
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		index, term := rf.getLastLogInfo()
		// candidate's log is at least as up-to-date as receiver's log
		// bigger Term || same Term ,more logs
		if term < args.LastLogTerm ||
			(term == args.LastLogTerm && index <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.resetTimer()
			rf.persist()
			return
		}
	}
	reply.VoteGranted = false
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	atomic.AddInt32(&RequestVoteCounts, 1)

	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	defer func() {
		//log.Println(rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.logLength(), rf.logs)
		//log.Printf("%+v,%+v\n", args, reply)
	}()
	//1.Reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset timer when receiving AppendEntries from current leader
	rf.resetTimer()

	// update currentTerm
	rf.updateTerm(args.Term)
	//if rf.state == Candidate && args.Term == rf.currentTerm {
	//	rf.state = Follower
	//}

	// 2.Reply false if log doesnt contain an entry at PrevLogIndex
	// whose Term matches prevLogTerm

	// conflictIndex & conflictTerm
	// for more details, see: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
	logEntry := rf.getLogEntry(args.PrevLogIndex)
	if logEntry == nil {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.logLength()
		reply.ConflictTerm = 0
		return
	}
	if logEntry.Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = logEntry.Term
		reply.ConflictIndex = rf.firstIndexOfTerm(logEntry.Term)
		return
	}

	// 3.If an existing entry conflicts with new one...
	// 4.Append any new entries not already in the log
	rf.addEntries(args.PrevLogIndex, args.Entries)

	// 5.If leaderCommit > commitIndex ,set commitIndex = min(leaderCommit,index of lastNewEntry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntry)
		rf.apply()
	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	atomic.AddInt32(&AppendEntriesCounts, 1)
	if !ok {
		atomic.AddInt32(&AppendEntriesFailed, 1)

	}
	return ok
}
