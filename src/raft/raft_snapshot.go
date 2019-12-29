package raft

import "time"

func (rf *Raft) Snapshot(index int, data []byte) {
	rf.Lock()
	defer rf.Unlock()
	//defer log.Println(rf.persister.RaftStateSize())
	if index <= rf.lastIncludedIndex {
		return
	}
	// release reference
	rf.logs = append([]*Entry{}, rf.logs[rf.convertIndex(index):]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	rf.persistWithSnapshot(data)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.resetTimer()
	defer rf.persist()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	cutPoint := rf.convertIndex(args.LastIncludedIndex)
	rf.lastIncludedIndex = args.LastIncludedIndex
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)

	if cutPoint < len(rf.logs) {
		rf.logs = rf.logs[cutPoint:]
	} else {
		rf.logs = []*Entry{{args.LastIncludedTerm, nil}}
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
	if rf.commitIndex > oldCommitIndex {
		go func() {
			select {
			case rf.applyCh <- ApplyMsg{CommandValid: false}:
			case <-time.After(10*time.Second):
				panic("....")

			}
		}()
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshotTo(server int) {
	if rf.state != Leader {
		return
	}

	args := &InstallSnapshotArgs{
		LeaderId: rf.me,
		Term:     rf.currentTerm,

		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.getLogEntry(rf.lastIncludedIndex).Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := new(InstallSnapshotReply)
	go func() {
		if rf.sendInstallSnapshot(server, args, reply) {
			rf.mu.Lock()
			if rf.updateTerm(reply.Term) {

			} else {
				rf.matchIndex[server] = max(rf.matchIndex[server], rf.lastIncludedIndex)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			rf.mu.Unlock()
		}
	}()
}

/*
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()

	//1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTimer()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	// skip 2~4 for entire snapshot data

	// 5.discard snapshot with smaller index
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else {
		rf.saveStateSnapshot(args.Data)
	}

	r := bytes.NewBuffer(args.Data)
	decoder := labgob.NewDecoder(r)
	var table map[int64]*Result
	var kvdb Db
	decoder.Decode(&table)
	decoder.Decode(&kvdb)
	//log.Println(rf,"leader",args.LeaderId, kvdb)

	// 6. if existing log entry has same index and term as
	//	snapshot's last included entry, retain log entries following
	//	and reply
	entry := rf.getLogEntry(args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	if entry != nil && entry.Term == args.LastIncludedTerm {
		rf.logs = rf.logs[rf.convertIndex(args.LastIncludedIndex):]
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persist()
		return
	}
	// 7.Discard the entire log
	rf.logs = []*Entry{{Term: args.LastIncludedTerm}}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
	// 8.Reset state machine using snapshot contents
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.resetStateMachine()

}


func (rf *Raft) SaveSnapshot(data []byte, lastIncluded int) {
	rf.Lock()
	defer rf.Unlock()
	//	n := strconv.FormatInt(rand.Int63(), 16)
	if lastIncluded <= rf.lastIncludedIndex {
		return
	}

	lastLog := rf.getLogEntry(lastIncluded)
	rf.logs = rf.logs[rf.convertIndex(lastIncluded):]
	rf.lastIncludedIndex = lastIncluded
	rf.lastIncludedTerm = lastLog.Term
	rf.saveStateSnapshot(data)
}

func (rf *Raft) saveStateSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}

func (rf *Raft) resetStateMachine() {
	msg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
	}
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshotTo(server int) {
	args := &InstallSnapshotArgs{
		LeaderId:          rf.me,
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := new(InstallSnapshotReply)
	go func() {
		ok := rf.sendInstallSnapshot(server, args, reply)
		if ok {
			//	log.Println("install snapshot ",args.LastIncludedIndex)
			rf.Lock()
			defer rf.Unlock()

			// delayed
			if reply.Term < rf.currentTerm || rf.state!=Leader{
				return
			}
			if rf.updateTerm(reply.Term) {
				return
			}
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}()
}




//--------------delete
type Result struct {
	Seq   int64
	Value string
}

func (r *Result) String() string {
	return fmt.Sprintf("%v:%v", r.Seq, r.Value)
}

type Db map[string]string
func (db Db)String()string{
	str :="[ \n"
	for key,value :=range db{
		str+=fmt.Sprintf("key :\t%v\tvalue:%v\n",key,value)

	}
	str+="\n  ]"
	return str
}


*/
