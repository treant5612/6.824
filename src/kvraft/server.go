package kvraft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"raft"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid   int64
	Seq   int64
	Op    string
	Key   string
	Value string
}
type Result struct {
	Seq   int64
	Value string
}

func( kv *KVServer)Lock(){
	kv.mu.Lock()
	kv.lockLocation = fmt.Sprint(runtime.Caller(1))+ strconv.FormatUint(uint64(rand.Uint32()),16)

}

func (kv *KVServer)Unlock(){
	kv.lockLocation = "unlock" + fmt.Sprint(runtime.Caller(1))

	kv.mu.Unlock()
}
func (r *Result) String() string {
	return fmt.Sprintf("%v:%v", r.Seq, r.Value)
}

type Db map[string]string

func (db Db) String() string {
	str := "[ \n"
	for key, value := range db {
		str += fmt.Sprintf("key :\t%v\tvalue:%v\n", key, value)

	}
	str += "\n  ]"
	return str
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       Db
	dupTable *DupTable

	lastApplied int

	lockLocation string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Cid: args.Cid,
		Op:  "Get",
		Seq: args.Seq,
		Key: args.Key,
	}
	_, _, ok := kv.execute(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	result, err := kv.getResult(op)
	if err != nil {
		reply.Err = "OperationError"
		return
	}
	reply.Err = ""
	reply.Value = result.Value
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		//	log.Println(kv.me,args,reply)

	}()
	op := Op{
		Cid:   args.Cid,
		Op:    args.Op,
		Seq:   args.Seq,
		Key:   args.Key,
		Value: args.Value,
	}
	_, _, ok := kv.execute(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	_, err := kv.getResult(op)
	if err != nil {
		reply.Err = Err(err.Error())
		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) execute(op Op) (int, int, bool) {
	index, term, isLeader := kv.rf.Start(op)
	return index, term, isLeader
}

func (kv *KVServer) applyWatcher() {
	for !kv.killed() {
		msg := <-kv.applyCh
		//>snapshot
		kv.Lock()
		if !msg.CommandValid {
			kv.readSnapshot()
			kv.Unlock()
			continue
		}
		//<snapshot
		op := msg.Command.(Op)
		kv.lastApplied = msg.CommandIndex
		if kv.dupTable.opStale(op) {
			kv.Unlock()
			continue
		}
		result := &Result{Seq: op.Seq}
		switch op.Op {
		case "Get":
			result.Value = kv.db[op.Key]
		case "Put":
			kv.db[op.Key] = op.Value
		case "Append":
			kv.db[op.Key] += op.Value
		}
		//	log.Println(strings.Repeat("-",50),"\n",kv.me,kv.db,"\n\n\n\n\n ")
		kv.dupTable.setResult(op.Cid, result)
		kv.DoLogCompaction(msg.CommandIndex)

		kv.Unlock()
	}
}

func (kv *KVServer) getResult(op Op) (*Result, error) {
	return kv.dupTable.getResult(op.Cid, op.Seq)
}

func (kv *KVServer) snapshotBytes() []byte {
	DPrintf("snapshots")
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	table := kv.dupTable.table
	encoder.Encode(table)
	encoder.Encode(kv.db)
	return w.Bytes()
}
func (kv *KVServer) readSnapshot() {

	data := kv.persister.ReadSnapshot()
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var table map[int64]*Result
	var kvdb Db
	if nil != decoder.Decode(&table) || nil != decoder.Decode(&kvdb) {
		panic("read snapshot error")
	}
	kv.dupTable.restoreDupTable(table)
	kv.db = kvdb
	//log.Println(kv.me,fmt.Sprint(kv.rf.String())+"readsnapshot \n",kv.db)
}

func (kv *KVServer) DoLogCompaction(index int) {
	// no compaction
	if kv.maxraftstate < 1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate/2 {
		go kv.rf.Snapshot(index, kv.snapshotBytes())
	}
}

var t int32

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	if maxraftstate <= 0 {
		//maxraftstate = 5
	}
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dupTable = newDupTable()
	kv.db = make(map[string]string)
	kv.readSnapshot()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.applyWatcher()
	go func(){
		for range time.Tick(time.Second/2){
			if kv.killed(){
				return
			}
		//	log.Println(kv.me,kv.lockLocation)
		}
	}()
	return kv
}
