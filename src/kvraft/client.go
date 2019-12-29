package kvraft

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	<-time.After(time.Second / 2)
	return ck
}

//
// fetch the current Value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.seq++
	// You will have to modify this function.
	args := &GetArgs{Seq: ck.seq, Cid: ck.clientId, Key: key}
	start := time.Now()
	for time.Since(start) < 5*time.Hour {
		reply := new(GetReply)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Value
	}
	panic("get timeout")
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := &PutAppendArgs{Seq: ck.seq, Op: op, Cid: ck.clientId, Key: key, Value: value}

	start := time.Now()
	for time.Since(start) < 5*time.Hour {
		reply := new(PutAppendReply)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return
	}
	panic("PutAppend timeout")
	return

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
