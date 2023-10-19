package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id  int64	// client ID
	seq int64	// RPC seq number
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
	ck.id = nrand()
	ck.seq = 0

	return ck
}

func (ck *Clerk) incSeq() {
	ck.seq++
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	defer ck.incSeq()
	// retry until found leader server and succeed
	ind := 0
	ok := false
	for {
		args := GetArgs{}
		args.Key = key
		args.Id = ck.id
		args.Seq = ck.seq
		reply := GetReply{}
		ok = ck.sendGet(ind, &args, &reply)
		if ok && reply.Err == OK {
			// fmt.Printf("C:Get(%v)=%v\n", key, reply.Value)
			fmt.Printf("C:Get(%v)\n", key)
			return reply.Value
		}
		ind = (ind + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	defer ck.incSeq()
	// retry until found leader server and succeed
	ind := 0
	ok := false
	for {
		args := PutAppendArgs{}
		args.Op = op
		args.Key = key
		args.Value = value
		args.Id = ck.id
		args.Seq = ck.seq
		reply := PutAppendReply{}
		ok = ck.sendPutAppend(ind, &args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ind = (ind + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	defer fmt.Printf("C:Put(%v,%v)\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	defer fmt.Printf("C:Append(%v,%v)\n", key, value)
	ck.PutAppend(key, value, "Append")
}
