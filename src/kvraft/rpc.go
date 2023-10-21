package kvraft

import "time"

type OpType int

const (
	OpPut OpType = iota
	OpAppend
	OpGet
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string

	Id  int64
	Seq int64
}

// Put or Append
type PutAppendArgs struct {
	Op    string // "Put" or "Append"
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64
	Seq int64
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id  int64
	Seq int64
}

type GetReply struct {
	Err   string
	Value string
}

func (kv *KVServer) PutAppendHandler(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = OK
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	// convert args to operation to send to raft
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Id:    args.Id,
		Seq:   args.Seq,
	}
	if args.Op == "Put" {
		op.Type = OpPut
	} else if args.Op == "Append" {
		op.Type = OpAppend
	} else {
		panic("unknown op")
	}

	// check duplicate
	kv.mu.Lock()
	de, ok := kv.duplicate[op.Id]
	if ok && de.seq >= args.Seq {
		if de.seq > args.Seq {
			// should not happen, since lab assumes that a client will
			// make only one call into a Clerk at a time.
			panic("fuck")
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// else start an agreement
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// when the command is executed, the result appears in the duplicate table
	for {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		de, ok := kv.duplicate[op.Id]
		if ok && de.seq == args.Seq {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) GetHandler(args *GetArgs, reply *GetReply) {
	reply.Err = OK
	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	op := Op{
		Type: OpGet,
		Key:  args.Key,
		Id:   args.Id,
		Seq:  args.Seq,
	}

	// check duplicate
	kv.mu.Lock()
	de, ok := kv.duplicate[op.Id]
	if ok && de.seq >= args.Seq {
		if de.seq > args.Seq {
			panic("fuck")
		}
		reply.Value = de.rep
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// start agreement
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		time.Sleep(5 * time.Millisecond)
		kv.mu.Lock()
		de, ok := kv.duplicate[op.Id]
		if ok && de.seq == args.Seq {
			reply.Value = de.rep
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppendHandler", args, reply)
	// if ok {
	// 	fmt.Printf("C:sentPA(%v,%v)(GOOD)\n", args.Key, args.Value)
	// } else {
	// 	fmt.Printf("C:sentPA(%v,%v)(BAD)\n", args.Key, args.Value)
	// }
	return ok
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.GetHandler", args, reply)
	// if ok {
	// 	fmt.Printf("C:sentGet(%v)(GOOD)\n", args.Key)
	// } else {
	// 	fmt.Printf("C:sentGet(%v)(BAD)\n", args.Key)
	// }
	return ok
}
