package kvraft

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
}

// Put or Append
type PutAppendArgs struct {
	Op    string // "Put" or "Append"
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   string
	Value string
}

func (kv *KVServer) PutAppendHandler(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	// convert args to operation to send to raft
	op := Op {
		Key: args.Key,
		Value: args.Value,
	}
	if args.Op == "Put" {
		op.Type = OpPut
	} else if args.Op == "Append" {
		op.Type = OpAppend
	} else {
		panic("unknown op")
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// TODO: the operation may fail, thus not sent to applyCh
	msg := <-kv.applyCh
	reply.Err = OK
	cmd := msg.Command.(Op)
	if cmd.Key != args.Key || cmd.Value != args.Value {
		panic("args key/value doesn't match cmd")
	}
	if args.Op == "Put" && cmd.Type == OpPut {
		kv.db[cmd.Key] = cmd.Value
		// Debug(dClient, "S%d Put(%v,%v)", kv.me, cmd.Key, cmd.Value)
	} else if args.Op == "Append" && cmd.Type == OpAppend {
		kv.db[cmd.Key] += cmd.Value
		// Debug(dClient, "S%d Append(%v,%v)", kv.me, cmd.Key, cmd.Value)
	} else {
		panic("args Op doesn't match cmd")
	}
}

func (kv *KVServer) GetHandler(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	op := Op {
		Type: OpGet,
		Key: args.Key,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// TODO: the operation may fail, thus not sent to applyCh
	msg := <-kv.applyCh
	reply.Err = OK
	cmd := msg.Command.(Op)
	if cmd.Type != OpGet {
		panic("args Op doesn't match cmd")
	}
	if cmd.Key != args.Key {
		panic("args key doesn't match cmd")
	}
	reply.Value = kv.db[cmd.Key]
	// Debug(dClient, "S%d Get(%v)=%v", kv.me, cmd.Key, reply.Value)
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppendHandler", args, reply)
	return ok
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.GetHandler", args, reply)
	return ok
}
