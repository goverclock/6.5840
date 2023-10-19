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
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
	if kv.duplicate[op.Id] == nil {
		kv.duplicate[op.Id] = make(map[int64]string)
	}
	_, ok := kv.duplicate[op.Id][op.Seq]
	if ok {
		return
	}

	// else start an agreement
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// execute the command
	msg := <-kv.applyCh
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

	// after the execution, record reply content in duplicate table
	kv.duplicate[op.Id][op.Seq] = ""	// Put or Append doesn't has a reply value
}

func (kv *KVServer) GetHandler(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
	if kv.duplicate[op.Id] == nil {
		kv.duplicate[op.Id] = make(map[int64]string)
	}
	rep, ok := kv.duplicate[op.Id][op.Seq]
	if ok {
		reply.Value = rep
		return
	}

	// start agreement
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// execute the command
	msg := <-kv.applyCh
	cmd := msg.Command.(Op)
	if cmd.Type != OpGet {
		panic("args Op doesn't match cmd")
	}
	if cmd.Key != args.Key {
		panic("args key doesn't match cmd")
	}
	reply.Value = kv.db[cmd.Key]
	// Debug(dClient, "S%d Get(%v)=%v", kv.me, cmd.Key, reply.Value)

	// after the execution, record reply content in duplicate table
	kv.duplicate[op.Id][op.Seq] = reply.Value
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
