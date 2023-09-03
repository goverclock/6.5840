package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply := getWork()
		task := reply.MRTask
		switch task.Ttype {
		case PendingTask:
			dprint("worker: pending task")
			time.Sleep(time.Second)
		case NoTask:
			dprint("worker: no task, quit")
			os.Exit(0)
		case MapTask:
			dprint("worker: got map task:", task.Fname)
			kva := []KeyValue{}
			fname, content := getMapFuncArgs(task)
			kva = append(kva, mapf(fname, content)...)
			writeKva(kva, reply.Num, reply.NReduce)
			doneMap(reply.Num)
		case ReduceTask:
			dprint("worker: got reduce task:", reply)
			kva := []KeyValue{}
			for i := 0; i < reply.MRTask.Fnum; i++ {	// reduce on each file
				kva = append(kva, readReduceKv(i, reply.Num)...) 
			}
			sort.Sort(ByKey(kva))
			fname := fmt.Sprintf("mr-out-%v", reply.Num)
			ofile, _ := os.Create(fname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[i].Key == kva[j].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			doneReduce(reply.Num)
		}
	}
}

// notify coordinator the worker has finished map task
func doneMap(num int) {
	args := WorkerCallArgs{}
	args.Ct = DoneMap
	args.Num = num
	reply := WorkerCallReply{}
	go call("Coordinator.WorkerCall", &args, &reply) // dont care about reply
}

func doneReduce(num int) {
	args := WorkerCallArgs{}
	args.Ct = DoneReduce
	args.Num = num
	reply := WorkerCallReply{}
	go call("Coordinator.WorkerCall", &args, &reply) // dont care about reply
}

func writeKva(kva []KeyValue, num int, nReduce int) {
	// create nReduce temp files first
	tmpfnames := []string{}
	encoders := []*json.Encoder{}
	for i := 0; i < nReduce; i++ {
		tmpf, err := os.CreateTemp(".", "mrtemp-")
		if err != nil {
			log.Fatal("fail to create temp file", err)
		}
		defer tmpf.Close()
		tmpfnames = append(tmpfnames, tmpf.Name())
		encoders = append(encoders, json.NewEncoder(tmpf))
	}

	// write kva to temp files, distributed by ihash()
	for _, kv := range kva {
		err := encoders[ihash(kv.Key)%nReduce].Encode(&kv)
		if err != nil {
			log.Fatal("fail to encode", err)
		}
	}

	// rename temp file to real name
	os.Mkdir("./map-result", os.ModePerm) // ignore error when dir already exists
	for i := 0; i < nReduce; i++ {
		tmpname := tmpfnames[i]
		realname := fmt.Sprintf("./map-result/mr-%v-%v", num, i)
		err := os.Rename(tmpname, realname)
		if err != nil {
			log.Println("fail to rename", tmpname, "to", realname, err) // no need to fatal
			os.Remove(tmpname)
		}
	}
}

func readReduceKv(filenum int, n int) (kva []KeyValue) {
	fname := fmt.Sprintf("./map-result/mr-%v-%v", filenum, n)
	f, err := os.Open(fname)
	if err != nil {
		log.Fatal("fail to open", fname, err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return
}

func getMapFuncArgs(t Task) (fname string, content string) {
	f, err := os.Open(t.Fname)
	if err != nil {
		log.Fatal("fail to open", t.Fname)
	}
	defer f.Close()
	fbytes, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("fail to read", t.Fname)
	}
	return t.Fname, string(fbytes)
}

func getWork() WorkerCallReply {
	args := WorkerCallArgs{}
	args.Ct = WantWork
	reply := WorkerCallReply{}
	args.Num = -1

	ok := call("Coordinator.WorkerCall", &args, &reply)
	if ok {
		return reply
	}
	// coordinator not replying, consider it finished
	dprint("worker: coordinator not replying, quit")
	os.Exit(0)
	return WorkerCallReply{}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
