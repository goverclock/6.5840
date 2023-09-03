package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MRPhase int

const (
	MapPhase MRPhase = iota
	ReducePhase
	Finished
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []Task
	reduceTasks []Task
	phase       MRPhase
	nReduce     int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) WorkerCall(args *WorkerCallArgs, reply *WorkerCallReply) error {
	// update task status first
	c.updateTaskStatus()

	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Ct {
	case WantWork:
		dprint("coor: worker want work")
		// find an unassigned map/reduce task
		pendingTask := Task{}
		pendingTask.Ttype = PendingTask
		reply.MRTask = pendingTask
		reply.NReduce = c.nReduce
		if c.phase == MapPhase {
			for i, mt := range c.mapTasks {
				if mt.Status == Unassigned { // give map task
					reply.Num = i
					reply.MRTask = mt
					c.mapTasks[i].Status = Assigned
					c.mapTasks[i].AssignTime = time.Now()
					break
				}
			}
		} else if c.phase == ReducePhase {
			for i, rt := range c.reduceTasks {
				if rt.Status == Unassigned { // give reduce task
					reply.Num = i
					reply.MRTask = rt
					c.reduceTasks[i].Status = Assigned
					c.reduceTasks[i].AssignTime = time.Now()
					break
				}
			}
		} else { // c.phase == Finished, give no task
			t := Task{}
			t.Ttype = NoTask
			reply.Num = -1
			reply.MRTask = t
		}
	case DoneMap:
		dprint("coor: worker done map")
		c.mapTasks[args.Num].Status = Done
	case DoneReduce:
		dprint("coor: worker done reduce")
		c.reduceTasks[args.Num].Status = Done
	}

	return nil
}

func (c *Coordinator) updateTaskStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapPhase {
		done := 0
		for i, mt := range c.mapTasks {
			if mt.Status == Done {
				done++
			} else if mt.Status == Assigned && time.Since(mt.AssignTime) > 10*time.Second {
				c.mapTasks[i].Status = Unassigned
			}
		}
		if done == len(c.mapTasks) {
			dprint("coor: all map done, into reduce phase")
			c.phase = ReducePhase
		}
	}
	if c.phase == ReducePhase {
		done := 0
		for i, rt := range c.reduceTasks {
			if rt.Status == Done {
				done++
			} else if rt.Status == Assigned && time.Since(rt.AssignTime) > 10*time.Second {
				c.reduceTasks[i].Status = Unassigned
			}
		}
		if done == len(c.reduceTasks) {
			dprint("coor: all reduce done, into finished phase")
			c.phase = Finished
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.phase == Finished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, fn := range files { // init map tasks
		t := Task{}
		t.Ttype = MapTask
		t.Fname = fn
		t.Fnum = -1
		t.Status = Unassigned
		c.mapTasks = append(c.mapTasks, t)
		dprint(t)
	}
	for i := 0; i < nReduce; i++ { // init reduce tasks
		t := Task{}
		t.Ttype = ReduceTask
		t.Fname = ""
		t.Fnum = len(files)
		t.Status = Unassigned
		c.reduceTasks = append(c.reduceTasks, t)
	}
	c.phase = MapPhase
	c.nReduce = nReduce

	c.server()
	return &c
}
