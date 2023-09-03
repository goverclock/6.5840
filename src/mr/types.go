package mr

import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type TaskStatus int

const (
	Unassigned TaskStatus = iota
	Assigned
	Done
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	PendingTask
	NoTask
)

type Task struct {
	Ttype      TaskType
	Fname      string // for map phase only
	Fnum       int    // for reduce phase only
	Status     TaskStatus
	AssignTime time.Time
}
