package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetTaskError string

const (
	eWait GetTaskError = "WAIT"
	eDone GetTaskError = "DONE"
	eErr  GetTaskError = "ERROR"
	eNone GetTaskError = ""
)

type GetTaskArgs struct {
	// none
}

type GetTaskReply struct {
	Error      GetTaskError
	TaskType   string
	TaskID     string
	InputFiles []string
	NReduce    int
}

type TaskFinishedArgs struct {
	TaskType    string
	TaskID      string
	OutputFiles []string
}

type TaskFinishedReply struct {
	// none
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
