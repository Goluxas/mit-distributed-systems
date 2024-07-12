package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type InputStatus string

/*
  ready		= file waiting for a task
  started	= file is currently being mapped or reduced
  done		= file has been processed
*/

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	inputFiles     []string
	nReduce        int
	mapStatus      map[string]InputStatus
	mapAssignments []string
	mapNextId      int
	mapDone        bool
	reduceStatus   []InputStatus
	reduceNextId   int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// iterate over mapDone and return the first unstarted map task
	if !c.mapDone {
		for filename, status := range c.mapStatus {
			if status == "ready" {
				// Coordinator tracking
				c.mapStatus[filename] = "started"
				c.mapAssignments[c.mapNextId] = filename

				// Reply for RPC
				reply.TaskType = "MAP"
				reply.TaskID = strconv.Itoa(c.mapNextId)
				c.mapNextId++
				reply.InputFiles = []string{filename}
				reply.NReduce = c.nReduce
				reply.Error = eNone

				return nil
			}
		}
	}

	// if no map tasks are left to assign, check if mapping is done
	c.checkMapTasks()

	// if mapping is done, assign a reduce task
	if c.mapDone {
		// TODO

		// TEMPORARY
	} else {
		// Reduce tasks are coming, please wait
		reply.Error = eWait
	}

	reply.Error = eDone
	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskId, err := strconv.Atoi(args.TaskID)
	if err != nil {
		log.Fatal("Invalid task ID in TaskFinished: ", args.TaskID, " -- ", err)
	}

	switch {
	case args.TaskType == "MAP":
		c.mapStatus[c.mapAssignments[taskId]] = "done"
		c.checkMapTasks()

	case args.TaskType == "REDUCE":
		c.reduceStatus[taskId] = "done"
	}

	return nil
}

func (c *Coordinator) checkMapTasks() {
	mapDone := true
	for _, status := range c.mapStatus {
		if status != "done" {
			mapDone = false
		}
	}

	c.mapDone = mapDone
	/*
		// TEMPORARY
		if c.mapDone {
			println("Mapping complete!")
		}
	*/
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	// Your code here.

	// TEMPORARY
	ret = c.mapDone

	/*
		// ACTUAL -- if mapping is done and reducing is done then we're all done
		if c.mapDone {
			ret = true
			for _, status := range c.reduceStatus {
				if status != "done" {
					ret = false
				}
			}
		}
	*/

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce

	c.mapStatus = make(map[string]InputStatus)
	for _, filename := range c.inputFiles {
		c.mapStatus[filename] = "ready"
	}
	c.mapAssignments = make([]string, len(files))

	c.reduceStatus = make([]InputStatus, nReduce)
	for i := range c.reduceStatus {
		c.reduceStatus[i] = "ready"
	}

	c.server()
	return &c
}
