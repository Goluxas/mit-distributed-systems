package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	mu           sync.Mutex
	inputFiles   []string
	nReduce      int
	mapStatus    map[string]InputStatus
	mapDone      bool
	reduceStatus []InputStatus
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// iterate over mapDone and return the first unstarted map task
	mapTask := false
	if !c.mapDone {
		for filename, status := range c.mapStatus {
			if status == "ready" {
				// Coordinator tracking
				c.mapStatus[filename] = "started"
				mapTask = true

				// Reply for RPC
				reply.TaskType = "MAP"
				reply.InputFiles = []string{filename}
				reply.NReduce = c.nReduce
				reply.Error = ""

				return nil
			}
		}
	}

	// if no map tasks are left to assign, check if mapping is done
	if !mapTask {
		mapDone := true
		for _, status := range c.mapStatus {
			if status != "done" {
				mapDone = false
			}
		}
		c.mapDone = mapDone
	}

	// if mapping is done, assign a reduce task
	if c.mapDone {
		// TODO
	}

	return nil
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

	c.reduceStatus = make([]InputStatus, nReduce)
	for i := range c.reduceStatus {
		c.reduceStatus[i] = "ready"
	}

	c.server()
	return &c
}
