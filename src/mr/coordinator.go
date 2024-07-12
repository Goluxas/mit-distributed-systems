package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type InputStatus string

const (
	iReady   InputStatus = "ready"
	iStarted InputStatus = "started"
	iDone    InputStatus = "done"
)

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
	reduceFiles    [][]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// iterate over mapDone and return the first unstarted map task
	if !c.mapDone {
		for filename, status := range c.mapStatus {
			if status == iReady {
				// Coordinator tracking
				c.mapStatus[filename] = iStarted
				taskId := c.mapNextId
				c.mapNextId++
				//c.mapAssignments[taskId] = filename
				// index should automatically match because we start mapNextId at 0 and only append here when assigning map tasks
				c.mapAssignments = append(c.mapAssignments, filename)

				// Reply for RPC
				reply.TaskType = "MAP"
				reply.TaskID = strconv.Itoa(taskId)
				reply.InputFiles = []string{filename}
				reply.NReduce = c.nReduce
				reply.Error = eNone

				go c.healthCheck("MAP", taskId)

				return nil
			}
		}
	}

	// if no map tasks are left to assign, check if mapping is done
	c.checkMapTasks()

	// if mapping is done, assign a reduce task
	if c.mapDone {
		for reduceJob, status := range c.reduceStatus {
			if status == iReady {
				// Coordinator tracking
				c.reduceStatus[reduceJob] = iStarted

				// RPC Reply
				reply.TaskType = "REDUCE"
				reply.TaskID = strconv.Itoa(reduceJob)
				reply.InputFiles = c.reduceFiles[reduceJob]
				reply.NReduce = c.nReduce
				reply.Error = eNone

				go c.healthCheck("REDUCE", reduceJob)

				return nil
			}
		}

		// If we got here, all reduce tasks are currently taken
		// TEMPORARY
		// TODO - check for hanging tasks and reassign instead of assuming we're done
		reply.Error = eDone
		return nil

	} else {
		// Reduce tasks are coming, please wait
		reply.Error = eWait
		return nil
	}
}

func (c *Coordinator) healthCheck(taskType string, taskId int) {
	// If the worker hasn't completed the task in 10 seconds, reassign it
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	switch taskType {
	case "MAP":
		fileForTask := c.mapAssignments[taskId]
		if c.mapStatus[fileForTask] != iDone {
			// setting status back to ready allows us to reassign the task
			c.mapStatus[fileForTask] = iReady
		}
	case "REDUCE":
		if c.reduceStatus[taskId] != iDone {
			c.reduceStatus[taskId] = iReady
		}
	}
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	Debug(dRpc, "Coordinator received Task Finished message from %v Task %v with file(s) %v", args.TaskType, args.TaskID, args.OutputFiles)

	c.mu.Lock()
	defer c.mu.Unlock()

	taskId, err := strconv.Atoi(args.TaskID)
	if err != nil {
		log.Fatal("Invalid task ID in TaskFinished: ", args.TaskID, " -- ", err)
	}

	switch {
	case args.TaskType == "MAP":
		// Only update status if the job was not finished
		// (eg. task 1 was taking too long and got reassigned, but finished before the new worker's return which is now)
		if c.mapStatus[c.mapAssignments[taskId]] != iDone {
			c.mapStatus[c.mapAssignments[taskId]] = iDone
			c.collateIntermediateFiles(args.OutputFiles)
			c.checkMapTasks()
		}

	case args.TaskType == "REDUCE":
		c.reduceStatus[taskId] = iDone
	}

	return nil
}

func (c *Coordinator) collateIntermediateFiles(files []string) {
	// ONLY call from within a Locked context
	Debug(dInfo, "Coordinator organizing intermediate files")

	/*
		DO NOT lock and unlock like this from within a nested function call.
		We're still locked inside TaskFinished so this Lock() will deadlock
		c.mu.Lock()
		defer c.mu.Unlock()
	*/

	for _, filename := range files {
		reduceJob, err := strconv.Atoi(filename[len(filename)-1:])
		if err != nil {
			log.Fatalf("Could not infer reduce job ID from filename: %v", filename)
		}

		c.reduceFiles[reduceJob] = append(c.reduceFiles[reduceJob], filename)
	}
}

func (c *Coordinator) checkMapTasks() {
	// ONLY call within a locked context
	Debug(dInfo, "Coordinator checking if all map tasks are done")

	/*
		c.mu.Lock()
		defer c.mu.Unlock()
	*/

	mapDone := true
	for _, status := range c.mapStatus {
		if status != iDone {
			mapDone = false
		}
	}

	c.mapDone = mapDone
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
	//ret = c.mapDone

	// ACTUAL -- if mapping is done and reducing is done then we're all done
	if c.mapDone {
		ret = true
		for _, status := range c.reduceStatus {
			if status != iDone {
				ret = false
			}
		}
	}

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
		c.mapStatus[filename] = iReady
	}
	c.mapAssignments = []string{}

	c.reduceStatus = make([]InputStatus, nReduce)
	c.reduceFiles = make([][]string, nReduce)
	for i := range c.reduceStatus {
		c.reduceStatus[i] = iReady
	}

	c.server()
	return &c
}
