package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// Ask coordinator for a task
		getTaskArgs := &GetTaskArgs{}
		getTaskReply := &GetTaskReply{}
		ok := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		if !ok || getTaskReply.Error != "" {
			log.Fatal("Task failed! RPC Error: ", getTaskReply.Error)
		}

		// Execute assigned task
		if getTaskReply.TaskType == "MAP" {
			mapFiles(mapf, getTaskReply.InputFiles, getTaskReply.NReduce)
		} else if getTaskReply.TaskType == "REDUCE" {
			reduceFiles(reducef, getTaskReply.InputFiles)
		}

	}
}

func mapFiles(mapf func(string, string) []KeyValue, inputFiles []string, nReduce int) {
	kvs := []KeyValue{}
	for _, filename := range inputFiles {
		contents, err := os.ReadFile(filename)
		if err != nil {
			log.Fatal("Failed to open file:", filename)
		}
		newKvs := mapf(filename, string(contents))
		kvs = append(kvs, newKvs...) // join two slices with ... expansion

	}
	writeMapOutput(kvs, nReduce)
}

func writeMapOutput(kvs []KeyValue, nReduce int) {
	// open temp output files for writing

	// TEMPORARY
	/*
		for _, kv := range kvs {
			println("This is the fake write output", kv.Key, kv.Value)
		}
	*/
}

func reduceFiles(reducef func(string, []string) string, inputFiles []string) {

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
