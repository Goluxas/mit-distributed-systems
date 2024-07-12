package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var workerId string

func init() {
	// Possibly not always unique, uses the last 3 digits of the current nanosecond
	workerId = fmt.Sprintf("%03d", time.Now().Nanosecond()%1e3)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		Debug(dRpc, "W%v asking Coordinator for Task", workerId)
		getTaskArgs := &GetTaskArgs{}
		getTaskReply := &GetTaskReply{}
		ok := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		if !ok {
			log.Fatal("Failed RPC call")
		}

		/*
			NOTE:
			When I didn't have the Coordinator return WAIT or DONE messages, this loop would execute
			hundreds of times per worker while a Map job was still in process, eventually overwhelming the socket
		*/
		if getTaskReply.Error == eWait {
			Debug(dInfo, "W%v waiting before next task request.", workerId)
			time.Sleep(1 * time.Second)
			continue
		}

		if getTaskReply.Error == eDone {
			// No more tasks. You may rest, son
			Debug(dInfo, "W%v no tasks left. Shutting down.", workerId)
			return
		}

		// Execute assigned task
		if getTaskReply.TaskType == "MAP" {
			Debug(dInfo, "W%v received MAP task %v for %v", workerId, getTaskReply.TaskID, getTaskReply.InputFiles)
			mapFiles(mapf, getTaskReply.InputFiles, getTaskReply.NReduce, getTaskReply.TaskID)
		} else if getTaskReply.TaskType == "REDUCE" {
			Debug(dInfo, "W%v received REDUCE task %v for %v", workerId, getTaskReply.TaskID, getTaskReply.InputFiles)
			reduceFiles(reducef, getTaskReply.InputFiles, getTaskReply.TaskID)
		}

	}
}

func mapFiles(mapf func(string, string) []KeyValue, inputFiles []string, nReduce int, taskId string) {
	kvs := []KeyValue{}
	for _, filename := range inputFiles {
		contents, err := os.ReadFile(filename)
		if err != nil {
			log.Fatal("Failed to open file:", filename)
		}
		newKvs := mapf(filename, string(contents))
		kvs = append(kvs, newKvs...) // join two slices with ... expansion

	}
	outputFiles := writeMapOutput(kvs, nReduce, taskId)

	Debug(dRpc, "W%v notifying Coordinator of MAP task %v completion", workerId, taskId)
	args := TaskFinishedArgs{TaskType: "MAP", TaskID: taskId, OutputFiles: outputFiles}
	reply := TaskFinishedReply{}
	ok := call("Coordinator.TaskFinished", &args, &reply)
	if !ok {
		log.Fatal("Failed to report task completion")
	}
}

func writeMapOutput(kvs []KeyValue, nReduce int, taskId string) []string {
	// open temp output files for writing
	tempFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp("", "")
		if err != nil {
			log.Fatal("Could not open temp file:", err)
		}

		tempFiles[i] = tempFile
		encs[i] = json.NewEncoder(tempFile)
	}

	// write key-value pairs to temp files
	for _, kv := range kvs {
		reduceIdx := ihash(kv.Key) % nReduce

		//fmt.Fprintf(tempFiles[reduceIdx], "%v %v\n", kv.Key, kv.Value)
		if err := encs[reduceIdx].Encode(&kv); err != nil {
			log.Fatalf("Could not JSON Encode value (%v: %v). %v", kv.Key, kv.Value, err)
		}
	}

	for _, file := range tempFiles {
		file.Close()
	}

	// rename temp files to final versions
	// format: mr-{map task id}-{reduce task id}
	outputFilenameBase := "./mr-" + taskId + "-"
	outputFilenames := []string{}
	for i, tempFile := range tempFiles {
		newName := outputFilenameBase + strconv.Itoa(i)
		err := os.Rename(tempFile.Name(), newName)
		if err != nil {
			log.Fatal("Failed to rename temp file ", tempFile.Name(), " to ", newName, ": ", err)
		}
		outputFilenames = append(outputFilenames, newName)
	}

	return outputFilenames
}

func reduceFiles(reducef func(string, []string) string, inputFiles []string, taskId string) {
	// Create output file
	ofileName := fmt.Sprintf("mr-out-%v", taskId)
	outFile, err := os.Create(ofileName)
	if err != nil {
		log.Fatalf("Could not create file %v. %v")
	}

	// Read all files in
	kva := []KeyValue{}
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Could not open file %v. %v", filename, err)
		}

		dec := json.NewDecoder(file)
		// dec.More() is true if there are additional values to read
		for dec.More() {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				log.Fatalf("Error decoding JSON value. %v", err)
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// Sort by key
	sort.Sort(ByKey(kva))

	for i := 0; i < len(kva); i++ {
		key := kva[i].Key

		// declaring j outside the for loop lets us use it beyond the for loop scope
		var j int
		values := []string{}
		for j = i; j < len(kva); j++ {
			if kva[j].Key != key {
				break
			}
			values = append(values, kva[j].Value)
		}
		// off by one error because i increments at the end of the loop
		// COULD replace i++ in the loop declaration with i=j but that is even weirder
		i = j - 1

		// Call reducef on each key
		reduced := reducef(key, values)
		// write each return of reducef to output file
		fmt.Fprintf(outFile, "%v %v\n", key, reduced)
	}

	Debug(dRpc, "W%v notifying Coordinator of REDUCE task %v completion", workerId, taskId)
	args := TaskFinishedArgs{
		TaskType:    "REDUCE",
		TaskID:      taskId,
		OutputFiles: []string{ofileName},
	}
	reply := TaskFinishedReply{}
	ok := call("Coordinator.TaskFinished", &args, &reply)
	if !ok {
		log.Fatal("RPC error")
	}

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
