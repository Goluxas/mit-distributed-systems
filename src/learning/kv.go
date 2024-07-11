package main

import (
	"log"
	"net"
	"net/rpc"
	"sync"
)

/******
CLIENT
******/

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string) string {
	client := connect()
	defer client.Close()

	args := GetArgs{key}
	reply := GetReply{}

	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	return reply.Value
}

func put(key string, value string) {
	client := connect()
	defer client.Close()

	args := PutArgs{key, value}
	reply := PutReply{}

	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
}

/*****
SERVER
******/

type KV struct {
	mu   sync.Mutex
	data map[string]string
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = "OK"
		reply.Value = val
	}

	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value
	reply.Err = "OK"

	return nil
}

func server() {
	kv := &KV{data: map[string]string{}}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	go func() {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				break
			}

			go rpcs.ServeConn(conn)
		}
	}()

}

/*******
MAIN
******/

func main() {
	server()

	put("fish", "bird")
	res := get("fish")

	println(res)
}
