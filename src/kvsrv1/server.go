package kvsrv

import (
	"log"
	"sync"
	// Rpc "net/rpc"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVValue struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]KVValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.mu = sync.Mutex{}
	kv.data = make(map[string]KVValue)
	// Rpc.Register()
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.data[args.Key] = KVValue{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if value.Version == args.Version {
			kv.data[args.Key] = KVValue{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
