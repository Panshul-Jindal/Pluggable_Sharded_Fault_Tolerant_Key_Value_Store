package kvsrv

import (
	"log"
	"sync"

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
// Internal struct to hold value and version
type Item struct {
	value   string
	version rpc.Tversion
}


type KVServer struct {
	mu sync.Mutex

	// Key -> (Value, Version)
	store map[string]Item
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.store = make(map[string]Item)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	item, exists := kv.store[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = item.value
	reply.Version = item.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	item, exists := kv.store[args.Key]

	if !exists {
		// "Put installs the value if the args.Version is 0"
		if args.Version == 0 {
			kv.store[args.Key] = Item{
				value:   args.Value,
				version: 1, // Version 1 is usually the first valid version after 0
			}
			reply.Err = rpc.OK
		} else {
			// "returns ErrNoKey otherwise"
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	// Key exists: Check version match
	if args.Version == item.version {
		// Match: update value and increment version
		item.value = args.Value
		item.version++
		kv.store[args.Key] = item
		reply.Err = rpc.OK
	} else {
		// Mismatch
		reply.Err = rpc.ErrVersion
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
