package kvraft

import (
    "sync"
    "sync/atomic"

    "6.5840/kvraft1/rsm"
    "6.5840/kvsrv1/rpc"
    "6.5840/labgob"
    "6.5840/labrpc"
    "6.5840/tester1"
)

// Internal item type for key/value + version.
type Item struct {
    Value   string
    Version rpc.Tversion
}

type KVServer struct {
    me   int
    dead int32 // set by Kill()
    rsm  *rsm.RSM

    mu    sync.Mutex
    store map[string]Item
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    switch args := req.(type) {
    case rpc.GetArgs:
        // Get(key): return value+version or ErrNoKey
        item, ok := kv.store[args.Key]
        if !ok {
            return rpc.GetReply{
                Err: rpc.ErrNoKey,
            }
        }
        return rpc.GetReply{
            Value:   item.Value,
            Version: item.Version,
            Err:     rpc.OK,
        }

    case rpc.PutArgs:
        // Put(key, value, version) with Lab 2 semantics
        var rep rpc.PutReply

        item, exists := kv.store[args.Key]
        if !exists {
            // "Put installs the value if the args.Version is 0"
            if args.Version == 0 {
                kv.store[args.Key] = Item{
                    Value:   args.Value,
                    Version: 1,
                }
                rep.Err = rpc.OK
            } else {
                // "returns ErrNoKey otherwise"
                rep.Err = rpc.ErrNoKey
            }
            return rep
        }

        // Key exists; check version match
        if args.Version == item.Version {
            item.Value = args.Value
            item.Version++
            kv.store[args.Key] = item
            rep.Err = rpc.OK
        } else {
            rep.Err = rpc.ErrVersion
        }
        return rep

    default:
        // Should not happen
        return nil
    }
}

func (kv *KVServer) Snapshot() []byte {
    // Part C will use this. For 4B we can just return nil.
    return nil
}

func (kv *KVServer) Restore(data []byte) {
    // Part C will use this. For 4B we can ignore data.
}

// Get RPC handler: submit to RSM and return the result.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
    // Submit the *value* of args (non-pointer), because labgob registered rpc.GetArgs.
    err, res := kv.rsm.Submit(*args)
    if err == rpc.ErrWrongLeader {
        reply.Err = rpc.ErrWrongLeader
        return
    }
    r := res.(rpc.GetReply)
    *reply = r
}

// Put RPC handler: submit to RSM and return the result.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
    err, res := kv.rsm.Submit(*args)
    if err == rpc.ErrWrongLeader {
        reply.Err = rpc.ErrWrongLeader
        return
    }
    r := res.(rpc.PutReply)
    *reply = r
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
    atomic.StoreInt32(&kv.dead, 1)
    // Your code here, if desired.
}

func (kv *KVServer) killed() bool {
    z := atomic.LoadInt32(&kv.dead)
    return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(rsm.Op{})
    labgob.Register(rpc.PutArgs{})
    labgob.Register(rpc.GetArgs{})

    kv := &KVServer{me: me}
    kv.store = make(map[string]Item)

    kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
    // You may need initialization code here.
    return []tester.IService{kv, kv.rsm.Raft()}
}
