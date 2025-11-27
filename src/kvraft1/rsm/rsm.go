package rsm

import (
	"sync"
	"time"
	 "sync/atomic"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	// "6.5840/raft1"
	"6.5840/zookeeper"
	"6.5840/raftapi"
	"6.5840/tester1"

)

var useRaftStateMachine bool // to plug in another raft besided raft1

var opCounter int64

func nrand() int64 {
    return atomic.AddInt64(&opCounter, 1)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID   int64     // unique op id
    Req  any       // the original request (GetArgs or PutArgs)
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	lastApplied  int                  // last applied index
   pending      map[int64]chan any   // opID → response channel
clientID     int64                // optional for dedup in B
}


func (rsm *RSM) runReader() {
    for msg := range rsm.applyCh {
        if msg.CommandValid {
            op := msg.Command.(Op)

            // 1. Apply to state machine
            result := rsm.sm.DoOp(op.Req)

            rsm.mu.Lock()
            if ch, ok := rsm.pending[op.ID]; ok {
                ch <- result
                delete(rsm.pending, op.ID)
            }
            rsm.mu.Unlock()
        }
    }
}


// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	rsm.pending = make(map[int64]chan any)
	go rsm.runReader()

	if !useRaftStateMachine {
		// rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		rsm.rf = zookeeper.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	op := Op{ID: nrand(), Req: req}

    _, term, isLeader := rsm.rf.Start(op)
    if !isLeader {
        return rpc.ErrWrongLeader, nil
    }

    ch := make(chan any, 1) // buffered to avoid deadlock

    rsm.mu.Lock()
    rsm.pending[op.ID] = ch
    rsm.mu.Unlock()

    for {
        select {
        case res := <-ch:
            return rpc.OK, res

        case <-time.After(200 * time.Millisecond):
            curTerm, isCurLeader := rsm.rf.GetState()
            if !isCurLeader || curTerm != term {
                // Lost leadership
                rsm.mu.Lock()
                delete(rsm.pending, op.ID)
                rsm.mu.Unlock()
                return rpc.ErrWrongLeader, nil
            }
        }
    }
}
