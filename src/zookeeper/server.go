package zookeeper

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
	"reflect"


)

const (
	SnapShotInterval = 10
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type rfsrv struct {
	ts          *Test
	me          int
	applyErr    string // from apply channel readers
	lastApplied int
	persister   *tester.Persister

	mu   sync.Mutex
	raft raftapi.Raft
	logs map[int]any // copy of each server's committed entries
}

func newRfsrv(ts *Test, srv int, ends []*labrpc.ClientEnd, persister *tester.Persister, snapshot bool) *rfsrv {
	//log.Printf("mksrv %d", srv)
	s := &rfsrv{
		ts:        ts,
		me:        srv,
		logs:      map[int]any{},
		persister: persister,
	}
	applyCh := make(chan raftapi.ApplyMsg)
	if !useRaftStateMachine {
		s.raft = Make(ends, srv, persister, applyCh)
	}
	if snapshot {
		snapshot := persister.ReadSnapshot()
		if snapshot != nil && len(snapshot) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := s.ingestSnap(snapshot, -1)
			if err != "" {
				tester.AnnotateCheckerFailureBeforeExit("failed to ingest snapshot", err)
				ts.t.Fatal(err)
			}
		}
		go s.applierSnap(applyCh)
	} else {
		go s.applier(applyCh)
	}
	return s
}

func (rs *rfsrv) Kill() {
	//log.Printf("rs kill %d", rs.me)
	rs.mu.Lock()
	rs.raft = nil // tester will call Kill() on rs.raft
	rs.mu.Unlock()
	if rs.persister != nil {
		// mimic KV server that saves its persistent state in case it
		// restarts.
		raftlog := rs.persister.ReadRaftState()
		snapshot := rs.persister.ReadSnapshot()
		rs.persister.Save(raftlog, snapshot)
	}
}

func (rs *rfsrv) GetState() (int, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft.GetState()
}

func (rs *rfsrv) Raft() raftapi.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft
}
func (rs *rfsrv) Logs(i int) (any, bool) {
    rs.mu.Lock()
    defer rs.mu.Unlock()

    v, ok := rs.logs[i]
    return v, ok
}


// 🎯 This helper *recursively unwraps* any array/slice until the real value is found
func unwrapCommand(cmd interface{}) interface{} {
    for {
        v := reflect.ValueOf(cmd)
        if v.Kind() == reflect.Slice && v.Len() > 0 {
            cmd = v.Index(0).Interface() // take first element
        } else {
            break
        }
    }
    return cmd
}


// applier reads message from apply ch and checks that they match the log
// contents
func (rs *rfsrv) applier(applyCh chan raftapi.ApplyMsg) {
    for m := range applyCh {
        if !m.CommandValid {
            continue
        }

        rs.mu.Lock()
        rf := rs.raft.(*Raft)
        rs.mu.Unlock()

        rf.mu.Lock()
        committedUpTo := rf.lastCommitted
        rf.mu.Unlock()

        // Ensure command index ≤ lastCommitted.Counter
        if int64(m.CommandIndex) > committedUpTo.Counter {
            log.Fatalf("apply error: server %v applied uncommitted index %v > committed %v",
                rs.me, m.CommandIndex, committedUpTo.Counter)
        }

        // Track in mirror log
        rs.mu.Lock()
        rs.logs[m.CommandIndex] = m.Command
        rs.mu.Unlock()
    }
}

func (rs *rfsrv) applierSnap(applyCh chan raftapi.ApplyMsg) {
	if rs.raft == nil {
		return
	}

	for m := range applyCh {
		if m.SnapshotValid {
			err_msg := rs.ingestSnap(m.Snapshot, m.SnapshotIndex)
			if err_msg != "" {
				log.Fatalf("snapshot ingest error: %v", err_msg)
			}
			continue
		}

		if !m.CommandValid {
			continue
		}

		// 1️⃣ Enforce in-order application (same as before)
		if m.CommandIndex != rs.lastApplied+1 {
			err_msg := fmt.Sprintf(
				"server %v apply out of order: expected %v, got %v",
				rs.me, rs.lastApplied+1, m.CommandIndex)
			log.Fatalf(err_msg)
		}

		// 2️⃣ Store applied command (for snapshot testing)
		rs.mu.Lock()
		rs.logs[m.CommandIndex] = m.Command
		rs.lastApplied = m.CommandIndex
		rs.mu.Unlock()

		// 3️⃣ Snapshot creation
		if (m.CommandIndex+1)%SnapShotInterval == 0 {
			rf := rs.raft.(*Raft)

			rf.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			e.Encode(m.CommandIndex) // lastIncludedIndex
			var xlog []any
			for j := 1; j <= m.CommandIndex && j < len(rf.history); j++ {
				xlog = append(xlog, rf.history[j].Command)
			}
			e.Encode(xlog)
			rf.mu.Unlock()

			start := tester.GetAnnotateTimestamp()
			rs.raft.Snapshot(m.CommandIndex, w.Bytes())
			tester.AnnotateInfoInterval(start, "snapshot created",
				fmt.Sprintf("snapshot created at index %v", m.CommandIndex))
		}
	}
}


// returns "" or error string
func (rs *rfsrv) ingestSnap(snapshot []byte, index int) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if snapshot == nil {
		tester.AnnotateCheckerFailureBeforeExit("failed to ingest snapshot", "nil snapshot")
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []any
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		text := "failed to decode snapshot"
		tester.AnnotateCheckerFailureBeforeExit(text, text)
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", rs.me)
		return err
	}
	rs.logs = map[int]any{}
	for j := 0; j < len(xlog); j++ {
		rs.logs[j] = xlog[j]
	}
	rs.lastApplied = lastIncludedIndex
	return ""
}
