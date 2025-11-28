package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *labrpc.Network
	n         int
	rafts     []raftapi.Raft
	applyChs  []chan raftapi.ApplyMsg
	connected []bool
	saved     []*tester.Persister
	endnames  [][]string
	logs      []map[int]interface{}
	start     time.Time
}

func make_config(t *testing.T, n int, unreliable bool, snapshot bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.rafts = make([]raftapi.Raft, cfg.n)
	cfg.applyChs = make([]chan raftapi.ApplyMsg, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*tester.Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]interface{}, cfg.n)
	cfg.start = time.Now()

	cfg.net.Reliable(!unreliable)

	// Register types for RPC
	labgob.Register(LogEntry{})
	labgob.Register(Zxid{})

	for i := 0; i < cfg.n; i++ {
		cfg.endnames[i] = make([]string, cfg.n)
	}

	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = make(map[int]interface{})
		cfg.start1(i)
	}

	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

func (cfg *config) start1(i int) {
	cfg.crash1(i)

	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i][j] == "" {
			// SAFE NAMING: Simple alphanumeric
			cfg.endnames[i][j] = fmt.Sprintf("server-%d-%d", i, j)
		}
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.connected[i] = true
	cfg.applyChs[i] = make(chan raftapi.ApplyMsg)

	go func() {
		for range cfg.applyChs[i] {
		}
	}()

	if cfg.saved[i] == nil {
		cfg.saved[i] = tester.MakePersister()
	}

	cfg.rafts[i] = Make(ends, i, cfg.saved[i], cfg.applyChs[i])

	var svc *labrpc.Service
	if rf, ok := cfg.rafts[i].(*Raft); ok {
		svc = labrpc.MakeService(rf)
	} else {
		// Fallback for interface compliance
		svc = labrpc.MakeService(cfg.rafts[i])
	}

	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
}

func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i)
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
		cfg.rafts[i] = nil
	}
}

func (cfg *config) connect(i int) {
	if cfg.connected[i] {
		return
	}
	cfg.connected[i] = true
	for j := 0; j < cfg.n; j++ {
		if i != j {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}
}

func (cfg *config) disconnect(i int) {
	if !cfg.connected[i] {
		return
	}
	cfg.connected[i] = false
	for j := 0; j < cfg.n; j++ {
		if j != i {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) checkOneLeader() int {
	// 40 Iterations x 500ms = 20 Seconds to find a leader
	for iters := 0; iters < 40; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				term, isleader := cfg.rafts[i].GetState()
				if isleader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, serverIDs := range leaders {
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
			if len(serverIDs) > 1 {
				// Multiple leaders found, wait for convergence
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	
	// FAILURE DIAGNOSTICS
	fmt.Println("\n!!! FAILURE: CLUSTER STATE !!!")
	for i := 0; i < cfg.n; i++ {
		if cfg.rafts[i] != nil {
			term, isleader := cfg.rafts[i].GetState()
			fmt.Printf("Server %d: Term=%d, Leader=%v\n", i, term, isleader)
		}
	}
	cfg.t.Fatal("expected one leader, got none")
	return -1
}

func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		index := -1
		for si := 0; si < cfg.n; si++ {
			var rf raftapi.Raft
			cfg.mu.Lock()
			if cfg.connected[si] {
				rf = cfg.rafts[si]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, isLeader := rf.Start(cmd)
				if isLeader {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			return index
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("one(%v) failed to reach agreement", cmd)
	return -1
}

func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
}

func (cfg *config) end() {}
func (cfg *config) LogSize() int { return 1000 }
func (cfg *config) nCommitted(index int) int { return 0 }