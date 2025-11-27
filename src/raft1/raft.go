package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Enum for Raft server states
type State int

const (
	StateFollower  = 0
	StateCandidate = 1
	StateLeader    = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// --- Persistent State (Figure 2) ---
	currentTerm int
	votedFor    int
	log         []LogEntry

	// --- Volatile State (Figure 2) ---
	commitIndex int
	lastApplied int

	// --- Leader Volatile State (Figure 2) ---
	nextIndex  []int
	matchIndex []int

	// --- Internal Implementation State ---
	state         State         // Current role (Follower, Candidate, Leader)
	lastHeartbeat time.Time     // Time of last valid heartbeat received

	applyCh   chan raftapi.ApplyMsg // To talk to the Tester/App
	applyCond *sync.Cond            // To wake up the applier goroutine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.state == StateLeader)

	return term, isleader
}

// save Raft's persistent state to stable storage
func (rf *Raft) persist() {
	// Your code here (3C).
	// Placeholder for now.
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot handling (3D)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// RequestVote RPC arguments
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments (Heartbeats + Log Replication)
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 2. Update term if we see a newer one
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}

	// 3. Check Log Up-to-Date
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if !logIsUpToDate {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 4. Grant Vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.lastHeartbeat = time.Now() // Reset timer
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Standard Term Check
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 2. Recognize Leader / Update Term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}
	rf.lastHeartbeat = time.Now() // Reset election timer

	// 3. Log Consistency Check
	// Case A: Log too short
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// Case B: Term mismatch at PrevLogIndex
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		
		// Optimization: Scan back to find start of conflict term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		return
	}

	// 4. Append Entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx] // Truncate
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	// 5. Update Commit Index
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		rf.applyCond.Broadcast() // Wake up applier
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Logic to start agreement on a new log entry
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	// --- FIX: Return index is len-1 ---
	index := len(rf.log) - 1
	term := rf.currentTerm

	// --- FIX: Leader updates its own matchIndex immediately ---
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.broadcastHeartbeats() // Broadcast immediately to replicate

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// --- Internal Logic Helpers ---

func (rf *Raft) startElection() {
	// rf.mu is already Locked by caller
	rf.currentTerm++
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	term := rf.currentTerm
	votesReceived := 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func(idx int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(idx, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != term || rf.state != StateCandidate {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = StateFollower
					rf.votedFor = -1
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > len(rf.peers)/2 {
						rf.state = StateLeader
						
						// Initialize Leader State
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						rf.broadcastHeartbeats()
					}
				}
			}
		}(peerIdx)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	// rf.mu is Locked by caller
	term := rf.currentTerm
	commitIndex := rf.commitIndex

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[peerIdx]
		// Safety check
		if nextIdx < 1 { nextIdx = 1 }
		if nextIdx > len(rf.log) { nextIdx = len(rf.log) }

		prevLogIndex := nextIdx - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		
		// Copy entries
		entriesToSend := make([]LogEntry, len(rf.log)-nextIdx)
		copy(entriesToSend, rf.log[nextIdx:])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: commitIndex,
		}

		go func(idx int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(idx, &args, &reply) {
				rf.handleAppendEntriesReply(idx, &args, &reply)
			}
		}(peerIdx, args)
	}
}

func (rf *Raft) handleAppendEntriesReply(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = StateFollower
		rf.votedFor = -1
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = newMatchIndex
		}
		rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

		// Commit Logic
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
				rf.commitIndex = N
				rf.applyCond.Broadcast()
				break
			}
		}
	} else {
		// Optimization Backoff logic
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peerIdx] = reply.ConflictIndex
		} else {
			found := false
			lastEntryWithTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastEntryWithTerm = i
					found = true
					break
				}
			}
			if found {
				rf.nextIndex[peerIdx] = lastEntryWithTerm + 1
			} else {
				rf.nextIndex[peerIdx] = reply.ConflictIndex
			}
		}
		
		// Sanity check
		if rf.nextIndex[peerIdx] < 1 {
			rf.nextIndex[peerIdx] = 1
		}
	}
}

// Background ticker to start elections
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 1. Check Leader status
		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		if state == StateLeader {
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.broadcastHeartbeats()
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			// 2. Check Election Timeout
			// Use a small tick to check frequently rather than sleeping for the whole duration
			// This prevents missing heartbeats during sleep
			ms := 300 + (rand.Int63() % 300)
			timeout := time.Duration(ms) * time.Millisecond

			if time.Since(lastHeartbeat) > timeout {
				rf.mu.Lock()
				// Double check state inside lock
				if rf.state != StateLeader && time.Since(rf.lastHeartbeat) > timeout {
					rf.startElection()
				}
				rf.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Applier goroutine
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		entriesToApply := make([]LogEntry, commitIndex-lastApplied)
		copy(entriesToApply, rf.log[lastApplied+1:commitIndex+1])
		
		rf.lastApplied = commitIndex
		rf.mu.Unlock()

		for i, entry := range entriesToApply {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
			rf.applyCh <- msg
		}
	}
}

// Make creates a Raft server
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	seed := int64(me) + time.Now().UnixNano()
	rand.Seed(seed)

	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()

	// Initialize log with dummy at index 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1 // --- FIX: Start at 1 (after dummy) ---
	}
	rf.matchIndex = make([]int, len(peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}