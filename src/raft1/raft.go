package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// --- Definitions to satisfy util.go / logger.go ---

type State int

const (
	StateFollower  State = 0 // ZAB: Following
	StateCandidate State = 1 // ZAB: Looking (Election)
	StateLeader    State = 2 // ZAB: Leading
)

// The LogEntry struct must have a 'Term' field for util.go to print it.
// In ZAB, 'Term' is the Epoch.
type LogEntry struct {
	Command interface{}
	Term    int // Acts as ZAB Epoch
	// Zxid is implicit: (Term, Index)
}

// Raft struct implementing ZAB Logic
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// --- Persistent State (Mapped to ZAB) ---
	currentTerm int        // ZAB: Current Epoch
	votedFor    int        // ServerID voted for
	log         []LogEntry // History

	// --- Volatile State ---
	commitIndex int // ZAB: Index of CommitZxid
	lastApplied int // Index of last applied entry

	// --- Leader State (Mapped for util.go) ---
	nextIndex  []int // Map index to send
	matchIndex []int // ZAB: Index of highest Acked Zxid

	// --- Internal Helper State ---
	state         State
	lastHeartbeat time.Time
	applyCh       chan raftapi.ApplyMsg
	applyCond     *sync.Cond
	logger        *log.Logger
	timeoutDur    time.Duration
	lastBroadcast []time.Time // For throttling
}

// --- Interface Implementation ---

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Start (ZAB: New Proposal)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}

	// 1. Create Proposal (Entry)
	// ZAB assigns Zxid = (Epoch, Counter).
	// Here, Epoch = currentTerm, Counter = len(log).
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm, // Epoch
	}

	// 2. Local Append
	rf.log = append(rf.log, entry)
	rf.persist()

	// 3. Update Leader State
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	// 4. Broadcast
	go rf.broadcastProposals()

	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

// --- Persistence ---

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&logs) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// --- ZAB Logic wrapped in Raft RPCs ---

// We reuse RequestVoteArgs/Reply for ZAB Notification/Election
type RequestVoteArgs struct {
	Term         int // Epoch
	CandidateID  int
	LastLogIndex int // Last Zxid Counter
	LastLogTerm  int // Last Zxid Epoch
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote Handler (ZAB: Handle Notification)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Default reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. Epoch Check (ZAB: Notification comparison)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	}

	// 2. Election Logic (ZAB Fast Leader Election)
	// Vote if:
	// a. Epoch is higher (already handled above)
	// b. Same Epoch, but we haven't voted yet
	// c. Candidate is more up-to-date (Higher Zxid)

	if args.Term < rf.currentTerm {
		return // Ignore old epochs
	}

	// Check Up-to-Date (Zxid comparison)
	// Zxid = (Epoch, Counter) -> (LastLogTerm, LastLogIndex)
	myLastIdx := len(rf.log) - 1
	myLastTerm := rf.log[myLastIdx].Term

	isUpToDate := false
	if args.LastLogTerm > myLastTerm {
		isUpToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx {
		isUpToDate = true
	}

	if !isUpToDate {
		return
	}

	// Grant Vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		rf.state = StateFollower // Ensure we aren't Candidate
		rf.resetTimer()
		rf.persist()
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) startElection() {
	rf.state = StateCandidate
	rf.currentTerm++ // New Epoch
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTimer()

	term := rf.currentTerm
	votes := 1
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(idx, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != StateCandidate || rf.currentTerm != args.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = StateFollower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						rf.becomeLeader()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	if rf.state == StateLeader {
		return
	}
	rf.state = StateLeader

	// Initialize Leader State
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// ZAB Synchronization: Broadcast immediately
	go rf.broadcastProposals()
}

// --- Proposal / Atomic Broadcast (ZAB) ---

// We reuse AppendEntries for ZAB Proposal
type AppendEntriesArgs struct {
	Term         int // Epoch
	LeaderID     int
	PrevLogIndex int // Counter of prev Zxid
	PrevLogTerm  int // Epoch of prev Zxid
	Entries      []LogEntry
	LeaderCommit int // Commit Zxid (Index)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. Epoch Check
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	}
	rf.resetTimer()

	// 2. Consistency Check (PrevZxid)
	// Check if our log contains the entry at PrevLogIndex with PrevLogTerm
	if len(rf.log) <= args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		return
	}

	// 3. Append / Truncate (Atomic Broadcast)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	// 4. Commit Check
	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) broadcastProposals() {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	commit := rf.commitIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[i]
		if nextIdx < 1 {
			nextIdx = 1
		}

		// Optimization: Throttle heartbeats if up to date
		if nextIdx >= len(rf.log) &&
			rf.matchIndex[i] >= commit &&
			time.Since(rf.lastBroadcast[i]) < 90*time.Millisecond {
			continue
		}

		// Prepare Entries
		var entries []LogEntry
		if nextIdx < len(rf.log) {
			entries = make([]LogEntry, len(rf.log)-nextIdx)
			copy(entries, rf.log[nextIdx:])
		}

		prevIdx := nextIdx - 1
		prevTerm := rf.log[prevIdx].Term

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: commit,
		}

		go func(idx int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(idx, &args, &reply) {
				rf.handleProposalReply(idx, &args, &reply)
			}
		}(i, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleProposalReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		rf.lastBroadcast[server] = time.Now()
		// Update match (Ack)
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
			rf.nextIndex[server] = newMatch + 1
		}

		// Commit Logic (Quorum of Acks)
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
		// Backoff / Sync
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// Search for conflict term
			found := false
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					rf.nextIndex[server] = i + 1
					found = true
					break
				}
			}
			if !found {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
}

// --- Ticker / Applier ---

func (rf *Raft) resetTimer() {
	rf.lastHeartbeat = time.Now()
	ms := 350 + (rand.Int63() % 350)
	rf.timeoutDur = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.state == StateLeader {
			// ZAB: Keep alive / Broadcast
			if time.Since(rf.lastHeartbeat) > 100*time.Millisecond {
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				rf.broadcastProposals()
				rf.mu.Lock()
			}
		} else {
			if time.Since(rf.lastHeartbeat) > rf.timeoutDur {
				rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

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
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.lastApplied = commitIndex
		rf.mu.Unlock()

		for i, entry := range entries {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
			rf.applyCh <- msg
		}
	}
}

// --- Factory ---

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Logger initialization
	rf.logger = createServerLogger(me)

	rf.currentTerm = 0
	rf.state = StateFollower
	rf.votedFor = -1
	rf.resetTimer()

	// Dummy Entry at 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastBroadcast = make([]time.Time, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}