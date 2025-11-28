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

type State int

const (
	StateFollower  State = 0
	StateCandidate State = 1
	StateLeader    State = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         State
	lastHeartbeat time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
	
	logger    *log.Logger
	electionTimeout time.Duration
	rng             *rand.Rand 
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

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

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	}

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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	} else if rf.state == StateCandidate {
		rf.state = StateFollower
	}
	rf.resetElectionTimer()

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		return
	}

	logChanged := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
				logChanged = true
			}
		} else {
			rf.log = append(rf.log, entry)
			logChanged = true
		}
	}

	if logChanged {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

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
	rf.persist()

	index := len(rf.log) - 1
	term := rf.currentTerm

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	go rf.broadcastHeartbeats()

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

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
					rf.persist()
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > len(rf.peers)/2 {
						if rf.state != StateLeader {
							rf.state = StateLeader
							for i := range rf.peers {
								rf.nextIndex[i] = len(rf.log)
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = len(rf.log) - 1
							go rf.broadcastHeartbeats()
						}
					}
				}
			}
		}(peerIdx)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return 
	}

	term := rf.currentTerm
	commitIndex := rf.commitIndex

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[peerIdx]
		if nextIdx < 1 { nextIdx = 1 }
		if nextIdx > len(rf.log) { nextIdx = len(rf.log) }

		prevLogIndex := nextIdx - 1
		prevLogTerm := rf.log[prevLogIndex].Term

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
		rf.persist()
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = newMatchIndex
		}
		rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

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
		if rf.nextIndex[peerIdx] < 1 {
			rf.nextIndex[peerIdx] = 1
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
	// SAFETY: Priority Bias. Server 0 is faster.
	baseMs := 600
	if rf.me == 0 {
		baseMs = 300
	}
	ms := baseMs + (rf.rng.Intn(300))
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == StateLeader {
			if time.Since(rf.lastHeartbeat) >= 100*time.Millisecond {
				rf.mu.Unlock()
				go rf.broadcastHeartbeats()
				rf.mu.Lock()
				rf.lastHeartbeat = time.Now()
			}
		} else {
			if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.logger = createServerLogger(me)
	rf.rng = rand.New(rand.NewSource(int64(me)*777 + time.Now().UnixNano()))

	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimer()

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}