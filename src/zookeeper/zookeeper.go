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

type Zxid struct {
	Epoch   int64
	Counter int64
}

type LogEntry struct {
	Command interface{}
	Term    int 
	Zxid    Zxid
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
	state      State
	logger     *log.Logger

	acceptedEpoch int64
	lastZxid      Zxid
	lastCommitted Zxid
	
	proposalCounter      int64
	outstandingProposals map[Zxid]*ProposalTracker
	
	recvset         map[int]*Notification
	electionTimer   *time.Timer
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	rng             *rand.Rand

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

type ProposalTracker struct {
	entry    LogEntry
	acks     map[int]bool
	ackCount int
}

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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}
	if rf.outstandingProposals == nil {
		return -1, -1, false
	}

	rf.proposalCounter++
	zxid := Zxid{
		Epoch:   int64(rf.currentTerm),
		Counter: rf.proposalCounter,
	}

	entry := LogEntry{
		Command: command,
		Zxid:    zxid,
		Term:    rf.currentTerm,
	}

	rf.log = append(rf.log, entry)
	rf.lastZxid = zxid
	
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	
	rf.persist()

	rf.outstandingProposals[zxid] = &ProposalTracker{
		entry:    entry,
		acks:     map[int]bool{rf.me: true},
		ackCount: 1,
	}

	go rf.broadcastProposal(entry)
	
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

type Notification struct {
	ProposedLeader int
	ProposedZxid   Zxid
	ProposedEpoch  int64
	SenderID       int
	ElectionEpoch  int64
}

func (rf *Raft) ProcessNotification(args *Notification, reply *Notification) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.ElectionEpoch > int64(rf.currentTerm) {
		rf.state = StateCandidate
		rf.currentTerm = int(args.ElectionEpoch)
		rf.recvset = make(map[int]*Notification)
		rf.votedFor = rf.me
		rf.persist()
	}

	reply.ProposedLeader = rf.votedFor
	reply.ProposedZxid = rf.lastZxid
	reply.ProposedEpoch = int64(rf.acceptedEpoch)
	reply.ElectionEpoch = int64(rf.currentTerm)

	if rf.state != StateCandidate {
		return
	}

	rf.recvset[args.SenderID] = args
	
	votes := make(map[int]int)
	for _, n := range rf.recvset {
		if n.ElectionEpoch == int64(rf.currentTerm) {
			votes[n.ProposedLeader]++
		}
	}

	if votes[rf.votedFor] >= len(rf.peers)/2+1 {
		if rf.votedFor == rf.me {
			rf.becomeLeader()
		} else {
			rf.state = StateFollower
			rf.resetElectionTimerLocked()
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
	rf.acceptedEpoch = int64(rf.currentTerm)
	rf.proposalCounter = 0
	rf.outstandingProposals = make(map[Zxid]*ProposalTracker)
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.persist()
	rf.resetElectionTimerLocked()
	go rf.leaderHeartbeat()
}

func (rf *Raft) broadcastProposal(entry LogEntry) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	args := ProposalArgs{
		SenderID: rf.me,
		Zxid:     entry.Zxid,
		Entry:    entry,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me { continue }
		go func(peer int) {
			var reply ProposalReply
			if rf.peers[peer].Call("Raft.Proposal", &args, &reply) && reply.Ack {
				rf.handleProposalAck(peer, reply.Zxid)
			}
		}(i)
	}
}

type ProposalArgs struct {
	SenderID int
	Zxid     Zxid
	Entry    LogEntry
}
type ProposalReply struct {
	Ack  bool
	Zxid Zxid
}

func (rf *Raft) Proposal(args *ProposalArgs, reply *ProposalReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Ack = false
	if int64(rf.currentTerm) != args.Zxid.Epoch {
		return 
	}
	
	rf.log = append(rf.log, args.Entry)
	rf.lastZxid = args.Zxid
	rf.persist()
	
	reply.Ack = true
	reply.Zxid = args.Zxid
}

func (rf *Raft) handleProposalAck(peer int, zxid Zxid) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.outstandingProposals == nil { return }
	tracker := rf.outstandingProposals[zxid]
	if tracker == nil { return }
	
	tracker.acks[peer] = true
	tracker.ackCount++
	
	currentLogLen := len(rf.log)
	rf.matchIndex[peer] = currentLogLen - 1 
	rf.nextIndex[peer] = currentLogLen

	if tracker.ackCount >= len(rf.peers)/2+1 {
		rf.lastCommitted = zxid
		
		for i := len(rf.log)-1; i >=0; i-- {
			if rf.log[i].Zxid == zxid {
				rf.commitIndex = i
				break
			}
		}
		
		rf.applyCond.Broadcast()
		delete(rf.outstandingProposals, zxid)
	}
}

func (rf *Raft) leaderHeartbeat() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for !rf.killed() {
		<-ticker.C
		rf.mu.Lock()
		if rf.state == StateLeader {
            // Heartbeat
		} else {
            ticker.Stop()
            rf.mu.Unlock()
            return
        }
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetElectionTimerLocked() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	// Bias for Server 0
	baseMs := 600
	if rf.me == 0 {
		baseMs = 300
	}
	ms := baseMs + (rf.rng.Intn(300))

	rf.electionTimeout = time.Duration(ms) * time.Millisecond
	rf.electionTimer = time.AfterFunc(rf.electionTimeout, func() {
		rf.mu.Lock()
		if !rf.killed() && rf.state != StateLeader {
			rf.state = StateCandidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.recvset = make(map[int]*Notification)
			
			myVote := Notification{
				ProposedLeader: rf.me,
				ProposedZxid:   rf.lastZxid,
				ProposedEpoch:  int64(rf.acceptedEpoch),
				SenderID:       rf.me,
				ElectionEpoch:  int64(rf.currentTerm),
			}
			rf.recvset[rf.me] = &myVote
			rf.persist()
			
			rf.mu.Unlock()
			
			for i := range rf.peers {
				if i == rf.me { continue }
				go func(peer int) {
					var reply Notification
					if rf.peers[peer].Call("Raft.ProcessNotification", &myVote, &reply) {
						rf.ProcessNotification(&reply, &Notification{})
					}
				}(i)
			}
			
			rf.mu.Lock()
			rf.resetElectionTimerLocked()
		}
		rf.mu.Unlock()
	})
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
	if data == nil || len(data) < 1 { return }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&logs) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		if len(rf.log) > 0 {
			rf.lastZxid = rf.log[len(rf.log)-1].Zxid
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
	
	rf.rng = rand.New(rand.NewSource(int64(me)*777 + time.Now().UnixNano()))
	rf.logger = createServerLogger(me)

	rf.state = StateFollower
	rf.recvset = make(map[int]*Notification)
	rf.log = make([]LogEntry, 1) 
	
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimerLocked()
	
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.applyCond.Wait()
				if rf.killed() { rf.mu.Unlock(); return }
			}
			commit := rf.commitIndex
			applied := rf.lastApplied
			entries := make([]LogEntry, commit-applied)
			copy(entries, rf.log[applied+1:commit+1])
			rf.lastApplied = commit
			rf.mu.Unlock()
			
			for i, e := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command: e.Command,
					CommandIndex: applied + 1 + i,
				}
			}
		}
	}()

	return rf
}