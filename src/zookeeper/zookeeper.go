package zookeeper

import (
	"bytes"
	"fmt"
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

// --- ZAB State Definitions ---

type State int

const (
	StateLooking State = iota
	StateFollowing
	StateLeading
)

// Zxid represents ZooKeeper Transaction ID (epoch, counter)
type Zxid struct {
	Epoch   int64
	Counter int64
}

func (z Zxid) Less(other Zxid) bool {
	if z.Epoch != other.Epoch {
		return z.Epoch < other.Epoch
	}
	return z.Counter < other.Counter
}

func (z Zxid) LessOrEqual(other Zxid) bool {
	return z.Less(other) || z.Equal(other)
}

func (z Zxid) Equal(other Zxid) bool {
	return z.Epoch == other.Epoch && z.Counter == other.Counter
}

// LogEntry represents a transaction proposal
type LogEntry struct {
	Command interface{}
	Zxid    Zxid
	Term    int // For compatibility with util.go (maps to Epoch)
}

// --- Main ZAB Structure ---

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// --- ZAB Persistent State ---
	acceptedEpoch int64      // Highest epoch accepted from a leader
	currentEpoch  int64      // Logical clock for FLE rounds
	lastZxid      Zxid       // Last transaction ID in history
	history       []LogEntry // Transaction log (index 0 is dummy)

	// --- ZAB Volatile State ---
	state          State
	electionLeader int // Leader ID elected in this round

	// --- Commit State ---
	lastCommitted Zxid // Last committed zxid
	lastApplied   int  // Index in history of last applied entry

	// --- Leader-Only State ---
	proposalCounter      int64
	outstandingProposals map[Zxid]*ProposalTracker
	followerProgress     map[int]*FollowerState

	// --- Election State ---
	recvset         map[int]*Notification // Current epoch's votes
	electionTimer   *time.Timer
	electionTimeout time.Duration
	lastHeartbeat   time.Time // Track when we last heard from leader

	// --- Communication ---
	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
	logger    *log.Logger
}

type ProposalTracker struct {
	entry    LogEntry
	acks     map[int]bool
	ackCount int
}

type FollowerState struct {
	lastAckedZxid Zxid
	nextZxid      Zxid
}

// --- ZAB Phase 0: Fast Leader Election (FLE) ---

type Notification struct {
	ProposedLeader int
	ProposedZxid   Zxid
	ProposedEpoch  int64
	State          State
	SenderID       int
	ElectionEpoch  int64
}

func (rf *Raft) ProcessNotification(args *Notification, reply *Notification) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Handle Step Down / Epoch Update BEFORE checking state.
	if args.ElectionEpoch > rf.currentEpoch {
		rf.logger.Printf("Server %d: Stepping down from %v to Looking due to epoch %d", rf.me, rf.state, args.ElectionEpoch)
		rf.state = StateLooking
		rf.currentEpoch = args.ElectionEpoch
		rf.recvset = make(map[int]*Notification)
		// Vote for self in new epoch initially
		rf.recvset[rf.me] = &Notification{
			ProposedLeader: rf.me,
			ProposedZxid:   rf.lastZxid,
			ProposedEpoch:  rf.acceptedEpoch,
			State:          StateLooking,
			SenderID:       rf.me,
			ElectionEpoch:  rf.currentEpoch,
		}
		rf.electionLeader = rf.me
		rf.persist()
	}

	// Always send back current vote for the requester
	if rf.recvset[rf.me] != nil {
		myVote := rf.recvset[rf.me]
		reply.ProposedLeader = myVote.ProposedLeader
		reply.ProposedZxid = myVote.ProposedZxid
		reply.ProposedEpoch = myVote.ProposedEpoch
		reply.State = rf.state
		reply.SenderID = rf.me
		reply.ElectionEpoch = rf.currentEpoch
	} else {
		reply.ProposedLeader = rf.me
		reply.ProposedZxid = rf.lastZxid
		reply.ProposedEpoch = rf.acceptedEpoch
		reply.State = rf.state
		reply.SenderID = rf.me
		reply.ElectionEpoch = rf.currentEpoch
	}

	// Only process votes if we are Looking
	if rf.state != StateLooking {
		return
	}

	// Ignore stale notifications
	if args.ElectionEpoch < rf.currentEpoch {
		return
	}

	// Store notification
	rf.recvset[args.SenderID] = args

	// Check if we should update our vote
	if rf.shouldUpdateVote(args) {
		rf.electionLeader = args.ProposedLeader
		rf.recvset[rf.me] = &Notification{
			ProposedLeader: args.ProposedLeader,
			ProposedZxid:   args.ProposedZxid,
			ProposedEpoch:  args.ProposedEpoch,
			State:          StateLooking,
			SenderID:       rf.me,
			ElectionEpoch:  rf.currentEpoch,
		}
		go rf.broadcastNotifications()
	}

	// Check for quorum
	rf.checkForQuorum()
}

func (rf *Raft) sendNotification(server int, args *Notification, reply *Notification) bool {
	return rf.peers[server].Call("Raft.ProcessNotification", args, reply)
}

func (rf *Raft) shouldUpdateVote(n *Notification) bool {
	currentVote := rf.recvset[rf.me]
	if currentVote == nil {
		return true
	}

	// ZAB FLE comparison:
	// 1. Higher epoch (ZAB epoch) wins
	if n.ProposedEpoch != currentVote.ProposedEpoch {
		return n.ProposedEpoch > currentVote.ProposedEpoch
	}
	// 2. Higher zxid wins
	if !n.ProposedZxid.Equal(currentVote.ProposedZxid) {
		return !n.ProposedZxid.Less(currentVote.ProposedZxid)
	}
	// 3. Higher server ID wins
	return n.ProposedLeader > currentVote.ProposedLeader
}

func (rf *Raft) checkForQuorum() {
	if rf.state != StateLooking {
		return
	}

	votes := make(map[int]int)
	for _, n := range rf.recvset {
		if n.ElectionEpoch == rf.currentEpoch {
			votes[n.ProposedLeader]++
		}
	}

	quorum := len(rf.peers)/2 + 1

	for leader, count := range votes {
		if count >= quorum {
			rf.finalizeElection(leader)
			return
		}
	}
}

func (rf *Raft) finalizeElection(leader int) {
	rf.logger.Printf("Server %d: Election finalized, leader=%d, epoch=%d", rf.me, leader, rf.currentEpoch)

	if leader == rf.me {
		rf.state = StateLeading
		rf.electionLeader = rf.me
		go rf.startLeading()
	} else {
		rf.state = StateFollowing
		rf.electionLeader = leader
		rf.lastHeartbeat = time.Now()
		rf.resetElectionTimerLocked()
	}
}

func (rf *Raft) resetElectionTimerLocked() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	// Robust timeout to prevent instability in unreliable networks
	timeout := 800 + rand.Int63n(800)
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond

	rf.electionTimer = time.AfterFunc(rf.electionTimeout, func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() {
			return
		}

		if rf.state == StateLooking {
			rf.logger.Printf("Server %d: Election timeout in LOOKING, restarting election", rf.me)
			rf.startElectionLocked()
			return
		}

		if rf.state == StateFollowing {
			if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
				rf.logger.Printf("Server %d: No heartbeat, re-entering election", rf.me)
				rf.state = StateLooking
				rf.startElectionLocked()
			}
		}
	})
}

func (rf *Raft) startElectionLocked() {
	if rf.state != StateLooking {
		return
	}

	rf.currentEpoch++ // Increment logical election clock
	rf.persist()      // Persist the new term
	rf.recvset = make(map[int]*Notification)

	myVote := &Notification{
		ProposedLeader: rf.me,
		ProposedZxid:   rf.lastZxid,
		ProposedEpoch:  rf.acceptedEpoch,
		State:          StateLooking,
		SenderID:       rf.me,
		ElectionEpoch:  rf.currentEpoch,
	}

	rf.recvset[rf.me] = myVote
	rf.electionLeader = rf.me

	rf.logger.Printf("Server %d: Starting election, epoch=%d, zxid=(%d,%d)",
		rf.me, rf.currentEpoch, rf.lastZxid.Epoch, rf.lastZxid.Counter)

	go rf.broadcastNotifications()
}

func (rf *Raft) broadcastNotifications() {
	rf.mu.Lock()
	if rf.state != StateLooking {
		rf.mu.Unlock()
		return
	}
	myVote := rf.recvset[rf.me]
	if myVote == nil {
		rf.mu.Unlock()
		return
	}
	args := *myVote
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply Notification
			if rf.sendNotification(peer, &args, &reply) {
				rf.ProcessNotification(&reply, &Notification{})
			}
		}(i)
	}
}

// --- ZAB Phase 1: Discovery ---

type NewLeaderEpochArgs struct {
	SenderID      int
	ProposedEpoch int64
}

type NewLeaderEpochReply struct {
	AcceptedEpoch int64
	LastZxid      Zxid
	Success       bool
}

func (rf *Raft) NewLeaderEpoch(args *NewLeaderEpochArgs, reply *NewLeaderEpochReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	// Update logical clock if leader is ahead
	if args.ProposedEpoch > rf.currentEpoch {
		rf.currentEpoch = args.ProposedEpoch
		rf.persist()
	}

	// Strictly reject if the proposed epoch is stale (lower than our current view)
	if args.ProposedEpoch < rf.currentEpoch {
		return
	}

	if args.ProposedEpoch > rf.acceptedEpoch {
		rf.acceptedEpoch = args.ProposedEpoch
		rf.electionLeader = args.SenderID
		rf.state = StateFollowing
		rf.persist()
	} else if args.ProposedEpoch < rf.acceptedEpoch {
		return
	}

	reply.AcceptedEpoch = rf.acceptedEpoch
	reply.LastZxid = rf.lastZxid
	reply.Success = true
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendNewLeaderEpoch(server int, args *NewLeaderEpochArgs, reply *NewLeaderEpochReply) bool {
	return rf.peers[server].Call("Raft.NewLeaderEpoch", args, reply)
}

// --- ZAB Phase 2: Synchronization ---

type SyncMode int

const (
	DIFF SyncMode = iota
	TRUNC
	SNAP
)

type SyncData struct {
	Mode     SyncMode
	Zxid     Zxid
	Entries  []LogEntry
	Snapshot []byte
}

type SyncArgs struct {
	SenderID int
	Epoch    int64
	Data     SyncData
}

type SyncReply struct {
	Ack      bool
	LastZxid Zxid
}

func (rf *Raft) Sync(args *SyncArgs, reply *SyncReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Ack = false

	if args.Epoch < rf.acceptedEpoch {
		return
	}

	if args.Epoch > rf.acceptedEpoch {
		rf.acceptedEpoch = args.Epoch
		rf.currentEpoch = args.Epoch
		rf.persist()
	}

	switch args.Data.Mode {
	case DIFF:
		for _, e := range args.Data.Entries {
			rf.history = append(rf.history, e)
			rf.lastZxid = e.Zxid
		}
	case TRUNC:
		targetIndex := -1
		for i := len(rf.history) - 1; i >= 0; i-- {
			if rf.history[i].Zxid.Equal(args.Data.Zxid) {
				targetIndex = i
				break
			}
		}
		if targetIndex == -1 {
			// Should ideally SNAP, but simplified for now
			return
		}
		rf.history = rf.history[:targetIndex+1]
		rf.lastZxid = rf.history[len(rf.history)-1].Zxid
		for _, e := range args.Data.Entries {
			rf.history = append(rf.history, e)
			rf.lastZxid = e.Zxid
		}
	case SNAP:
		rf.history = make([]LogEntry, len(args.Data.Entries))
		copy(rf.history, args.Data.Entries)
		if len(rf.history) > 0 {
			rf.lastZxid = rf.history[len(rf.history)-1].Zxid
		}
	}

	rf.persist()
	reply.Ack = true
	reply.LastZxid = rf.lastZxid
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendSync(server int, args *SyncArgs, reply *SyncReply) bool {
	return rf.peers[server].Call("Raft.Sync", args, reply)
}

// --- ZAB Phase 3: Broadcast ---

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

	if args.Zxid.Epoch != rf.acceptedEpoch {
		return
	}

	rf.history = append(rf.history, args.Entry)
	rf.lastZxid = args.Zxid
	rf.persist()

	reply.Ack = true
	reply.Zxid = args.Zxid
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendProposal(server int, args *ProposalArgs, reply *ProposalReply) bool {
	return rf.peers[server].Call("Raft.Proposal", args, reply)
}

type CommitArgs struct {
	SenderID int
	Zxid     Zxid
}

type CommitReply struct {
	Success bool
}

func (rf *Raft) Commit(args *CommitArgs, reply *CommitReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if rf.state != StateFollowing || args.SenderID != rf.electionLeader {
		return
	}

	for i, entry := range rf.history {
		if entry.Zxid.Equal(args.Zxid) {
			if entry.Zxid.LessOrEqual(rf.lastCommitted) {
				reply.Success = true
				return
			}
			rf.lastCommitted = args.Zxid
			if i > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
			reply.Success = true
			rf.lastHeartbeat = time.Now()
			rf.resetElectionTimerLocked()
			return
		}
	}
}

func (rf *Raft) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	return rf.peers[server].Call("Raft.Commit", args, reply)
}

// --- Leader Functions ---

func (rf *Raft) startLeading() {
	rf.logger.Printf("Server %d: Starting as leader", rf.me)

	rf.mu.Lock()
	newEpoch := rf.currentEpoch
	rf.mu.Unlock()

	// Phase 1: Discovery
	success, _ := rf.runDiscovery(newEpoch)

	rf.mu.Lock()
	// Critical Check: If state changed during blocking discovery, abort.
	if rf.state != StateLeading || rf.currentEpoch != newEpoch {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if !success {
		rf.logger.Printf("Server %d: Discovery failed", rf.me)
		rf.mu.Lock()
		rf.state = StateLooking
		rf.startElectionLocked()
		rf.mu.Unlock()
		return
	}

	// Phase 2: Synchronization
	if !rf.runSynchronization(newEpoch) {
		rf.logger.Printf("Server %d: Synchronization failed", rf.me)
		rf.mu.Lock()
		// Only restart election if we are still attempting to be leader for this epoch
		if rf.state == StateLeading && rf.currentEpoch == newEpoch {
			rf.state = StateLooking
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	// Critical Check: If state changed during blocking sync, abort.
	if rf.state != StateLeading || rf.currentEpoch != newEpoch {
		rf.mu.Unlock()
		return
	}

	// Phase 3: Ready for broadcast
	rf.acceptedEpoch = newEpoch
	rf.persist()

	// Reset proposal counter for new epoch to ensure strict Zxid ordering
	rf.proposalCounter = 0

	rf.outstandingProposals = make(map[Zxid]*ProposalTracker)
	rf.followerProgress = make(map[int]*FollowerState)

	for i := range rf.peers {
		if i != rf.me {
			rf.followerProgress[i] = &FollowerState{
				lastAckedZxid: rf.lastZxid,
				nextZxid: Zxid{
					Epoch:   newEpoch,
					Counter: rf.proposalCounter + 1,
				},
			}
		}
	}
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	rf.logger.Printf("Server %d: Now leading with epoch %d", rf.me, newEpoch)

	go rf.leaderHeartbeat()
}

func (rf *Raft) runDiscovery(proposedEpoch int64) (bool, int64) {
	var mu sync.Mutex
	acks := 1 // Self

	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			if rf.killed() {
				return
			}
			var reply NewLeaderEpochReply
			args := NewLeaderEpochArgs{
				SenderID:      rf.me,
				ProposedEpoch: proposedEpoch,
			}
			if rf.sendNewLeaderEpoch(peer, &args, &reply) && reply.Success && reply.AcceptedEpoch == proposedEpoch {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	quorum := len(rf.peers)/2 + 1
	return acks >= quorum, proposedEpoch
}

func (rf *Raft) runSynchronization(epoch int64) bool {
	rf.mu.Lock()
	history := make([]LogEntry, len(rf.history))
	copy(history, rf.history)
	rf.mu.Unlock()

	var mu sync.Mutex
	acks := 1

	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			if rf.killed() {
				return
			}

			var reply NewLeaderEpochReply
			if !rf.sendNewLeaderEpoch(peer, &NewLeaderEpochArgs{SenderID: rf.me, ProposedEpoch: epoch}, &reply) {
				return
			}
			if !reply.Success {
				return
			}

			followerZxid := reply.LastZxid

			mode := DIFF
			var startZxid Zxid
			var entries []LogEntry

			commonIndex := -1
			for j := len(history) - 1; j >= 0; j-- {
				if history[j].Zxid.Equal(followerZxid) {
					commonIndex = j
					break
				}
			}

			if commonIndex == -1 {
				mode = SNAP
				entries = history
			} else if commonIndex < len(history)-1 {
				mode = DIFF
				entries = history[commonIndex+1:]
			}

			args := SyncArgs{
				SenderID: rf.me,
				Epoch:    epoch,
				Data: SyncData{
					Mode:    mode,
					Zxid:    startZxid,
					Entries: entries,
				},
			}

			var syncReply SyncReply
			if rf.sendSync(peer, &args, &syncReply) && syncReply.Ack {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	quorum := len(rf.peers)/2 + 1
	return acks >= quorum
}

func (rf *Raft) leaderHeartbeat() {
	// Heartbeat every 100ms. Also acts as batch Commit mechanism.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != StateLeading {
			rf.mu.Unlock()
			return
		}
		commitZxid := rf.lastCommitted
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				args := CommitArgs{
					SenderID: rf.me,
					Zxid:     commitZxid,
				}
				var reply CommitReply
				rf.sendCommit(peer, &args, &reply)
			}(i)
		}
	}
}

// --- Client Interface ---

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeading {
		return -1, -1, false
	}

	// Ensure leader is fully initialized (Phase 3)
	if rf.outstandingProposals == nil {
		return -1, -1, false
	}

	rf.proposalCounter++
	zxid := Zxid{
		Epoch:   rf.acceptedEpoch,
		Counter: rf.proposalCounter,
	}

	entry := LogEntry{
		Command: command,
		Zxid:    zxid,
		Term:    int(rf.acceptedEpoch),
	}

	rf.history = append(rf.history, entry)
	rf.lastZxid = zxid
	rf.persist()

	rf.outstandingProposals[zxid] = &ProposalTracker{
		entry:    entry,
		acks:     map[int]bool{rf.me: true},
		ackCount: 1,
	}

	go rf.broadcastProposal(entry)
	return int(zxid.Counter), int(zxid.Epoch), true
}

func (rf *Raft) broadcastProposal(entry LogEntry) {
	rf.mu.Lock()
	if rf.state != StateLeading {
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
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply ProposalReply
			if rf.sendProposal(peer, &args, &reply) && reply.Ack {
				rf.handleProposalAck(peer, reply.Zxid)
			}
		}(i)
	}
}

func (rf *Raft) handleProposalAck(peer int, zxid Zxid) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if zxid.Epoch != rf.acceptedEpoch || rf.state != StateLeading {
		return
	}

	if rf.outstandingProposals == nil {
		return
	}

	tracker := rf.outstandingProposals[zxid]
	if tracker == nil || tracker.acks[peer] {
		return
	}

	tracker.acks[peer] = true
	tracker.ackCount++

	quorum := len(rf.peers)/2 + 1
	if tracker.ackCount >= quorum {
		rf.lastCommitted = zxid

		// OPTIMIZATION: Do NOT broadcast explicit Commit RPC here.
		// Instead, update local state and let the periodic heartbeat (100ms)
		// propagate the commit to followers. This saves huge RPC overhead.

		rf.applyCond.Broadcast() // Wake up applier to apply locally
		delete(rf.outstandingProposals, zxid)
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentEpoch), rf.state == StateLeading
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Not required for basic ZAB
}

// --- Persistence ---

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.acceptedEpoch)
	e.Encode(rf.currentEpoch)
	e.Encode(rf.lastZxid)
	e.Encode(rf.history)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var acceptedEpoch int64
	var currentEpoch int64
	var lastZxid Zxid
	var history []LogEntry

	if d.Decode(&acceptedEpoch) == nil {
		if err := d.Decode(&currentEpoch); err != nil {
			currentEpoch = acceptedEpoch
		}
		if d.Decode(&lastZxid) == nil && d.Decode(&history) == nil {
			rf.acceptedEpoch = acceptedEpoch
			rf.currentEpoch = currentEpoch
			rf.lastZxid = lastZxid
			rf.history = history
		}
	}
}

// --- Background Goroutines ---

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == StateLooking {
			go rf.broadcastNotifications()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		for rf.lastApplied < len(rf.history)-1 &&
			rf.history[rf.lastApplied+1].Zxid.LessOrEqual(rf.lastCommitted) {

			entry := rf.history[rf.lastApplied+1]
			rf.lastApplied++

			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int(entry.Zxid.Counter),
			}
			rf.mu.Lock()
		}

		rf.applyCond.Wait()
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

	rf.state = StateLooking
	rf.recvset = make(map[int]*Notification)
	rf.history = make([]LogEntry, 1) // Dummy entry at index 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.logger = log.New(log.Writer(), fmt.Sprintf("ZK%d ", me), log.LstdFlags|log.Lmicroseconds)

	rf.readPersist(persister.ReadRaftState())

	if rf.currentEpoch < rf.acceptedEpoch {
		rf.currentEpoch = rf.acceptedEpoch
	}

	go rf.ticker()
	go rf.applier()
	go rf.startElectionLocked()

	return rf
}
