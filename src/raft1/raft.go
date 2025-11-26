package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	state         State     // Current role (Follower, Candidate, Leader)
	lastHeartbeat time.Time // Time of last valid heartbeat received     (Why do we need this ?)

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
	// Your code here (3A).

	term = rf.currentTerm
	isleader = (rf.state == StateLeader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs  (Heartbeats)        TODO : What all fields represent?
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int
	Success bool

	//Optimization fields
	ConflictIndex int
    ConflictTerm  int

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm (Section 5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 2. If RPC request or response contains term > currentTerm:
	// set currentTerm = term, convert to follower (Section 5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1 // Reset vote because it's a new term
	} //if you are a Leader and you see a request with a higher term, you must immediately step down to Foll

	// 3. Check if we can vote for this candidate
	// (Figure 2: Receiver implementation #2)

	// Check if candidate's log is at least as up-to-date as ours.
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

	// We vote IF:
	// - We haven't voted yet (votedFor == -1) OR we already voted for this candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm // send back current term

		// IMPORTANT: Granting a vote resets our election timer!
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) except if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. If not leader, return false immediately   // ensures that only leader can start any kind of request..
	// why? because only leader can append entries to log, its the one point contact for clients, other client
	//redirect the requests to the leader)
	if rf.state != StateLeader {
		return -1, -1, false
	}

	// 2. Append entry to local log
	index := len(rf.log) // The new index will be the current length (since 0-indexed dummy exists)
	term := rf.currentTerm
	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	// 3. Update Leader's own tracking state (optional but clean)
	// The leader always matches itself.
	// (Note: nextIndex/matchIndex will be properly managed in the sender loop usually)

	// 4. Optimization: Trigger a broadcast immediately so we don't wait 100ms
	// You can call rf.broadcastHeartbeats() here if you want faster tests.

	return index, term, true //return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// Note: rf.mu is already Locked by the caller (ticker)

	rf.currentTerm++
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now() // Reset timer

	term := rf.currentTerm
	votesReceived := 1 // We vote for ourselves

	// Inside startElection...
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// Send RequestVote to all peers apart from yourself
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		// Request Vote in  Parallel       Launch a goroutine for each peer so we don't block
		go func(idx int) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateID: rf.me,
				// LastLogIndex/Term will be needed for 2B, 0 for now is fine
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			// Send the RPC
			if rf.sendRequestVote(idx, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Check if our state changed while waiting for reply
				//Why? Because while we were waiting for the network, we might have received a heartbeat from a valid leader and turned back into a Follower
				if rf.currentTerm != term || rf.state != StateCandidate {
					return
				}

				if reply.Term > rf.currentTerm {
					// Oops, there is a newer leader/term. Step down.
					rf.currentTerm = reply.Term
					rf.state = StateFollower
					rf.votedFor = -1
					return
				}

				if reply.VoteGranted {
					votesReceived++
					// Check for Majority
					if votesReceived > len(rf.peers)/2 {
						// We won!
						rf.state = StateLeader

						// --- ADD THIS BLOCK ---
						// Reinitialize Volatile Leader State
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log) // Initialize to leader's log length
							rf.matchIndex[i] = 0          // Safely start at 0
						}

						// Trigger heartbeats immediately (Part 3B)
						rf.broadcastHeartbeats()
					}
				}
			}
		}(peerIdx)
	}
}

func (rf *Raft) handleAppendEntriesReply(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// State check
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
		// Update matchIndex and nextIndex
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = newMatchIndex
		}
		rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

		// CHECK FOR COMMIT (Figure 2: Rules for Leader)
		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
		// and log[N].term == currentTerm: set commitIndex = N

		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			count := 1 // Count self
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			//The Rule: A leader is not allowed to update commitIndex for an entry from a previous term, even if it is stored on a majority of servers. It must wait until it commits at least one entry from its current term.
			if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm { // Why check log[N].term == currentTerm?
				rf.commitIndex = N
				rf.applyCond.Broadcast() // Wake up applier
				break
			}
		}

	} else {
		// Failed check (Log Inconsistency)
		// Decrement nextIndex and retry later
		// rf.nextIndex[peerIdx]--
		// // Note: There is an optimization here to backup faster, but decrementing by 1 works for correctness.

		if reply.ConflictTerm == -1 {
            // Follower's log was too short.
            // Set nextIndex to the length of the follower's log.
            rf.nextIndex[peerIdx] = reply.ConflictIndex
        }else {
            // Term mismatch. Check if we have that term.
            // Goal: Find the last entry in our log with that ConflictTerm.
            
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
                // If we have the term, try to match just after it
                rf.nextIndex[peerIdx] = lastEntryWithTerm + 1
            } else {
                // We don't have this term at all.
                // Reset to the follower's first index for that term.
				// The follower has a term we've never seen (or deleted).
                // We must overwrite it. Back up to the start of their conflict.
                rf.nextIndex[peerIdx] = reply.ConflictIndex
            }
        }


        
        // Safety clamp: ensure we don't go out of bounds (shouldn't happen with above logic but good practice)
        if rf.nextIndex[peerIdx] < 1 {
            rf.nextIndex[peerIdx] = 1
        }





		if rf.nextIndex[peerIdx] < 1 {
			rf.nextIndex[peerIdx] = 1
		}
	}
}

func (rf *Raft) broadcastHeartbeats() {
	// Note: rf.mu is Locked by caller (ticker)
	term := rf.currentTerm
	commitIndex := rf.commitIndex

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		// Calculate Prevs based on nextIndex[peerIdx]
		nextIdx := rf.nextIndex[peerIdx]

		// Safety check: If nextIdx is invalid, reset it to a safe value///TODO Isn't needed explicitly
		if nextIdx > len(rf.log) {
			nextIdx = len(rf.log)
		}

		prevLogIndex := nextIdx - 1

		// Safety check: prevent out of bounds if nextIndex is somehow wrong
		if prevLogIndex < 0 {
			prevLogIndex = 0
		}
		if prevLogIndex >= len(rf.log) {
			prevLogIndex = len(rf.log) - 1
		}

		prevLogTerm := rf.log[prevLogIndex].Term

		// Grab the entries to send (from nextIdx to end)
		// Make a copy to avoid race conditions if log changes

		// print("Current:", len(rf.log), " ", nextIdx, "\n")

		entriesToSend := make([]LogEntry, len(rf.log)-nextIdx)
		// if entriesToSend == nil{
		// print("This didn't run ")
		// }
		copy(entriesToSend, rf.log[nextIdx:])

		go func(idx int, args AppendEntriesArgs) { // Launch goroutine for each peer, sending AppendEntries in parallel
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(idx, &args, &reply) {
				rf.handleAppendEntriesReply(idx, &args, &reply)
			}
		}(peerIdx, AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: commitIndex,
		})
	}

}

// Don't forget the RPC wrapper!
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Standard Term Check   Reply false if term < currentTerm (Section 5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 2.Heartbeat: Reset timer If term > currentTerm, become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}

	// 3. If term == currentTerm, we recognize this leader
	rf.lastHeartbeat = time.Now() // CRITICAL: Reset election timer!

	// 2. Log Consistency Check (Figure 2, Step 2)
	// Return false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm

	// Case A: My log is too short
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		// Optimization: Tell leader to try next at the end of my log
        reply.ConflictIndex = len(rf.log)
        reply.ConflictTerm = -1 // No term conflict, just length
		return
	}
	// Case B: I have the index, but the term doesn't match
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		// 1. Remember the conflicting term
        reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// 2. Scan backwards to find the first index of this conflicting term
        idx := args.PrevLogIndex
        for idx > 0 && rf.log[idx].Term == reply.ConflictTerm {
            idx--
        }

		// idx points to the entry *before* the conflict range, so add 1
        reply.ConflictIndex = idx + 1

		return
	}

	// 3. If we are here, the log matches up to PrevLogIndex!
	// Append any new entries not already in the log.
	// (Figure 2, Step 3 & 4)

	for i, entry := range args.Entries {
		// Calculate the index in our log where this entry goes
		idx := args.PrevLogIndex + 1 + i

		if idx < len(rf.log) {
			// Check for conflict
			if rf.log[idx].Term != entry.Term {
				// Delete everything from here onwards and append new
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
			// If terms match, we keep existing entry (idempotency)
		} else {
			// We are past the end of our log, just append
			rf.log = append(rf.log, entry)
		}
	}

	// 4. Update Commit Index
	// (Figure 2, Step 5)
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex = min(leaderCommit, index of last new entry)
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
		// Trigger the apply channel (we'll do this in a separate applier loop)
		rf.applyCond.Broadcast() // You'll need a condition variable for this   /// Appl
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == StateLeader {
			// Leader Logic: Send Heartbeats frequently
			rf.mu.Lock()
			if rf.state == StateLeader { // Double check inside lock
				rf.broadcastHeartbeats()
			}
			rf.mu.Unlock()

			// Heartbeat interval (must be < election timeout)
			time.Sleep(100 * time.Millisecond)
		} else {
			// Follower/Candidate Logic: Check Election Timeout

			// Calculate random timeout (e.g., 300-500ms)
			ms := 300 + (rand.Int63() % 200)
			timeout := time.Duration(ms) * time.Millisecond
			time.Sleep(timeout)

			rf.mu.Lock()
			if rf.state != StateLeader && time.Since(rf.lastHeartbeat) > timeout {
				rf.startElection()
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

// Applier goroutine: Applies committed entries to the state machine THE MAIN DELIVERY TRUCK
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		// 1. Wait until there is something to apply
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		// 2. Identify the entries to apply
		// We copy them to a slice so we can release the lock while sending
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		entriesToApply := make([]LogEntry, commitIndex-lastApplied)
		copy(entriesToApply, rf.log[lastApplied+1:commitIndex+1])

		// 3. Update internal state immediately
		rf.lastApplied = commitIndex

		rf.mu.Unlock()

		// 4. Deliver messages to the application (Outside the lock!)
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
	rf.applyCond = sync.NewCond(&rf.mu) // Link it to the main mutex

	// --- FIX START ---
	// Seed the random number generator with a unique value based on time and ID
	// Note: In newer Go versions (1.20+), global rand is auto-seeded,
	// but explicit seeding is safer for these labs.
	seed := int64(me) + time.Now().UnixNano()
	rand.Seed(seed)

	// Your initialization code here (3A, 3B, 3C).
	// Initialize state
	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 means null/no vote yet
	rf.lastHeartbeat = time.Now()

	// Initialize log with a dummy entry at index 0
	rf.log = make([]LogEntry, 1) // Length 1
	rf.log[0] = LogEntry{Term: 0}

	// Initialize leader state (even if not leader, good to have memory ready)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier() // Start the delivery truck

	return rf
}
