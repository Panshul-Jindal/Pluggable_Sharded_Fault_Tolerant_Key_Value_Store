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
    StateLeader   = 2
)

type LogEntry struct {
	Command   interface{}
	EntryTerm int
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
	lastHeartbeat time.Time  // Time of last valid heartbeat received     (Why do we need this ?)
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
    }//if you are a Leader and you see a request with a higher term, you must immediately step down to Foll

	

	// 3. Check if we can vote for this candidate
    // (Figure 2: Receiver implementation #2)
    // We vote IF:
    //  - We haven't voted yet (votedFor == -1) OR we already voted for this candidate
    //  - (For Lab 2B/C, we will also check if the log is up-to-date here)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
                LastLogIndex: 0, 
                LastLogTerm:  0,
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
                        // Trigger heartbeats immediately (Part 3B)
                        rf.broadcastHeartbeats() 
                    }
                }
            }
        }(peerIdx)
    }
}

func (rf *Raft) broadcastHeartbeats() {
    // Note: rf.mu is Locked by caller (ticker)
    term := rf.currentTerm
    
    for peerIdx := range rf.peers {
        if peerIdx == rf.me {
            continue
        }
        
        go func(idx int) {
            args := AppendEntriesArgs{
                Term:     term,
                LeaderID: rf.me,
                // Empty entries for now (Heartbeat)
                Entries:  nil, 
            }
            reply := AppendEntriesReply{}
            
            // Send RPC
            if rf.sendAppendEntries(idx, &args, &reply) {
                rf.mu.Lock()
                defer rf.mu.Unlock()
                
                // Check if our state changed
                if rf.currentTerm != term || rf.state != StateLeader {
                    return
                }

                // If reply.Term > currentTerm, step down!
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = StateFollower
                    rf.votedFor = -1
                    return
                }
            }
        }(peerIdx)
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

    // 1. Reply false if term < currentTerm (Section 5.1)
    if args.Term < rf.currentTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }

    // 2. If term > currentTerm, become follower
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = StateFollower
        rf.votedFor = -1
    }

    // 3. If term == currentTerm, we recognize this leader
    rf.lastHeartbeat = time.Now() // CRITICAL: Reset election timer!
    
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

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


	

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
