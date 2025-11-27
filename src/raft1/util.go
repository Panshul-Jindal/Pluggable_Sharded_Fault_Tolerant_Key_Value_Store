package raft

import (
	"fmt"
	// "log"
)

// ANSI color codes
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
)

// Map Raft state to color
func colorForState(state State) string {
	switch state {
	case StateLeader:
		return ColorBlue
	case StateFollower:
		return ColorGreen
	case StateCandidate:
		return ColorYellow
	default:
		return ColorReset
	}
}

// Debugging

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(format, a...)
	}
}

func (rf *Raft) Debug(format string, a ...interface{}) {
    if Debug && rf.logger != nil {
        rf.logger.Printf(format, a...)
    }
}

func (rf *Raft) DebugState(prefix string) {
	if !Debug || rf.logger == nil {
		return
	}

	stateStr := [...]string{"Follower", "Candidate", "Leader"}[rf.state]

	rf.logger.Printf("\n[%s] 🗂 S%d | Term=%d | Role=%s | CommitIndex=%d | LastApplied=%d",
		prefix, rf.me, rf.currentTerm, stateStr, rf.commitIndex, rf.lastApplied)

	// Log contents (index : term)
	rf.logger.Printf("     📜 Log: ")
	for i, e := range rf.log {
		rf.logger.Printf("        [%d:T%d] ", i, e.Term)
	}
	rf.logger.Printf("\n")

	if rf.state == StateLeader {
		rf.logger.Printf("     📌 nextIndex:  %v", rf.nextIndex)
		rf.logger.Printf("     📎 matchIndex: %v", rf.matchIndex)
	}
	rf.logger.Printf("---------------------------------------------------\n")
}
