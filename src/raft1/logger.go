package raft

import (
	"fmt"
	"log"
	"os"
)

const Debug = true
func createServerLogger(serverID int) *log.Logger {
	os.MkdirAll("logs", 0755) // Ensure logs/ directory exists

	filename := fmt.Sprintf("logs/raft_server_%d.log", serverID)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	return log.New(file,
		fmt.Sprintf("[S%d] ", serverID),
		log.LstdFlags|log.Lmicroseconds)
}

func (rf *Raft) Logf(format string, a ...interface{}) {
	if Debug && rf.logger != nil {
		// fmt.Printf("DEBUG is false still its getting  %v\n", Debug)
		prefix := fmt.Sprintf("[Term %d] ", rf.currentTerm)
		rf.logger.Printf(prefix+format, a...)
	}
}
