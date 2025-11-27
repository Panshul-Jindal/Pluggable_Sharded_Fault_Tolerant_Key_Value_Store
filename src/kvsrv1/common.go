// Added for lab 2

package kvsrv

// Put or Append
const (
	OpPut    = "Put"
	OpAppend = "Append"
)

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	// Field names must start with capital letters
	ClientID  int64
	RequestID int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string

	ClientID  int64
	RequestID int
}

type GetReply struct {
	Value string
}
