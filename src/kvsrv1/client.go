package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}

	for {
		// Reset reply for every retry
		*reply = rpc.GetReply{}
		
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
		
		// If RPC failed (network issue), wait briefly and retry
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	firstAttempt := true

	for {
		reply := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)

		if ok {
			if reply.Err == rpc.ErrVersion {
				// "If Put receives an ErrVersion on its first RPC, Put should return ErrVersion"
				if firstAttempt {
					return rpc.ErrVersion
				} else {
					// "If the server returns ErrVersion on a resend RPC... Put must return ErrMaybe"
					return rpc.ErrMaybe
				}
			}
			return reply.Err
		}

		// RPC failed; we are now entering retry mode
		firstAttempt = false
		time.Sleep(100 * time.Millisecond)
	}
}