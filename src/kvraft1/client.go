package kvraft

import (
	"time"

	"6.5840/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string

	lastLeader int // index of server we think is leader
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:       clnt,
		servers:    servers,
		lastLeader: 0,
	}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}

	for {
		// Try all servers, starting from lastLeader
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.lastLeader + i) % len(ck.servers)

			var reply rpc.GetReply
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Get", args, &reply)
			if !ok {
				// Network or server issue; try next server.
				continue
			}

			if reply.Err == rpc.ErrWrongLeader {
				// Not leader; try next server.
				continue
			}

			// Either OK or ErrNoKey (or other final error).
			ck.lastLeader = srv
			return reply.Value, reply.Version, reply.Err
		}

		// All servers failed or said WrongLeader. Pause and retry.
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	firstAttempt := true

	for {
		// Try all servers, starting from lastLeader
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.lastLeader + i) % len(ck.servers)

			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Put", args, &reply)

			if !ok {
				// RPC failed; we *might* have actually committed at a leader,
				// but didn't see the reply. From now on, treat ErrVersion as
				// potentially "maybe executed".
				firstAttempt = false
				continue
			}

			if reply.Err == rpc.ErrWrongLeader {
				// Not leader; keep firstAttempt as-is, since we know the
				// op was not committed through this server.
				continue
			}

			if reply.Err == rpc.ErrVersion && !firstAttempt {
				// Got ErrVersion after we've had at least one lost RPC.
				// We can't know if an earlier attempt actually succeeded.
				return rpc.ErrMaybe
			}

			// Either OK, ErrNoKey, or first-time ErrVersion -> final.
			ck.lastLeader = srv
			return reply.Err
		}

		// No server gave us a definite answer; all failed or WrongLeader.
		// We are now definitely past the first attempt.
		firstAttempt = false
		time.Sleep(100 * time.Millisecond)
	}
}
