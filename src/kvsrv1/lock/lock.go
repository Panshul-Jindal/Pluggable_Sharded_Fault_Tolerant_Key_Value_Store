package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	key  string // The key used in the KV store for this lock
	myId string // The unique identifier for this client
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:   ck,
		key:  l,
		myId: kvtest.RandValue(8), // Generate unique ID for this lock client
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 1. Check current state of the lock
		val, ver, err := lk.ck.Get(lk.key)

		// 2. Determine if lock is free
		// It is free if the key doesn't exist yet, or the value is empty string
		isFree := (err == rpc.ErrNoKey) || (val == "")

		if isFree {
			// 3. Try to acquire using Optimistic Concurrency Control
			// If ErrNoKey, version expected is 0. Otherwise use version from Get.
			targetVer := ver
			if err == rpc.ErrNoKey {
				targetVer = 0
			}

			putErr := lk.ck.Put(lk.key, lk.myId, targetVer)

			if putErr == rpc.OK {
				// Success: We hold the lock
				return
			} else if putErr == rpc.ErrMaybe {
				// Ambiguous result (dropped reply). Check if we actually won.
				// We perform a read to see if the value is now our ID.
				checkVal, _, _ := lk.ck.Get(lk.key)
				if checkVal == lk.myId {
					return
				}
				// If checkVal != lk.myId, the Put failed or someone else overwrote (unlikely if we follow protocol)
				// Loop again.
			}
			// If ErrVersion, someone else modified it between our Get and Put. Retry loop.
		} else {
			// Lock is held by someone else.
			// Sleep briefly to avoid hammering the server (spin-wait)
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	for {
		// 1. Get current version to perform atomic delete/clear
		val, ver, _ := lk.ck.Get(lk.key)

		// Safety check: Only release if we actually hold it
		// (Though lab instructions imply we can assume we hold it if Release is called)
		if val == lk.myId {
			// 2. Set value to empty string to indicate "Free"
			putErr := lk.ck.Put(lk.key, "", ver)

			if putErr == rpc.OK {
				return
			} else if putErr == rpc.ErrMaybe {
				// Ambiguous result. Check if release happened.
				checkVal, _, _ := lk.ck.Get(lk.key)
				if checkVal == "" {
					return
				}
				// If not empty, retry
			}
			// If ErrVersion, someone modified it? Retry loop to get new version.
		} else {
			// We don't hold the lock, or it's already released.
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
