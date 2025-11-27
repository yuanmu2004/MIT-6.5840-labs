package lock

import (
	// "6.5840/kvsrv1/rpc"
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l string
	// version rpc.Tversion
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l}
	// You may add code here
	lk.id = fmt.Sprintf("%s-%d-%d", l, rand.Int(), time.Now().Nanosecond())
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	validate := false
	for {
		value, version, err := lk.ck.Get(lk.l)
		if validate && err == rpc.OK {
			if value == lk.id {
				return
			}
			validate = false
		}
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.l, lk.id, 0)
			if err == rpc.OK {
				return
			}
		} else {
			// lock is free
			if value == "" {
				err = lk.ck.Put(lk.l, lk.id, version)
				if err == rpc.OK {
					return
				}
			} 
		}
		if err == rpc.ErrMaybe {
			validate = true
		}
		time.Sleep(time.Duration(rand.Intn(60) + 20) * time.Millisecond)
	}
	
}

func (lk *Lock) Release() {
	// Your code here
	validate := false
	for {
		value, version, err := lk.ck.Get(lk.l)
		if validate && err == rpc.OK {
			if value != lk.id {
				return
			}
			validate = false
		}
		if err == rpc.ErrNoKey {
			return
		} else {
			if value == lk.id {
				err = lk.ck.Put(lk.l, "", version)
				if err == rpc.OK {
					return
				}
			}
		}
		if err == rpc.ErrMaybe {
			validate = true
		}
		time.Sleep(time.Duration(rand.Intn(60) + 20) * time.Millisecond)
	}
}
