package core

import "fmt"

// RunSideEffect invokes se if non-nil. Backings call this for a WithSideEffect func inside their
// critical sections, once the operation is otherwise guaranteed to succeed; a non-nil return aborts
// (rolls back) the operation. A WithOnAdmit hook is not run through this — it runs with the lock
// released, via RunOnAdmit.
func RunSideEffect(se func() error) error {
	if se == nil {
		return nil
	}
	if err := se(); err != nil {
		return fmt.Errorf("%w: %w", ErrSideEffectFailed, err)
	}
	return nil
}

// RunSideEffectLocked invokes se exactly as RunSideEffect does, but releases the caller's lock if
// se panics or calls runtime.Goexit. Push, Pop, NotEmpty, NotFull and Close unlock explicitly on
// each return path rather than through a defer, so without this an abnormal unwind out of the
// caller's side effect carries the lock away with it and every later operation on the queue blocks
// forever. On a normal return the caller still holds the lock and unlocks as it always did.
//
// unlock must be the release matching the lock held: unlock for the write lock, runlock for the
// read lock. Operations that already unlock through a defer call RunSideEffect instead — passing an
// unlock here as well would release it twice.
func RunSideEffectLocked(se func() error, release func()) error {
	if se == nil {
		return nil
	}
	if err := RunLocked(se, release); err != nil {
		return fmt.Errorf("%w: %w", ErrSideEffectFailed, err)
	}
	return nil
}

// RunBackup invokes a Backup method the way RunSideEffectLocked invokes a side effect: under the
// caller's lock, releasing it via release if the Backup ends the frame without returning, and
// tagging whatever it returns.
func RunBackup(fn func() error, release func()) error {
	return WrapBackup(RunLocked(fn, release))
}

// RunLocked invokes fn, which is caller-supplied code running while the queue's lock is held, and
// releases that lock if fn ends the frame without returning — a panic, or a runtime.Goexit. The
// panic then continues unwinding with its own stack, which is the caller's to catch; all this
// guarantees is that it does not carry the lock away with it and wedge the queue for everyone else.
//
// Deciding on a completion flag rather than on recover() is what covers Goexit, where recover()
// reports nothing. release must undo everything the caller's return paths would have undone: at
// minimum the matching unlock, and on Push also the reservation a WithOnAdmit hook is holding.
//
// Functions that unlock through a defer do not need this; passing a release there would unlock
// twice. It exists for Push, Pop, NotEmpty, NotFull and Close, which unlock explicitly.
//
// Two callers use it for something other than a lock, and the mechanism is the same: New wraps the
// Hydrate call so an unwind out of a Backup still closes the backing and the Backup, and the
// on-disk constructor wraps its index rebuild so an unwind out of Item.Hash still releases the bolt
// handle. Nothing about the completion flag is lock-specific — release is simply whatever the
// return paths would have released — and both of those leaks were previously invisible because the
// only cleanup hung off an error return that an unwind never reaches.
func RunLocked(fn func() error, release func()) (err error) {
	returned := false
	defer func() {
		if !returned {
			release()
		}
	}()
	err = fn()
	returned = true
	return err
}

// RunLockedNoErr invokes fn, which is caller-supplied code running while the queue's lock is held,
// and releases that lock if fn ends the frame without returning. It is RunLocked for the caller
// code that cannot report a failure: none of the four Item methods returns an error, so there is
// nothing for RunLocked's error plumbing to carry, but a panic or a runtime.Goexit out of one of
// them wedges the queue exactly the same way.
//
// The Item methods are reached indirectly as often as directly — through the heap and btree
// comparators, through a sort's less func, and through the index's add/remove — so the call being
// guarded is frequently a container operation with no Item method named in it at all.
//
// Deciding on a completion flag rather than on recover() is what covers Goexit, where recover()
// reports nothing, and it leaves a panic to keep unwinding with its original stack rather than
// being re-raised from here. release must undo everything the caller's return paths would have
// undone: at minimum the matching unlock. It must not itself call back into the caller's Item
// methods — they are what just failed, and failing again from inside a deferred call during a
// panic takes the process down with the lock still held.
//
// Functions that unlock through a defer do not need this; passing a release there would unlock
// twice.
func RunLockedNoErr(fn func(), release func()) {
	returned := false
	defer func() {
		if !returned {
			release()
		}
	}()
	fn()
	returned = true
}

// RunOnAdmit runs a WithOnAdmit hook for a batch of n items with lk released. The caller must hold
// lk's write lock on entry and holds it again on every return, including when the hook panics.
//
// inflight is bumped by n for as long as the hook runs, so the maxSize admission gate and NotFull
// both count the reservation and cannot overshoot the bound while nothing is inserted yet. The
// reservation is released on every exit. On any exit that does not go on to insert — an error from
// the hook, or a panic — a parked producer is signalled, because the released reservation may be
// exactly the capacity it is waiting for. The caller owes that same signal on any later exit of its
// own that abandons the push.
func RunOnAdmit(lk *QLock, inflight *int, n int, notFull *Signal, hook func() error) (err error) {
	*inflight += n
	lk.Unlock()
	returned := false
	defer func() {
		lk.Lock()
		*inflight -= n
		if !returned || err != nil {
			if notFull.HasWaiters() {
				notFull.Signal()
			}
		}
		if !returned {
			// The hook panicked or called runtime.Goexit. Either way this frame is being
			// unwound and the insert path that would have unlocked will never run, so
			// release here. Deciding on a flag rather than on recover() is what covers
			// Goexit — recover() returns nil there — and it leaves a panic to keep
			// unwinding with its original stack instead of being re-raised from here.
			lk.Unlock()
		}
	}()
	if err = hook(); err != nil {
		err = fmt.Errorf("%w: %w", ErrOnAdmitFailed, err)
	}
	returned = true
	return err
}
