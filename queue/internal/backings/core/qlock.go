package core

import (
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
)

// QLock is the shared lock for a Queue and its backing. The mutex is only ever held for
// short critical sections — never across a channel/ctx wait. pending counts writers
// currently blocked on the write lock; a copy-on-write RangeAll watches it to decide
// when to snapshot the remainder and release so a waiting writer can proceed. cowActive
// counts running COW iterators so the common case (no COW active) skips the pending
// atomics in lock() entirely.
type QLock struct {
	mu        sync.RWMutex
	pending   atomic.Int32
	cowActive atomic.Int32
}

// Lock acquires the write lock. When a COW iterator is active it bumps pending so
// writeWanted observes the blocked writer; otherwise it skips the two atomics because
// nobody is looking at writeWanted.
func (l *QLock) Lock() {
	if l.cowActive.Load() > 0 {
		l.pending.Add(1)
		l.mu.Lock()
		l.pending.Add(-1)
		return
	}
	l.mu.Lock()
}

// Unlock releases the write lock.
func (l *QLock) Unlock() { l.mu.Unlock() }

// RLock acquires the read lock.
func (l *QLock) RLock() { l.mu.RLock() }

// RUnlock releases the read lock.
func (l *QLock) RUnlock() { l.mu.RUnlock() }

// CowEnter / CowExit bracket an AllCOW iterator that intends to observe writeWanted.
// While at least one is active, lock() takes the guarded (pending-bump) path so the
// COW can detect a blocked writer.
func (l *QLock) CowEnter() { l.cowActive.Add(1) }

func (l *QLock) CowExit() { l.cowActive.Add(-1) }

// WriteWanted reports whether a writer is currently blocked waiting for the write lock.
// While a copy-on-write RangeAll holds the read lock a blocked writer cannot acquire, so
// pending stays > 0 until the reader releases — the signal cannot be missed. Only valid
// for COW iterators that have bracketed themselves with cowEnter / cowExit; outside that
// bracket pending is not maintained.
func (l *QLock) WriteWanted() bool { return l.pending.Load() > 0 }
