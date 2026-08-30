package queue

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/btype"
)

// TestRangeAllCOWParity verifies RangeAllCOW yields the same items as RangeAll on a
// quiescent queue, across the in-memory backings.
func TestRangeAllCOWParity(t *testing.T) {
	tests := []struct {
		name     string
		priority bool
		backing  func() (Backing[Number[int]], error)
	}{
		{"in-memory FIFO btree", false, func() (Backing[Number[int]], error) { return NewBTreeFIFO[Number[int]]() }},
		{"in-memory priority", true, func() (Backing[Number[int]], error) { return NewBTreePriority[Number[int]]() }},
		{"in-memory FIFO index", false, func() (Backing[Number[int]], error) { return NewBTreeFIFO[Number[int]](WithIndex()) }},
		{"in-memory FIFO btype", false, func() (Backing[Number[int]], error) { return btype.New[Number[int]]() }},
	}

	for _, test := range tests {
		ctx := t.Context()
		backing, err := test.backing()
		if err != nil {
			t.Fatalf("TestRangeAllCOWParity(%s): backing got err == %s, want nil", test.name, err)
		}
		q, err := New[Number[int]](ctx, "test", backing, 0)
		if err != nil {
			t.Fatalf("TestRangeAllCOWParity(%s): New got err == %s, want nil", test.name, err)
		}
		for _, n := range []int{5, 1, 4, 2, 3} {
			if _, err := q.Push(ctx, []Number[int]{itemFor(test.priority, n)}); err != nil {
				t.Fatalf("TestRangeAllCOWParity(%s): Push got err == %s", test.name, err)
			}
		}

		var plain, cow []int
		for v, err := range q.RangeAll(ctx) {
			if err != nil {
				t.Fatalf("TestRangeAllCOWParity(%s): RangeAll got err == %s", test.name, err)
			}
			plain = append(plain, v.V)
		}
		for v, err := range q.RangeAllCOW(ctx) {
			if err != nil {
				t.Fatalf("TestRangeAllCOWParity(%s): RangeAllCOW got err == %s", test.name, err)
			}
			cow = append(cow, v.V)
		}
		if len(plain) != len(cow) {
			t.Fatalf("TestRangeAllCOWParity(%s): RangeAll len %d != RangeAllCOW len %d", test.name, len(plain), len(cow))
		}
		for i := range plain {
			if plain[i] != cow[i] {
				t.Errorf("TestRangeAllCOWParity(%s): item %d: RangeAll=%d RangeAllCOW=%d", test.name, i, plain[i], cow[i])
			}
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestRangeAllCOWParity(%s): Close got err == %s", test.name, err)
		}
	}
}

// TestRangeAllCOWContention verifies that a concurrent writer does not deadlock a
// RangeAllCOW iteration and that the iteration observes a consistent snapshot (the item
// pushed by the contending writer is not yielded). Correctness holds regardless of
// scheduling: if contention is observed, COW snapshots and releases; if the loop finishes
// first, it simply ranged the original items. Either way: no deadlock, snapshot is the
// pre-contention set, and the late Push lands.
func TestRangeAllCOWContention(t *testing.T) {
	ctx := t.Context()
	backing, err := NewBTreeFIFO[Number[int]]()
	if err != nil {
		t.Fatalf("TestRangeAllCOWContention: NewBTreeFIFO got err == %s, want nil", err)
	}
	q, err := New[Number[int]](ctx, "test", backing, 0)
	if err != nil {
		t.Fatalf("TestRangeAllCOWContention: New got err == %s, want nil", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		if _, err := q.Push(ctx, []Number[int]{fifoItem(i)}); err != nil {
			t.Fatalf("TestRangeAllCOWContention: Push(%d) got err == %s", i, err)
		}
	}

	release := make(chan struct{})
	bDone := make(chan error, 1)
	go func() {
		<-release
		_, err := q.Push(ctx, []Number[int]{fifoItem(10_000)})
		bDone <- err
	}()

	var got []int
	first := true
	for v, err := range q.RangeAllCOW(ctx) {
		if err != nil {
			t.Fatalf("TestRangeAllCOWContention: RangeAllCOW got err == %s", err)
		}
		if first {
			close(release) // trigger the concurrent writer mid-iteration
			first = false
		}
		got = append(got, v.V)
	}

	if err := <-bDone; err != nil {
		t.Fatalf("TestRangeAllCOWContention: concurrent Push got err == %s, want nil", err)
	}

	sort.Ints(got)
	if len(got) != n {
		t.Fatalf("TestRangeAllCOWContention: snapshot len got %d, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != i {
			t.Fatalf("TestRangeAllCOWContention: snapshot item %d got %d, want %d", i, got[i], i)
			break
		}
	}
	if l := q.Len(); l != int64(n+1) {
		t.Errorf("TestRangeAllCOWContention: Len after concurrent Push got %d, want %d", l, n+1)
	}
	if err := q.Close(ctx); err != nil {
		t.Errorf("TestRangeAllCOWContention: Close got err == %s", err)
	}
}

// TestRangeAllCOWReleasesLock is the in-memory sibling of TestBboltRangeAllCOWReleasesLock, and it
// exists because the eight explicit release() calls inside fifo.go's, btree.go's and heap.go's
// AllCOW were pinned by nothing at all: every one of them is shadowed by the deferred release on
// the same closure, so the whole suite stayed green with all eight deleted. Deleting them does not
// leak the lock — it holds it across every yield, which is precisely the copy-on-write contract
// AllCOW exists to offer, and no test looked at the lock while an iteration was still running.
//
// The instrument is a body that costs real time. A backing that snapshots and releases lets the
// waiting writer through with items still to come; one that yields under the read lock cannot let
// it through until the last item has been handed over. How far the scan had got when the writer
// finally landed is the same number on an idle machine and a thrashing one, which is why it is
// asserted instead of elapsed time.
func TestRangeAllCOWReleasesLock(t *testing.T) {
	const (
		items    = 8
		perItem  = 10 * time.Millisecond
		register = 50 * time.Millisecond
	)

	for _, m := range memMakers() {
		ctx := t.Context()
		name := "TestRangeAllCOWReleasesLock(" + m.name + ")"
		q := m.make(t, ctx, Unlimited)
		for i := 0; i < items; i++ {
			if _, err := q.Push(ctx, []Number[int]{m.item(i)}); err != nil {
				t.Fatalf("%s: Push(%d) got err == %s, want err == nil", name, i, err)
			}
		}

		// seen counts what the iteration has yielded; seenWhenPushed records where it had got to
		// when the contending writer finally got through.
		var seen atomic.Int64
		var seenWhenPushed atomic.Int64
		pushed := make(chan error, 1)
		launched := false
		for _, err := range q.RangeAllCOW(ctx) {
			if err != nil {
				t.Fatalf("%s: iteration got err == %s, want err == nil", name, err)
			}
			if !launched {
				launched = true
				reached := make(chan struct{})
				go func() {
					close(reached) // about to enter Push, which then blocks on the write lock
					_, e := q.Push(ctx, []Number[int]{m.item(999)})
					seenWhenPushed.Store(seen.Load())
					pushed <- e
				}()
				// Wait for the writer to reach Push, then give it time to register as pending.
				// The btree and slice backings key their snapshot on writeWanted, so a writer the
				// scheduler had not run yet would leave a lock-holding AllCOW looking correct.
				<-reached
				time.Sleep(register)
			}
			seen.Add(1)
			time.Sleep(perItem)
		}

		select {
		case err := <-pushed:
			if err != nil {
				t.Errorf("%s: the concurrent Push got err == %s, want err == nil", name, err)
			}
		case <-time.After(10 * time.Second):
			// Reported without closing: the writer holds nothing Close can take back, and Close
			// would park on the same lock, losing the diagnosis to the package timeout.
			t.Errorf("%s: the concurrent Push never completed at all", name)
			continue
		}
		if got := seenWhenPushed.Load(); got >= items {
			t.Errorf("%s: the writer only got through once all %d items had been yielded; want it to proceed while the scan still had items left, which is what releasing the read lock for COW buys", name, items)
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("%s: Close got err == %s, want err == nil", name, err)
		}
	}
}

// TestRangeAllCOWReleasesLockWhenClosed pins the other explicit release: the one on AllCOW's
// ErrClosed exit, which every in-memory backing performs before yielding the error. That yield runs
// the caller's loop body like any other, so holding the read lock across it parks anything the body
// does to the queue — and the deferred release, which covers the unwind, does not fire until the
// body has already returned. TestRangeAllCOWReleasesLock cannot reach this path: it never gets past
// the closed check.
func TestRangeAllCOWReleasesLockWhenClosed(t *testing.T) {
	for _, m := range memMakers() {
		ctx := t.Context()
		name := "TestRangeAllCOWReleasesLockWhenClosed(" + m.name + ")"
		q := m.make(t, ctx, Unlimited)
		if _, err := q.Push(ctx, []Number[int]{m.item(1)}); err != nil {
			t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
		}
		if err := q.Close(ctx); err != nil {
			t.Fatalf("%s: Close got err == %s, want err == nil", name, err)
		}

		yielded := 0
		for _, err := range q.RangeAllCOW(ctx) {
			yielded++
			if !errors.Is(err, ErrClosed) {
				t.Errorf("%s: iteration got err == %v, want ErrClosed", name, err)
				break
			}
			// A writer, and a timer rather than a context: a leaked read lock still lets readers
			// through, and a Push parked in lk.lock() is on a plain mutex that no deadline reaches.
			// The Push is expected to fail with ErrClosed — that it returns at all is the property.
			alive := make(chan struct{})
			go func() {
				q.Push(ctx, []Number[int]{m.item(2)})
				close(alive)
			}()
			select {
			case <-alive:
			case <-time.After(5 * time.Second):
				t.Errorf("%s: a Push made from the iterator body never returned; AllCOW yielded ErrClosed still holding the read lock", name)
			}
		}
		if yielded != 1 {
			t.Errorf("%s: the iteration yielded %d times, want 1; the ErrClosed exit was never taken and this case proves nothing", name, yielded)
		}
	}
}

// TestRangeAllCOWReleasesLockOnCancel pins the third error exit: the context cancellation. Like the
// ErrClosed exit, that yield runs the caller's loop body, so holding the read lock across it parks
// anything the body does to the queue. It was previously the odd one out — fifo.go, heap.go and
// btype_fifo.go released before it, while btree.go and bbolt.go yielded from inside the tree Scan
// and the bbolt View callback, under the lock. Not that they always did: once a writer had
// contended, both switch to a snapshot and finish the range with the lock already dropped, so which
// of the two behaviors a caller saw came down to whether a writer had turned up first. It is now
// unconditional, which is what makes this test writable at all.
//
// Both branches are covered per backing, since they are the two that used to disagree: the
// uncontended case cancels with no writer in sight so the scan is still walking the live structure,
// and the contended case blocks a writer first — proving it got through before the cancel — so the
// range is finishing from the copy.
func TestRangeAllCOWReleasesLockOnCancel(t *testing.T) {
	const (
		items    = 8
		register = 50 * time.Millisecond
		settle   = 5 * time.Second
	)

	tests := []struct {
		name      string
		contended bool
	}{
		{"uncontended", false},
		{"contended", true},
	}

	for _, m := range queueMakers() {
		for _, test := range tests {
			base := t.Context()
			name := "TestRangeAllCOWReleasesLockOnCancel(" + m.name + "/" + test.name + ")"
			q := m.make(t, base, Unlimited)
			for i := 0; i < items; i++ {
				if _, err := q.Push(base, []Number[int]{m.item(i)}); err != nil {
					t.Fatalf("%s: Push(%d) got err == %s, want err == nil", name, i, err)
				}
			}

			// The iteration alone is canceled. Every writer below is handed base, because a Push on
			// a canceled context can refuse before it ever reaches the lock, which would make a
			// held lock look released.
			ctx, cancel := context.WithCancel(base)
			contender := make(chan error, 1)
			launched := false
			canceled := false
			errYields := 0
			for _, err := range q.RangeAllCOW(ctx) {
				if err != nil {
					errYields++
					if !errors.Is(err, context.Canceled) {
						t.Errorf("%s: iteration got err == %v, want context.Canceled", name, err)
						break
					}
					// A writer and a timer rather than a context, for the reason the ErrClosed case
					// gives: a leaked read lock still admits readers, and a Push parked in lk.lock()
					// is on a plain mutex no deadline reaches. That it returns at all is the property.
					alive := make(chan struct{})
					go func() {
						q.Push(base, []Number[int]{m.item(901)})
						close(alive)
					}()
					select {
					case <-alive:
					case <-time.After(settle):
						t.Errorf("%s: a Push made from the iterator body never returned; AllCOW yielded the cancellation still holding the read lock", name)
					}
					break
				}
				switch {
				case test.contended && !launched:
					launched = true
					reached := make(chan struct{})
					go func() {
						close(reached) // about to enter Push, which then blocks on the write lock
						_, e := q.Push(base, []Number[int]{m.item(900)})
						contender <- e
					}()
					// Wait for the writer to reach Push, then give it time to register as pending:
					// btree and bbolt key their snapshot on writeWanted, so a writer the scheduler
					// had not run yet would leave this case walking the live structure instead.
					<-reached
					time.Sleep(register)
				case test.contended && !canceled:
					// The writer is only through once the scan has copied the remainder and dropped
					// the lock, so this both waits for it and proves the copy path was taken.
					select {
					case e := <-contender:
						if e != nil {
							t.Errorf("%s: the contending Push got err == %s, want err == nil", name, e)
						}
					case <-time.After(settle):
						t.Errorf("%s: the contending Push never got through, so the iteration never took the copy-and-release path and this case proves nothing", name)
					}
					canceled = true
					cancel()
				default:
					canceled = true
					cancel()
				}
			}
			cancel()

			if errYields != 1 {
				t.Errorf("%s: the iteration yielded %d errors, want 1; the cancellation exit was never taken and this case proves nothing", name, errYields)
			}
			if err := q.Close(base); err != nil {
				t.Errorf("%s: Close got err == %s, want err == nil", name, err)
			}
		}
	}
}
