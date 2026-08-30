package queue

import (
	"errors"
	"iter"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/worker"
	"github.com/gostdlib/base/context"
)

// faultMethod names the Item method a test row makes misbehave. Item methods are the one body of
// caller code a queue cannot be configured without — the constraint requires them — so reaching
// them needs a purpose-built item rather than an option or a hook.
type faultMethod int32

const (
	unknownFaultMethod faultMethod = 0 // no method faults
	faultLess          faultMethod = 1 // Item.Less
	faultHash          faultMethod = 2 // Item.Hash
)

// faultMode is how the selected method ends the frame without returning. Goexit is here because it
// is the mode a recover()-based guard silently fails to cover: recover() returns nil during a
// Goexit, so a guard that decides on it releases nothing.
type faultMode int32

const (
	unknownFaultMode faultMode = 0 // the method returns normally
	modePanic        faultMode = 1 // the method panics
	modeGoexit       faultMode = 2 // the method calls runtime.Goexit
)

// itemFault is the arming control for lockItem. It is package level rather than a field on the
// item because the on-disk backing round-trips items through a codec: the item whose Hash the
// on-disk Pop calls is the one it just decoded off disk, not the one the test pushed, so a fault
// carried on the pushed value would never reach the site. calls proves the armed method actually
// ran — without it a row whose site was never reached passes trivially, which is the failure mode
// this whole file exists to avoid.
var itemFault struct {
	method atomic.Int32
	mode   atomic.Int32
	calls  atomic.Int64
}

// itemFaultArm arms method/mode and zeroes the call counter.
func itemFaultArm(method faultMethod, mode faultMode) {
	itemFault.calls.Store(0)
	itemFault.mode.Store(int32(mode))
	itemFault.method.Store(int32(method))
}

// itemFaultDisarm stops faulting and returns how many times the armed method ran.
func itemFaultDisarm() int64 {
	itemFault.method.Store(int32(unknownFaultMethod))
	itemFault.mode.Store(int32(unknownFaultMode))
	return itemFault.calls.Load()
}

// itemFaultRun is called at the top of each instrumented lockItem method.
func itemFaultRun(m faultMethod) {
	if faultMethod(itemFault.method.Load()) != m {
		return
	}
	itemFault.calls.Add(1)
	switch faultMode(itemFault.mode.Load()) {
	case modePanic:
		panic("lockItem: the armed Item method exploded")
	case modeGoexit:
		runtime.Goexit()
	}
}

// lockItem is the Item for this file. V is identity (Equal/Hash) and P the priority; the JSON
// encoding of both exported fields is what the on-disk backing stores, so no WithCodec is needed.
type lockItem struct {
	V int
	P uint64
}

func (i lockItem) Less(o lockItem) bool {
	itemFaultRun(faultLess)
	return i.P < o.P
}

func (i lockItem) Equal(o lockItem) bool { return i.V == o.V }

// Priority is deliberately not instrumented. Its only under-lock call sites (validateKindOne during
// Hydrate, keyOf on the flusher) release through a defer or hold no queue lock at all, and arming it
// would fire in validateKind before Push ever takes the lock — a row that proves nothing.
func (i lockItem) Priority() uint64 { return i.P }

func (i lockItem) Hash() uint64 {
	itemFaultRun(faultHash)
	return uint64(i.V)
}

// lockSite is one place a queue lock is held across a call that reaches an Item method, on a path
// that releases the lock with a plain statement rather than a defer. The Item method is usually
// reached indirectly — through a heap or btree comparator, a sort's less func, or the hash index —
// so the guarded line names no Item method at all.
type lockSite struct {
	name     string
	priority bool
	method   faultMethod
	backing  func(t *testing.T, ctx context.Context) (Backing[lockItem], error)
	// op reaches the site. It runs on a probe goroutine and is expected to unwind. priority is the
	// site's backing kind: an item whose Priority does not match it is rejected by validateKind
	// before Push ever takes the lock, which is a probe that reaches nothing.
	op func(ctx context.Context, q *Queue[lockItem], priority bool)
}

func lockSiteOps() (push, pop, rangeCOW func(ctx context.Context, q *Queue[lockItem], priority bool)) {
	push = func(ctx context.Context, q *Queue[lockItem], priority bool) {
		q.Push(ctx, []lockItem{lockItemFor(priority, 9)})
	}
	pop = func(ctx context.Context, q *Queue[lockItem], priority bool) { q.Pop(ctx, 1) }
	rangeCOW = func(ctx context.Context, q *Queue[lockItem], priority bool) {
		for range q.RangeAllCOW(ctx) {
		}
	}
	return push, pop, rangeCOW
}

// lockSites enumerates the sites from the Item interface outward: every method Item declares, every
// call site of each, and of those the ones where a queue lock is held and released by a plain
// statement. Sites released through a defer (Exists, Del, Hydrate, All, Clear on every backing) are
// safe by construction and are not listed.
func lockSites() []lockSite {
	push, pop, rangeCOW := lockSiteOps()
	memPriority := func(idx bool) func(*testing.T, context.Context) (Backing[lockItem], error) {
		return func(t *testing.T, ctx context.Context) (Backing[lockItem], error) {
			return NewBTreePriority[lockItem](indexOpts(idx)...)
		}
	}
	return []lockSite{
		{
			name:     "priority-heap Push reaches Item.Less through heap.Push",
			priority: true,
			method:   faultLess,
			backing:  func(t *testing.T, ctx context.Context) (Backing[lockItem], error) { return NewPriority[lockItem]() },
			op:       push,
		},
		{
			name:     "priority-heap Pop reaches Item.Less through heap.Pop",
			priority: true,
			method:   faultLess,
			backing:  func(t *testing.T, ctx context.Context) (Backing[lockItem], error) { return NewPriority[lockItem]() },
			op:       pop,
		},
		{
			name:     "priority-heap AllCOW reaches Item.Less through sortedSnapshot's sort",
			priority: true,
			method:   faultLess,
			backing:  func(t *testing.T, ctx context.Context) (Backing[lockItem], error) { return NewPriority[lockItem]() },
			op:       rangeCOW,
		},
		{
			name:     "priority-btree Push reaches Item.Less through tree.Load",
			priority: true,
			method:   faultLess,
			backing:  memPriority(false),
			op:       push,
		},
		{
			name:     "priority-btree+index Push reaches Item.Less through tree.Load",
			priority: true,
			method:   faultLess,
			backing:  memPriority(true),
			op:       push,
		},
		{
			name:     "priority-btree+index Push reaches Item.Hash through idx.add",
			priority: true,
			method:   faultHash,
			backing:  memPriority(true),
			op:       push,
		},
		{
			name:     "priority-btree+index Pop reaches Item.Hash through idx.remove",
			priority: true,
			method:   faultHash,
			backing:  memPriority(true),
			op:       pop,
		},
		{
			name:     "fifo-btree+index Push reaches Item.Hash through idx.add",
			priority: false,
			method:   faultHash,
			backing: func(t *testing.T, ctx context.Context) (Backing[lockItem], error) {
				return NewBTreeFIFO[lockItem](WithIndex())
			},
			op: push,
		},
		{
			name:     "fifo-btree+index Pop reaches Item.Hash through idx.remove",
			priority: false,
			method:   faultHash,
			backing: func(t *testing.T, ctx context.Context) (Backing[lockItem], error) {
				return NewBTreeFIFO[lockItem](WithIndex())
			},
			op: pop,
		},
		{
			name:     "fifo-bbolt+index Pop reaches Item.Hash through idx.remove",
			priority: false,
			method:   faultHash,
			backing: func(t *testing.T, ctx context.Context) (Backing[lockItem], error) {
				return NewBboltFIFO[lockItem](ctx, diskRoot(t), WithIndex())
			},
			op: pop,
		},
		{
			name:     "priority-bbolt+index Pop reaches Item.Hash through idx.remove",
			priority: true,
			method:   faultHash,
			backing: func(t *testing.T, ctx context.Context) (Backing[lockItem], error) {
				return NewBboltPriority[lockItem](ctx, diskRoot(t), WithIndex())
			},
			op: pop,
		},
	}
}

// lockItemFor builds a seed item for the site's backing kind.
func lockItemFor(priority bool, v int) lockItem {
	if priority {
		return lockItem{V: v, P: uint64(v)}
	}
	return lockItem{V: v}
}

// TestItemMethodLockRelease covers the fifth body of caller code under a queue lock: the Item's own
// methods. Every other body — the Backup, the codec, the side effect, the WithOnAdmit hook — is
// something a caller opts into and can therefore be guarded at a named call. Item methods are
// required by the type constraint and are reached indirectly, through heap and btree comparators,
// through a sort's less func and through the hash index, so a sweep for the method names misses
// them and the guards were missing at every site below.
//
// The assertion is not that the unwind propagated — it always did, and a test that checks only that
// passes against the bug. It is that a LATER writer still makes progress: a lost lock produces no
// error and no race report, only silence. Every wait is a goroutine against a timer rather than a
// context deadline, because a Push parked inside lk.lock() cannot observe a context deadline.
//
// Goexit is a row of its own because it is what defeats a recover()-based fix: recover() returns nil
// during a Goexit, so a guard that decides on it releases nothing.
func TestItemMethodLockRelease(t *testing.T) {
	tests := []struct {
		name string
		mode faultMode
		// wantUnwind says the probe is expected to end without a normal return.
		wantUnwind bool
	}{
		{
			// The anchor. Without it nothing shows the harness reaches the site at all, rather than
			// that the site is unreachable and every row passes for free.
			name: "Success: an Item method that returns normally leaves the queue usable",
			mode: unknownFaultMode,
		},
		{
			name:       "Error: an Item method that panics must not carry the lock away",
			mode:       modePanic,
			wantUnwind: true,
		},
		{
			name:       "Error: an Item method that calls runtime.Goexit must not carry the lock away",
			mode:       modeGoexit,
			wantUnwind: true,
		},
	}

	for _, site := range lockSites() {
		for _, test := range tests {
			ctx := t.Context()
			name := "TestItemMethodLockRelease(" + site.name + "/" + test.name + ")"

			b, err := site.backing(t, ctx)
			if err != nil {
				t.Fatalf("%s: building the backing got err == %s, want err == nil", name, err)
			}
			// Unbounded: a site that leaves the queue's accounting mid-operation must not be able
			// to block the liveness probe on capacity instead of on the lock.
			q, err := New[lockItem](ctx, "", b, 0)
			if err != nil {
				t.Fatalf("%s: New got err == %s, want err == nil", name, err)
			}
			// Seeded before arming: an empty heap or tree does no comparisons, so a probe against
			// one would never reach Item.Less.
			for i := 1; i <= 4; i++ {
				if _, err := q.Push(ctx, []lockItem{lockItemFor(site.priority, i)}); err != nil {
					t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
				}
			}

			itemFaultArm(site.method, test.mode)
			// Written before the deferred close and read after it, so the channel orders the two.
			returned := false
			probed := make(chan struct{})
			go func() {
				// LIFO: recover first so a panicking row does not take the test binary down, then
				// close. close runs on a Goexit too, which is the point of a deferred close here.
				defer close(probed)
				defer func() { recover() }()
				site.op(ctx, q, site.priority)
				returned = true
			}()
			select {
			case <-probed:
			case <-time.After(10 * time.Second):
				itemFaultDisarm()
				t.Fatalf("%s: the probe never returned", name)
			}
			calls := itemFaultDisarm()
			switch {
			case returned && test.wantUnwind:
				t.Errorf("%s: the probe returned normally, want the armed method to have ended the frame", name)
			case !returned && !test.wantUnwind:
				t.Errorf("%s: the probe did not return normally on a row that arms nothing", name)
			}
			if calls == 0 {
				t.Errorf("%s: the armed Item method never ran, so this row proves nothing", name)
				q.Close(ctx)
				continue
			}

			// The property. A writer on a fresh goroutine, bounded by a timer: a leaked lock parks
			// it inside lk.lock(), where no context deadline can reach it.
			alive := make(chan struct{})
			go func() {
				q.Push(ctx, []lockItem{lockItemFor(site.priority, 100)})
				close(alive)
			}()
			select {
			case <-alive:
			case <-time.After(5 * time.Second):
				// Reported without closing: Close parks on the very lock that was lost, and the
				// test would then die of the package timeout with the diagnosis unflushed.
				t.Errorf("%s: a later Push never completed; the unwind carried the lock away", name)
				continue
			}
			q.Close(ctx)
		}
	}
}

// TestNewWithLimitedPool covers a queue construction that never returned. The on-disk backing runs
// its flusher for the whole life of the queue, and it was submitted to whatever pool the context
// carried. On a worker.Pool.Limited that meant each live queue permanently held one of the pool's
// slots, so New for the (limit+1)th queue blocked forever — and the submit context has its
// cancellation stripped, so the caller's own ctx could not break the tie either.
//
// It lives in this file for want of a collision-free name of its own, not because it is about Item
// methods.
func TestNewWithLimitedPool(t *testing.T) {
	tests := []struct {
		name string
		// limit is the Limited pool size; 0 means use the context's default (unlimited) pool.
		limit int
		// queues is how many queues to build against that one pool.
		queues int
	}{
		{
			name:   "Success: an unlimited pool builds more queues than any bound",
			queues: 4,
		},
		{
			name:   "Success: a Limited pool builds more queues than its own limit",
			limit:  2,
			queues: 4,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestNewWithLimitedPool(" + test.name + ")"
		if test.limit > 0 {
			ctx = context.SetPool(ctx, worker.Default().Limited(ctx, "queue-flusher-test", test.limit))
		}
		// Every queue is held open until the whole batch is built, and only then closed. Closing
		// each one before building the next handed its slot straight back, so at most one flusher
		// was ever live and the (limit+1)th build never had to compete for anything: the test
		// passed with the fix reverted, which is the one failure this file exists to prevent.
		type built struct {
			q   *Queue[Number[int]]
			err error
		}
		queues := make([]*Queue[Number[int]], 0, test.queues)
		for i := 0; i < test.queues; i++ {
			ch := make(chan built, 1)
			go func() {
				b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
				if err != nil {
					ch <- built{err: err}
					return
				}
				q, err := New[Number[int]](ctx, "", b, 0)
				ch <- built{q: q, err: err}
			}()
			stalled := false
			select {
			case got := <-ch:
				switch {
				case got.err != nil:
					t.Errorf("%s: queue %d got err == %s, want err == nil", name, i, got.err)
				default:
					queues = append(queues, got.q)
				}
			case <-time.After(20 * time.Second):
				// A goroutine and a timer, not a context deadline: New parks inside the pool's
				// semaphore acquire on a context whose cancellation was stripped.
				t.Errorf("%s: queue %d never finished being built", name, i)
				stalled = true
			}
			if stalled {
				// The pool is exhausted; every later build would wait out the same timeout and
				// report the same thing.
				break
			}
		}
		for i, q := range queues {
			if err := q.Close(ctx); err != nil {
				t.Errorf("%s: queue %d Close got err == %s, want err == nil", name, i, err)
			}
		}
	}
}

// lockBackup is a Backup[lockItem] whose Del can be made to fail. It exists for one site that no
// backing reaches without a Backup: the rollback the heap's Pop runs when the mirror refuses.
type lockBackup struct {
	delHook func() error
}

func (b *lockBackup) Push(ctx context.Context, vs []lockItem) error { return nil }

func (b *lockBackup) Del(ctx context.Context, vs []lockItem) error {
	if b.delHook != nil {
		return b.delHook()
	}
	return nil
}

func (b *lockBackup) Restore(ctx context.Context, vs []lockItem) error { return nil }

func (b *lockBackup) Close(ctx context.Context) error { return nil }

func (b *lockBackup) Clear(ctx context.Context) error { return nil }

func (b *lockBackup) RangeAll(ctx context.Context) iter.Seq2[lockItem, error] {
	return func(yield func(lockItem, error) bool) {}
}

func (b *lockBackup) OnLoad(ctx context.Context, v lockItem) error { return nil }

// TestPriorityHeapPopRestoreReleasesTheLock covers the rollback the heap's Pop runs when the mirror
// refuses, which only a failing Backup can reach. It used to be an Item-method site: the rollback put
// the popped items back and re-heapified through Item.Less while holding the write lock it releases
// with a plain statement. It is not one any more, and this test now pins that it stays that way —
// re-heapifying there let an unwinding Less destroy the frame that owed the caller the mirror's
// error, so the rollback replays the heap's journaled moves instead and reaches no Item method at
// all. The invariant that error carries is asserted in invariant_test.go; what is checked here is
// the two things this file is about: the fault is never reached, and the lock comes back.
//
// The fault is armed from inside the Backup's Del, which is exactly the moment between the pop and
// the rollback, so the pop itself still runs unfaulted.
func TestPriorityHeapPopRestoreReleasesTheLock(t *testing.T) {
	tests := []struct {
		name string
		mode faultMode
	}{
		{
			name: "Success: a mirror failure whose rollback runs cleanly leaves the queue usable",
			mode: unknownFaultMode,
		},
		{
			name: "Success: an Item.Less armed to panic is never reached by the rollback",
			mode: modePanic,
		},
		{
			name: "Success: an Item.Less armed to end the goroutine is never reached by the rollback",
			mode: modeGoexit,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestPriorityHeapPopRestoreReleasesTheLock(" + test.name + ")"

		mirrorRefused := errors.New("the mirror refused the delete")
		bu := &lockBackup{}
		b, err := NewPriority[lockItem]()
		if err != nil {
			t.Fatalf("%s: NewPriority got err == %s, want err == nil", name, err)
		}
		q, err := New[lockItem](ctx, "", b, 0, WithBackup(bu))
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		for i := 1; i <= 4; i++ {
			if _, err := q.Push(ctx, []lockItem{lockItemFor(true, i)}); err != nil {
				t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
			}
		}

		// Armed from inside Del so the pop that precedes the restore runs unfaulted.
		bu.delHook = func() error {
			itemFaultArm(faultLess, test.mode)
			return mirrorRefused
		}
		probed := make(chan struct{})
		go func() {
			defer close(probed)
			defer func() { recover() }()
			q.Pop(ctx, 1)
		}()
		select {
		case <-probed:
		case <-time.After(10 * time.Second):
			itemFaultDisarm()
			t.Fatalf("%s: the probe never returned", name)
		}
		bu.delHook = nil
		if calls := itemFaultDisarm(); calls != 0 {
			t.Errorf("%s: the rollback reached Item.Less %d times, want 0 — it must run no caller code", name, calls)
		}

		alive := make(chan struct{})
		go func() {
			q.Push(ctx, []lockItem{lockItemFor(true, 100)})
			close(alive)
		}()
		select {
		case <-alive:
		case <-time.After(5 * time.Second):
			t.Errorf("%s: a later Push never completed; the unwind carried the lock away", name)
			continue
		}
		q.Close(ctx)
	}
}
