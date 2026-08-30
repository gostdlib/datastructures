package queue

import (
	"errors"
	"iter"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/bbolt"

	"github.com/gostdlib/base/context"
	"github.com/kylelemons/godebug/pretty"
)

// This file covers what itemlock_test.go deliberately does not. That file asks one question of every
// site where caller code runs under a queue lock — does the lock come back — and it answers it by
// checking that a later writer makes progress. A guard can pass that check and still be wrong: the
// unwind releases the lock and leaves the data structure half-mutated, with an entry in the tree and
// not in the index, an index entry for an item no longer in the queue, or a prefix of a batch that a
// Push documented as all-or-none never finished inserting. None of those produce an error, a panic or
// a race report; they produce a queue that quietly disagrees with itself.
//
// So the assertions here are on the invariant, never on the lock: Len, Exists, Del's count and what
// Pop actually hands back have to agree with each other, and with what the operation was supposed to
// leave behind. Every site is covered for a panic and for runtime.Goexit, because recover() reports
// nothing during a Goexit and a guard written around it silently covers only half of the cases.

// ivMethod names the ivItem method a row makes misbehave.
type ivMethod int32

const (
	ivNoMethod ivMethod = 0 // no method faults
	ivLess     ivMethod = 1 // ivItem.Less
	ivHash     ivMethod = 2 // ivItem.Hash
)

// ivMode is how the armed method ends the frame without returning.
type ivMode int32

const (
	ivReturn ivMode = 0 // the method returns normally
	ivPanic  ivMode = 1 // the method panics
	ivGoexit ivMode = 2 // the method calls runtime.Goexit
)

// ivFault is the arming control for ivItem. It is package level rather than a field on the item for
// the same reason itemFault is: an on-disk backing hands the site an item it decoded off its own
// storage, not the one the test pushed.
//
// after is what itemFault has no use for and this file cannot do without: the defects here are about
// a batch left half-applied, so the fault has to land in the middle of one. It lets the first `after`
// calls through and arms the one after that.
var ivFault struct {
	method atomic.Int32
	mode   atomic.Int32
	after  atomic.Int64
	calls  atomic.Int64
}

// ivArm arms method/mode to fire on call number after+1 and zeroes the call counter.
func ivArm(method ivMethod, mode ivMode, after int64) {
	ivFault.calls.Store(0)
	ivFault.after.Store(after)
	ivFault.mode.Store(int32(mode))
	ivFault.method.Store(int32(method))
}

// ivDisarm stops faulting and returns how many times the armed method ran.
func ivDisarm() int64 {
	ivFault.method.Store(int32(ivNoMethod))
	ivFault.mode.Store(int32(ivReturn))
	return ivFault.calls.Load()
}

// ivRun is called at the top of each instrumented ivItem method.
func ivRun(m ivMethod) {
	if ivMethod(ivFault.method.Load()) != m {
		return
	}
	if ivFault.calls.Add(1) <= ivFault.after.Load() {
		return
	}
	switch ivMode(ivFault.mode.Load()) {
	case ivPanic:
		panic("ivItem: the armed Item method exploded")
	case ivGoexit:
		runtime.Goexit()
	}
}

// ivItem is the Item for this file. V is identity (Equal/Hash) and P the priority.
type ivItem struct {
	V int
	P uint64
}

func (i ivItem) Less(o ivItem) bool {
	ivRun(ivLess)
	return i.P < o.P
}

func (i ivItem) Equal(o ivItem) bool { return i.V == o.V }

func (i ivItem) Priority() uint64 { return i.P }

func (i ivItem) Hash() uint64 {
	ivRun(ivHash)
	return uint64(i.V)
}

// ivFor builds an item for a backing kind: a priority backing requires Priority()>0 and a FIFO
// backing requires Priority()==0, and an item on the wrong side is rejected by validateKind before
// the operation ever takes the lock — a probe that reaches nothing.
func ivFor(priority bool, v int) ivItem {
	if priority {
		return ivItem{V: v, P: uint64(v)}
	}
	return ivItem{V: v}
}

func ivItems(priority bool, vs ...int) []ivItem {
	out := make([]ivItem, 0, len(vs))
	for _, v := range vs {
		out = append(out, ivFor(priority, v))
	}
	return out
}

// ivBackup is a Backup[ivItem] whose Del can be made to fail, for the rollback that only a refusing
// mirror can reach.
type ivBackup struct {
	delHook func() error
}

func (b *ivBackup) Push(ctx context.Context, vs []ivItem) error { return nil }

func (b *ivBackup) Del(ctx context.Context, vs []ivItem) error {
	if b.delHook != nil {
		return b.delHook()
	}
	return nil
}

func (b *ivBackup) Restore(ctx context.Context, vs []ivItem) error { return nil }

func (b *ivBackup) Close(ctx context.Context) error { return nil }

func (b *ivBackup) Clear(ctx context.Context) error { return nil }

func (b *ivBackup) RangeAll(ctx context.Context) iter.Seq2[ivItem, error] {
	return func(yield func(ivItem, error) bool) {}
}

func (b *ivBackup) OnLoad(ctx context.Context, v ivItem) error { return nil }

// ivProbe runs op on its own goroutine and waits for it to end, however it ends. A Goexit takes the
// goroutine with it, so the probe cannot be a plain call, and recover() is here only to keep a
// panicking row from taking the test binary down. It reports whether op returned normally.
func ivProbe(t *testing.T, name string, op func()) bool {
	t.Helper()

	returned := false
	// Written before the deferred close and read after it, so the channel orders the two.
	probed := make(chan struct{})
	go func() {
		defer close(probed)
		defer func() { recover() }()
		op()
		returned = true
	}()
	select {
	case <-probed:
	case <-time.After(10 * time.Second):
		ivDisarm()
		t.Fatalf("%s: the probe never returned", name)
	}
	return returned
}

// ivSorted returns everything the queue holds, by value, sorted. It reads through RangeAll, which
// walks the backing's own container rather than the hash index, so it is the ground truth an index
// is checked against rather than a second opinion from the same source.
func ivSorted(t *testing.T, ctx context.Context, name string, q *Queue[ivItem]) []int {
	t.Helper()

	var out []int
	for v, err := range q.RangeAll(ctx) {
		if err != nil {
			t.Fatalf("%s: RangeAll got err == %s, want err == nil", name, err)
		}
		out = append(out, v.V)
	}
	sort.Ints(out)
	return out
}

// ivDrain pops the whole queue one item at a time and returns the values in pop order. It stops at
// want items rather than blocking on an empty queue, so the caller has to say what it expects.
func ivDrain(t *testing.T, ctx context.Context, name string, q *Queue[ivItem], want int) []int {
	t.Helper()

	out := make([]int, 0, want)
	for i := 0; i < want; i++ {
		got, err := q.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("%s: Pop got err == %s, want err == nil", name, err)
		}
		for _, v := range got {
			out = append(out, v.V)
		}
	}
	return out
}

// ivIndexSite is one indexed-btree operation that reaches Item.Hash while holding the write lock.
type ivIndexSite struct {
	name     string
	priority bool
	// op is the operation the fault lands in the middle of.
	op func(ctx context.Context, q *Queue[ivItem], priority bool)
	// wantClean is what the queue holds after op runs unfaulted, wantUnwound after it unwinds.
	wantClean   []int
	wantUnwound []int
}

// ivIndexSites enumerates the indexed-btree sites. Push and Pop are the two operations that mutate
// the tree and the index together while holding the lock; Del and Exists release through a defer and
// mutate neither halfway.
func ivIndexSites() []ivIndexSite {
	push := func(ctx context.Context, q *Queue[ivItem], priority bool) {
		q.Push(ctx, ivItems(priority, 9, 10, 11))
	}
	pop := func(ctx context.Context, q *Queue[ivItem], priority bool) { q.Pop(ctx, 2) }
	return []ivIndexSite{
		{
			name:        "fifo-btree+index Push",
			op:          push,
			wantClean:   []int{1, 2, 3, 4, 9, 10, 11},
			wantUnwound: []int{1, 2, 3, 4},
		},
		{
			name:        "priority-btree+index Push",
			priority:    true,
			op:          push,
			wantClean:   []int{1, 2, 3, 4, 9, 10, 11},
			wantUnwound: []int{1, 2, 3, 4},
		},
		{
			name:        "fifo-btree+index Pop",
			op:          pop,
			wantClean:   []int{3, 4},
			wantUnwound: []int{1, 2, 3, 4},
		},
		{
			name:        "priority-btree+index Pop",
			priority:    true,
			op:          pop,
			wantClean:   []int{3, 4},
			wantUnwound: []int{1, 2, 3, 4},
		},
	}
}

// TestBTreeIndexAgreesWithTheTree covers the two ways an Item.Hash that ends the frame without
// returning used to leave the hash index and the btree describing different queues.
//
// On Push the tree insert had already landed when idx.add ran Hash, so the entry was in the tree and
// never in the index: Exists reported false for an item the queue was holding, Del removed nothing
// when asked for it, and nothing short of Pop or Clear could get it out again. On Pop it was the
// mirror image — PopMin had already removed the entry when idx.remove ran Hash, so the index kept a
// key for an item that was gone, Exists became a permanent false positive and Del reported removals
// that never happened.
//
// Neither shows up as an error and neither is repairable afterwards, because putting the entry back
// means re-running the caller code that just failed. So the assertion is the agreement itself: what
// RangeAll walks out of the container, what Len counts, what Exists answers, what Del removes and
// what Pop hands back all have to describe one queue.
func TestBTreeIndexAgreesWithTheTree(t *testing.T) {
	// The fault lands on the second Hash of the operation, so a batch is genuinely half-applied
	// rather than refused at its first item — the shape both defects need to appear.
	const faultAfter = 1

	tests := []struct {
		name string
		mode ivMode
		// wantUnwind says the probe is expected to end without a normal return.
		wantUnwind bool
	}{
		{
			// The anchor: without it nothing shows the harness reaches the site at all, and every
			// faulted row would pass for free against a site that is never exercised.
			name: "Success: an Item.Hash that returns normally leaves the index describing the tree",
			mode: ivReturn,
		},
		{
			name:       "Error: an Item.Hash that panics mid-batch must not leave the index describing a different queue",
			mode:       ivPanic,
			wantUnwind: true,
		},
		{
			name:       "Error: an Item.Hash that ends the goroutine mid-batch must not leave the index describing a different queue",
			mode:       ivGoexit,
			wantUnwind: true,
		},
	}

	// Wider than anything the sites push, so Exists and Del are asked about values that were never
	// in the queue as well as values that were.
	universe := []int{1, 2, 3, 4, 9, 10, 11, 12}

	for _, site := range ivIndexSites() {
		for _, test := range tests {
			ctx := t.Context()
			name := "TestBTreeIndexAgreesWithTheTree(" + site.name + "/" + test.name + ")"

			var b Backing[ivItem]
			var err error
			switch site.priority {
			case true:
				b, err = NewBTreePriority[ivItem](WithIndex())
			default:
				b, err = NewBTreeFIFO[ivItem](WithIndex())
			}
			if err != nil {
				t.Fatalf("%s: building the backing got err == %s, want err == nil", name, err)
			}
			q, err := New[ivItem](ctx, "", b, 0)
			if err != nil {
				t.Fatalf("%s: New got err == %s, want err == nil", name, err)
			}
			for _, v := range ivItems(site.priority, 1, 2, 3, 4) {
				if _, err := q.Push(ctx, []ivItem{v}); err != nil {
					t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
				}
			}

			ivArm(ivHash, test.mode, faultAfter)
			returned := ivProbe(t, name, func() { site.op(ctx, q, site.priority) })
			calls := ivDisarm()
			switch {
			case returned && test.wantUnwind:
				t.Errorf("%s: the probe returned normally, want the armed method to have ended the frame", name)
			case !returned && !test.wantUnwind:
				t.Errorf("%s: the probe did not return normally on a row that arms nothing", name)
			}
			if calls <= faultAfter {
				t.Errorf("%s: Item.Hash ran %d times, want more than %d — the fault never landed mid-batch", name, calls, faultAfter)
				q.Close(ctx)
				continue
			}

			want := site.wantClean
			if test.wantUnwind {
				want = site.wantUnwound
			}
			// The container itself, which is what Pop will hand back.
			got := ivSorted(t, ctx, name, q)
			if diff := pretty.Compare(want, got); diff != "" {
				t.Errorf("%s: contents -want +got:\n%s", name, diff)
			}
			if int64(len(got)) != q.Len() {
				t.Errorf("%s: Len == %d, want %d", name, q.Len(), len(got))
			}
			// The index, asked the same question the container was just asked.
			held := map[int]bool{}
			for _, v := range got {
				held[v] = true
			}
			for _, v := range universe {
				exists, err := q.Exists(ctx, ivFor(site.priority, v))
				if err != nil {
					t.Fatalf("%s: Exists got err == %s, want err == nil", name, err)
				}
				if exists != held[v] {
					t.Errorf("%s: Exists(%d) == %t, want %t", name, v, exists, held[v])
				}
			}
			// Del counts through the index too, so a count it cannot back up with a removal is the
			// same disagreement showing up as a number.
			count, err := q.Del(ctx, ivItems(site.priority, universe...))
			if err != nil {
				t.Fatalf("%s: Del got err == %s, want err == nil", name, err)
			}
			if count != len(want) {
				t.Errorf("%s: Del removed %d, want %d", name, count, len(want))
			}
			if q.Len() != 0 {
				t.Errorf("%s: Len after deleting everything == %d, want 0", name, q.Len())
			}
			if err := q.Close(ctx); err != nil {
				t.Errorf("%s: Close got err == %s, want err == nil", name, err)
			}
		}
	}
}

// TestPriorityHeapPushIsAllOrNone covers a Push that is documented all-or-none and was not. An
// Item.Less that ends the frame partway through a batch left the already-pushed prefix in the heap
// for a Push that never returned and never reported anything — a three item batch left one behind —
// while the Pop side of the same backing rolled its batch back, so the two directions disagreed about
// what an unwind means.
//
// Contents are only half of it. The rollback also has to leave a valid heap: dropping the new entries
// out of the array is enough to make Len and Exists agree and still leaves the survivors in an order
// no Pop will respect, so the check is that they come back out in priority order afterwards.
func TestPriorityHeapPushIsAllOrNone(t *testing.T) {
	// Past the first item of the batch, so the heap is genuinely half-updated when the fault lands.
	const faultAfter = 1

	tests := []struct {
		name string
		mode ivMode
		// wantUnwind says the probe is expected to end without a normal return.
		wantUnwind bool
	}{
		{
			name: "Success: an Item.Less that returns normally admits the whole batch",
			mode: ivReturn,
		},
		{
			name:       "Error: an Item.Less that panics mid-batch must leave none of the batch behind",
			mode:       ivPanic,
			wantUnwind: true,
		},
		{
			name:       "Error: an Item.Less that ends the goroutine mid-batch must leave none of the batch behind",
			mode:       ivGoexit,
			wantUnwind: true,
		},
	}

	// The batch interleaves with the seeds so a rollback that merely drops the new entries, rather
	// than undoing the moves they made, leaves the survivors out of order.
	seeds := []int{10, 20, 30, 40}
	batch := []int{5, 15, 25}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestPriorityHeapPushIsAllOrNone(" + test.name + ")"

		b, err := NewPriority[ivItem]()
		if err != nil {
			t.Fatalf("%s: NewPriority got err == %s, want err == nil", name, err)
		}
		q, err := New[ivItem](ctx, "", b, 0)
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		for _, v := range ivItems(true, seeds...) {
			if _, err := q.Push(ctx, []ivItem{v}); err != nil {
				t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
			}
		}

		ivArm(ivLess, test.mode, faultAfter)
		returned := ivProbe(t, name, func() { q.Push(ctx, ivItems(true, batch...)) })
		calls := ivDisarm()
		switch {
		case returned && test.wantUnwind:
			t.Errorf("%s: the probe returned normally, want the armed method to have ended the frame", name)
		case !returned && !test.wantUnwind:
			t.Errorf("%s: the probe did not return normally on a row that arms nothing", name)
		}
		if calls <= faultAfter {
			t.Errorf("%s: Item.Less ran %d times, want more than %d — the fault never landed mid-batch", name, calls, faultAfter)
			q.Close(ctx)
			continue
		}

		want := append(append([]int{}, batch...), seeds...)
		if test.wantUnwind {
			want = append([]int{}, seeds...)
		}
		sort.Ints(want)
		if int64(len(want)) != q.Len() {
			t.Errorf("%s: Len == %d, want %d", name, q.Len(), len(want))
		}
		// Popped, not scanned: pop order is the heap's whole contract, and a rollback that leaves
		// the right items in an invalid heap is only visible here.
		if diff := pretty.Compare(want, ivDrain(t, ctx, name, q, len(want))); diff != "" {
			t.Errorf("%s: pop order -want +got:\n%s", name, diff)
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("%s: Close got err == %s, want err == nil", name, err)
		}
	}
}

// TestPriorityHeapPopMirrorRefusedKeepsTheError covers a rollback that was throwing away the error it
// existed to report. When the Backup's Del refuses, the heap's Pop puts the popped items back and
// used to re-heapify them — through Item.Less, the caller's own code, on the frame that owed the
// caller ErrBackupFailed. An Item.Less that ended that frame took the error with it: the Pop that had
// been reporting the mirror's refusal cleanly reported nothing at all, and the caller never learned
// the mirror had said no. Nothing deferred can hand an error back out of a frame being destroyed, so
// the rollback has to be incapable of failing — it replays the heap's journaled moves and reaches no
// Item method.
//
// Every row therefore expects the same answer. That is the point: arming Less must make no
// difference, so the rows differ only in how it is armed.
func TestPriorityHeapPopMirrorRefusedKeepsTheError(t *testing.T) {
	tests := []struct {
		name string
		mode ivMode
	}{
		{
			name: "Success: a refused mirror with nothing armed reports ErrBackupFailed",
			mode: ivReturn,
		},
		{
			name: "Success: a refused mirror still reports ErrBackupFailed with Item.Less armed to panic",
			mode: ivPanic,
		},
		{
			name: "Success: a refused mirror still reports ErrBackupFailed with Item.Less armed to end the goroutine",
			mode: ivGoexit,
		},
	}

	// More items than the Pop asks for, so the rollback has to restore a heap rather than an empty
	// array: an ascending run of every item the queue holds is a valid heap by accident.
	seeds := []int{10, 20, 30, 40, 50}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestPriorityHeapPopMirrorRefusedKeepsTheError(" + test.name + ")"

		mirrorRefused := errors.New("the mirror refused the delete")
		bu := &ivBackup{}
		b, err := NewPriority[ivItem]()
		if err != nil {
			t.Fatalf("%s: NewPriority got err == %s, want err == nil", name, err)
		}
		q, err := New[ivItem](ctx, "", b, 0, WithBackup(bu))
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		for _, v := range ivItems(true, seeds...) {
			if _, err := q.Push(ctx, []ivItem{v}); err != nil {
				t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
			}
		}

		// Armed from inside Del, which is exactly the moment between the pop and the rollback, so
		// the pop itself runs unfaulted and it is only the rollback that meets the fault.
		bu.delHook = func() error {
			ivArm(ivLess, test.mode, 0)
			return mirrorRefused
		}
		var gotErr error
		returned := ivProbe(t, name, func() {
			_, err := q.Pop(ctx, 2)
			gotErr = err
		})
		bu.delHook = nil
		ivDisarm()

		if !returned {
			t.Errorf("%s: Pop did not return; the rollback ran caller code that ended the frame", name)
			continue
		}
		switch {
		case !errors.Is(gotErr, ErrBackupFailed):
			t.Errorf("%s: Pop got err == %v, want it to match ErrBackupFailed", name, gotErr)
		case !errors.Is(gotErr, mirrorRefused):
			t.Errorf("%s: Pop got err == %v, want the mirror's own error to stay reachable", name, gotErr)
		}
		// Nothing was removed, and the heap is a heap again: the rolled-back items have to come
		// back out in priority order, not merely be present.
		if int64(len(seeds)) != q.Len() {
			t.Errorf("%s: Len == %d, want %d", name, q.Len(), len(seeds))
		}
		if diff := pretty.Compare(seeds, ivDrain(t, ctx, name, q, len(seeds))); diff != "" {
			t.Errorf("%s: pop order after the rollback -want +got:\n%s", name, diff)
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("%s: Close got err == %s, want err == nil", name, err)
		}
	}
}

// TestBboltConstructorContextCancel covers a queue that a cancelled context left open and unusable.
// The on-disk backing's flusher is the only thing that can ever commit for that queue, and its
// shutdown arm returned without marking the backing closed or settling the batch it was holding.
// Cancelling the context handed to NewBboltFIFO/NewBboltPriority therefore killed the flusher while
// the queue went on reporting itself open: the next Push staged its items and blocked on a flush
// result nobody was left to produce — a wait that is documented as not context-cancelable, so the
// caller's own context could not free it either — and Close returned without releasing it.
//
// Close is unaffected because it marks the backing closed before it cancels, and the success row is
// the one that says so: a live context still builds a queue that accepts a Push and closes cleanly.
func TestBboltConstructorContextCancel(t *testing.T) {
	tests := []struct {
		name string
		// cancelCtx cancels the constructor's context before the Push.
		cancelCtx bool
		// wantClosed says the Push must be refused rather than accepted.
		wantClosed bool
	}{
		{
			name: "Success: a live constructor context leaves the queue accepting pushes",
		},
		{
			name:       "Error: a cancelled constructor context closes the queue instead of wedging it",
			cancelCtx:  true,
			wantClosed: true,
		},
	}

	for _, test := range tests {
		name := "TestBboltConstructorContextCancel(" + test.name + ")"
		// live is deliberately not the constructor's context: the point is that the queue itself
		// stops the caller, not that the caller's own context does.
		live := t.Context()
		ctx, cancel := context.WithCancel(live)

		b, err := NewBboltFIFO[ivItem](ctx, diskRoot(t))
		if err != nil {
			cancel()
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		q, err := New[ivItem](ctx, "", b, 0)
		if err != nil {
			cancel()
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		if test.cancelCtx {
			cancel()
			// Wait for the flusher to actually be gone, so the row tests the state it leaves
			// behind rather than racing it.
			select {
			case <-b.(*bbolt.Backing[ivItem]).FlusherDone():
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: the flusher never exited after its context was cancelled", name)
			}
		}

		// On a goroutine against a timer: a Push that has staged its items parks on a channel no
		// context deadline can reach.
		var pushErr error
		pushed := make(chan struct{})
		go func() {
			defer close(pushed)
			_, pushErr = q.Push(live, []ivItem{{V: 1}})
		}()
		select {
		case <-pushed:
		case <-time.After(20 * time.Second):
			// Reported without closing: the wedged Push holds nothing Close can take back, and
			// waiting on it here would lose the diagnosis to the package timeout.
			cancel()
			t.Errorf("%s: Push never returned; the queue reports itself open with no flusher left", name)
			continue
		}
		switch {
		case test.wantClosed && !errors.Is(pushErr, ErrClosed):
			t.Errorf("%s: Push got err == %v, want it to match ErrClosed", name, pushErr)
		case !test.wantClosed && pushErr != nil:
			t.Errorf("%s: Push got err == %s, want err == nil", name, pushErr)
		}

		closed := make(chan error, 1)
		go func() { closed <- q.Close(live) }()
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("%s: Close got err == %s, want err == nil", name, err)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("%s: Close never returned", name)
		}
		cancel()
	}
}
