package heap

import (
	"container/heap"
	"fmt"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
	"iter"
	"sort"

	"github.com/gostdlib/base/context"
)

// minHeap implements container/heap.Interface ordered by core.PrioritySeqLess (defined in btree.go).
type minHeap[T core.Item[T]] struct {
	items []core.SeqItem[T]
	// swaps journals, as index pairs, every exchange Swap made while journal was set. container/
	// heap mutates the array only through Swap and through this type's own Push/Pop, so replaying
	// the journal backwards undoes a Push or Pop batch move for move. That is the whole point:
	// rolling back by re-heapifying would run Item.Less, and the rollbacks below happen exactly
	// when Less has either just failed or is about to be asked to fail again.
	swaps   []int
	journal bool
}

func (h *minHeap[T]) Len() int           { return len(h.items) }
func (h *minHeap[T]) Less(i, j int) bool { return core.PrioritySeqLess(h.items[i], h.items[j]) }

func (h *minHeap[T]) Swap(i, j int) {
	if h.journal {
		h.swaps = append(h.swaps, i, j)
	}
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// journalOn starts recording moves for a mutation that may have to be rolled back. journalOff ends
// the recording once the mutation is final. Both are called with the queue's write lock held, which
// is what makes a single shared journal enough: only one mutation is ever in flight.
func (h *minHeap[T]) journalOn() {
	h.journal = true
	h.swaps = h.swaps[:0]
}

func (h *minHeap[T]) journalOff() {
	h.journal = false
	h.swaps = h.swaps[:0]
}

// rollback undoes the journaled mutation, leaving the array exactly as it was: length origLen, every
// element back where it started, and therefore a valid heap again. popped holds the values the pops
// removed, newest last, and is nil for a rolled-back Push.
//
// It runs no Item method, which is the requirement — it is called from unwind releases, where the
// caller's Less has just ended a frame, and from the Backup-refused path, where re-running Less
// would let a second failure discard the very error being reported. Replay is done at whichever
// length is larger, because a Push's journal holds indices past origLen and a Pop's holds values
// past the current length.
func (h *minHeap[T]) rollback(origLen int, popped []core.SeqItem[T]) {
	work := len(h.items)
	if origLen > work {
		// Pop only reslices, so the capacity for the entries it removed is still there.
		work = origLen
	}
	h.items = h.items[:work]
	// The pops emptied the top slots in order, so the i-th pop took the entry at origLen-1-i.
	for i, x := range popped {
		h.items[origLen-1-i] = x
	}
	for i := len(h.swaps) - 2; i >= 0; i -= 2 {
		a, b := h.swaps[i], h.swaps[i+1]
		h.items[a], h.items[b] = h.items[b], h.items[a]
	}
	// A rolled-back Push must not leave the caller's items reachable through the array's tail.
	var zero core.SeqItem[T]
	for i := origLen; i < work; i++ {
		h.items[i] = zero
	}
	h.items = h.items[:origLen]
	h.journalOff()
}

func (h *minHeap[T]) Push(x any) {
	h.items = append(h.items, x.(core.SeqItem[T]))
}

func (h *minHeap[T]) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	var zero core.SeqItem[T]
	h.items[n-1] = zero
	h.items = h.items[:n-1]
	return x
}

// priorityHeap is an in-memory priority queue backed by container/heap. For unbounded or
// very large priority queues prefer NewBTreePriority, whose tree avoids the heap's
// reheapify cost on Del.
//
// Items pop in Item.Less order; ties break by insert sequence.
// Semantics (blocking, hydration, backup mirror) mirror fifo. lk and maxSize are injected
// by New via SetQueueLock and SetMaxSize before any other use.
type Backing[T core.Item[T]] struct {
	core.Seal
	lk      *core.QLock
	h       minHeap[T]
	nextSeq uint64
	maxSize int
	// hydrateEnd is the exclusive seq boundary of the restore: an entry is a restored one exactly
	// when its seq is below this. seq is assigned monotonically and Hydrate runs before the queue
	// is visible to anyone, so the boundary classifies every entry for the life of the queue.
	// Priority ordering means restored entries are not necessarily the ones that leave first,
	// which is why this backing classifies by seq rather than by position.
	hydrateEnd uint64
	// hydrated is how much of the current contents came from Hydrate and is therefore exempt
	// from maxSize. A restore is not admission: the items already existed, so refusing them or
	// blocking every later Push until they drain would make a bounded queue unusable after a
	// restart. The exemption decays as the queue drains (see dropHydrated) so the bound takes
	// full effect again once the restored surplus is gone.
	hydrated int
	// inflight is the number of items a Push has reserved capacity for but not yet inserted,
	// because a WithOnAdmit hook is running with the lock released. The maxSize admission gate
	// and NotFull both count it, so a reservation holds the bound exactly as a queued item would
	// and concurrent Pushes cannot overshoot while a hook is in flight.
	inflight int
	notFull  *core.Signal
	notEmpty *core.Signal
	closed   bool
	backup   core.Backup[T]
}

// NewPriority returns an in-memory priority Backing backed by container/heap. Items pop in
// Item.Less order, ties broken by insert order. Pass the result to New.
func New[T core.Item[T]]() (core.Backing[T], error) {
	return &Backing[T]{
		lk:       &core.QLock{},
		notFull:  core.NewSignal(),
		notEmpty: core.NewSignal(),
	}, nil
}

func (p *Backing[T]) SetQueueLock(lk *core.QLock) { p.lk = lk }

func (p *Backing[T]) SetMaxBatch(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: max batch must be at least 1, got %d", core.ErrBadOption, n)
	}
	return nil
}

func (p *Backing[T]) SetMaxSize(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: max size must be at least 0, got %d", core.ErrBadOption, n)
	}
	p.maxSize = n
	return nil
}

// Hydrate implements Backing.Hydrate().
func (p *Backing[T]) Hydrate(ctx context.Context, b core.Backup[T]) error {
	p.lk.Lock()
	defer p.lk.Unlock()
	// Set on every exit, not just the happy one: a partial load that errors out would otherwise
	// leave hydrated > 0 with the boundary at 0, and an exemption that can never decay.
	defer func() { p.hydrateEnd = p.nextSeq }()
	// Append in backup order, then heapify once: heap.Init is O(n) (Floyd build-heap)
	// vs O(n log n) for a per-item heap.Push. seq is assigned in RangeAll order so the
	// pop order (core.PrioritySeqLess) is unchanged regardless of the post-Init slice layout.
	for v, err := range b.RangeAll(ctx) {
		if err != nil {
			return core.WrapBackup(err)
		}
		if err := core.ValidateKindOne(true, v); err != nil {
			return err
		}
		if err := b.OnLoad(ctx, v); err != nil {
			return core.WrapBackup(err)
		}
		p.h.items = append(p.h.items, core.SeqItem[T]{Seq: p.nextSeq, Item: v})
		p.nextSeq++
		p.hydrated++
	}
	heap.Init(&p.h)
	p.backup = b
	return nil
}

// Push implements Backing.Push().
func (p *Backing[T]) Push(ctx context.Context, vs []T, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallPush, options)
	if err != nil {
		return err
	}
	if err := core.ValidateKind(true, vs); err != nil {
		return err
	}
	for {
		p.lk.Lock()
		if p.closed {
			p.lk.Unlock()
			return core.ErrClosed
		}
		if p.maxSize > 0 && len(vs) > p.maxSize {
			p.lk.Unlock()
			return core.ErrBatchTooLarge
		}
		if p.maxSize == 0 || p.h.Len()-p.hydrated+p.inflight+len(vs) <= p.maxSize {
			if opts.OnAdmit != nil {
				// Reserve the capacity, drop the lock for the caller's hook, then retake it
				// to insert. The reservation is what lets the lock go: concurrent Pushes and
				// NotFull count inflight, so the bound holds even with nothing yet inserted.
				// runOnAdmit releases the reservation on every exit, a panicking hook
				// included, and returns holding the lock.
				if err := core.RunOnAdmit(p.lk, &p.inflight, len(vs), p.notFull, opts.OnAdmit); err != nil {
					p.lk.Unlock()
					return err
				}
				if p.closed {
					// Closed while the hook ran. The hook cannot be un-run, the same as a
					// backup mirror failing after a side effect. Close already broadcasts to
					// every parked waiter, so this signal is redundant — it is here so that
					// every exit which gives a reservation back signals, checkable by reading
					// Push alone rather than by reasoning about Close.
					opts.ReleaseReservation(p.notFull)
					p.lk.Unlock()
					return core.ErrClosed
				}
			}
			// The unwind callback owes the same debt the error return below does: if the side
			// effect panics or calls runtime.Goexit, the reservation is handed back with nothing
			// inserted, and a producer parked on exactly that capacity has to be woken. Releasing
			// only the lock leaves it parked on a queue with room in it.
			if err := core.RunSideEffectLocked(opts.SideEffect, func() { opts.ReleaseReservation(p.notFull); p.lk.Unlock() }); err != nil {
				opts.ReleaseReservation(p.notFull)
				p.lk.Unlock()
				return err
			}
			if p.backup != nil {
				// A Backup is caller-supplied code holding the queue's lock, so it gets the
				// same unwind guard the side effect does, owing the same reservation debt.
				push := func() error { return p.backup.Push(ctx, vs) }
				if err := core.RunBackup(push, func() { opts.ReleaseReservation(p.notFull); p.lk.Unlock() }); err != nil {
					opts.ReleaseReservation(p.notFull)
					p.lk.Unlock()
					return err
				}
			}
			wasEmpty := p.h.Len() == 0
			// heap.Push reaches the caller's Item.Less through the heap comparator, and this path
			// unlocks explicitly, so an unwind out of Less would carry the lock away and wedge
			// every later operation on the queue.
			//
			// The release rolls the whole batch back, because Push is documented all-or-none and a
			// Less that unwinds on the second of three items had been leaving the first one in the
			// heap for a Push that never returned. It rolls back through the journal rather than by
			// dropping the new entries: dropping them leaves the array holding the right items in
			// the wrong order, and replaying the recorded moves backwards restores a valid heap
			// without asking the comparator that just failed for one more answer.
			//
			// It owes the same reservation debt the abandon paths above owe: nothing was inserted,
			// so a producer parked on exactly this capacity has to be woken.
			startLen, startSeq := p.h.Len(), p.nextSeq
			p.h.journalOn()
			core.RunLockedNoErr(func() {
				for _, v := range vs {
					heap.Push(&p.h, core.SeqItem[T]{Seq: p.nextSeq, Item: v})
					p.nextSeq++
				}
			}, func() {
				p.h.rollback(startLen, nil)
				p.nextSeq = startSeq
				opts.ReleaseReservation(p.notFull)
				p.lk.Unlock()
			})
			p.h.journalOff()
			if wasEmpty && p.notEmpty.HasWaiters() {
				p.notEmpty.Signal()
			}
			p.lk.Unlock()
			return nil
		}
		if err := p.notFull.Wait(ctx, p.lk.Unlock); err != nil {
			return p.ClosedOrCause(ctx)
		}
	}
}

// Pop implements Backing.Pop().
func (p *Backing[T]) Pop(ctx context.Context, n int, options ...core.OpOption) ([]T, error) {
	opts, err := core.ResolveOpOptions(core.CallPop, options)
	if err != nil {
		return nil, err
	}
	for {
		p.lk.Lock()
		if p.closed {
			p.lk.Unlock()
			return nil, core.ErrClosed
		}
		if p.h.Len() > 0 {
			k := n
			if k > p.h.Len() {
				k = p.h.Len()
			}
			// The side effect runs before anything is removed, so a failure aborts
			// with nothing removed.
			if err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock); err != nil {
				p.lk.Unlock()
				return nil, err
			}
			popped := make([]core.SeqItem[T], 0, k)
			out := make([]T, 0, k)
			startLen := p.h.Len()
			// heap.Pop reaches the caller's Item.Less through the heap comparator, and this path
			// unlocks explicitly. The journal makes both rollbacks below move-for-move exact: the
			// heap comes back a valid heap with the caller's items in it, and nothing in the undo
			// asks Item.Less for another answer.
			p.h.journalOn()
			// restore is the single undo for both of the ways this Pop can abandon: an unwind out
			// of the comparator, and a Backup that refuses the mirror. Sharing one closure is what
			// keeps the two from drifting.
			restore := func() { p.h.rollback(startLen, popped); p.lk.Unlock() }
			core.RunLockedNoErr(func() {
				for i := 0; i < k; i++ {
					x := heap.Pop(&p.h).(core.SeqItem[T])
					popped = append(popped, x)
					out = append(out, x.Item)
				}
			}, restore)
			// Mirror the exact popped items to the backup; on failure restore the
			// heap and report nothing removed.
			if p.backup != nil {
				// The items are already out of the heap here, so both the unwind guard and the
				// error return have to put them back before unlocking.
				//
				// The restore runs no Item method, and that is load-bearing rather than incidental.
				// Re-heapifying here reached Item.Less, so a Less that unwound during the rollback
				// took the frame with it and the core.ErrBackupFailed this rollback exists to report was
				// never returned to anyone: the caller was told nothing about the mirror refusing.
				// Nothing deferred can hand back an error from a frame that is being destroyed, so
				// the only fix is for the rollback to be incapable of failing.
				del := func() error { return p.backup.Del(ctx, out) }
				if err := core.RunBackup(del, restore); err != nil {
					restore()
					return nil, err
				}
			}
			p.h.journalOff()
			// Only now is the removal final. Dropping the exemption before the mirror would
			// leave it understated after the rollback above, and an understated exemption is
			// permanent: the queue would refuse work it has room for.
			for _, x := range popped {
				p.dropHydrated(x.Seq)
			}
			// Freed capacity: gated on HasWaiters; see Pop.
			if p.notFull.HasWaiters() {
				p.notFull.Signal()
			}
			p.lk.Unlock()
			return out, nil
		}
		if err := p.notEmpty.Wait(ctx, p.lk.Unlock); err != nil {
			return nil, p.ClosedOrCause(ctx)
		}
	}
}

// Peek implements Backing.Peek().
func (p *Backing[T]) Peek(ctx context.Context, options ...core.OpOption) (T, bool, error) {
	var zero T
	opts, err := core.ResolveOpOptions(core.CallPeek, options)
	if err != nil {
		return zero, false, err
	}
	p.lk.RLock()
	defer p.lk.RUnlock()
	if p.closed {
		return zero, false, core.ErrClosed
	}
	if p.h.Len() == 0 {
		return zero, false, core.RunSideEffect(opts.SideEffect)
	}
	return p.h.items[0].Item, true, core.RunSideEffect(opts.SideEffect)
}

// Exists implements Backing.Exists().
func (p *Backing[T]) Exists(ctx context.Context, v T, options ...core.OpOption) (bool, error) {
	opts, err := core.ResolveOpOptions(core.CallExists, options)
	if err != nil {
		return false, err
	}
	p.lk.RLock()
	defer p.lk.RUnlock()
	if p.closed {
		return false, core.ErrClosed
	}
	for _, it := range p.h.items {
		if it.Item.Equal(v) {
			return true, core.RunSideEffect(opts.SideEffect)
		}
	}
	return false, core.RunSideEffect(opts.SideEffect)
}

// dropHydrated reduces the hydration exemption by the number of restored entries among seqs. Only
// restored entries may reduce it: an admitted entry leaving has to give its capacity back, and
// charging that removal to the surplus instead would understate the bound for as long as the
// restore lasts — the queue would keep refusing work it has room for.
func (p *Backing[T]) dropHydrated(seqs ...uint64) {
	for _, seq := range seqs {
		if seq < p.hydrateEnd && p.hydrated > 0 {
			p.hydrated--
		}
	}
}

// Del implements Backing.Del().
func (p *Backing[T]) Del(ctx context.Context, v []T, options ...core.OpOption) (int, error) {
	opts, err := core.ResolveOpOptions(core.CallDel, options)
	if err != nil {
		return 0, err
	}
	p.lk.Lock()
	defer p.lk.Unlock()
	if p.closed {
		return 0, core.ErrClosed
	}
	kept := make([]core.SeqItem[T], 0, len(p.h.items))
	var removed []T
	var removedSeqs []uint64
	for _, it := range p.h.items {
		if core.MatchesAny(it.Item, v) {
			removed = append(removed, it.Item)
			removedSeqs = append(removedSeqs, it.Seq)
			continue
		}
		kept = append(kept, it)
	}
	if len(removed) == 0 {
		return 0, core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup mirror both run before the deletion is applied, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return 0, err
	}
	if p.backup != nil {
		if err := p.backup.Del(ctx, removed); err != nil {
			return 0, core.WrapBackup(err)
		}
	}
	p.h.items = kept
	heap.Init(&p.h)
	p.dropHydrated(removedSeqs...)
	// Freed capacity: gated on HasWaiters; see Pop.
	if p.notFull.HasWaiters() {
		p.notFull.Signal()
	}
	return len(removed), nil
}

// NotEmpty implements Backing.NotEmpty().
func (p *Backing[T]) NotEmpty(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotEmpty, options)
	if err != nil {
		return err
	}
	for {
		p.lk.RLock()
		if p.closed {
			p.lk.RUnlock()
			return core.ErrClosed
		}
		if p.h.Len() > 0 {
			err := core.RunSideEffectLocked(opts.SideEffect, p.lk.RUnlock)
			p.lk.RUnlock()
			return err
		}
		if err := p.notEmpty.Wait(ctx, p.lk.RUnlock); err != nil {
			return p.ClosedOrCause(ctx)
		}
	}
}

// NotFull implements Backing.NotFull().
func (p *Backing[T]) NotFull(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotFull, options)
	if err != nil {
		return err
	}
	for {
		p.lk.RLock()
		if p.closed {
			p.lk.RUnlock()
			return core.ErrClosed
		}
		if p.maxSize == 0 || p.h.Len()-p.hydrated+p.inflight < p.maxSize {
			err := core.RunSideEffectLocked(opts.SideEffect, p.lk.RUnlock)
			p.lk.RUnlock()
			return err
		}
		if err := p.notFull.Wait(ctx, p.lk.RUnlock); err != nil {
			return p.ClosedOrCause(ctx)
		}
	}
}

// Len implements Backing.Len().
func (p *Backing[T]) Len() int64 {
	p.lk.RLock()
	defer p.lk.RUnlock()
	return int64(p.h.Len())
}

// ClosedOrCause returns core.ErrClosed if the backing has been closed, else the ctx cause.
// Used in the ctx.Done() arm of a blocked wait so Close deterministically wins a race
// with ctx cancellation.
func (p *Backing[T]) ClosedOrCause(ctx context.Context) error {
	p.lk.RLock()
	c := p.closed
	p.lk.RUnlock()
	if c {
		return core.ErrClosed
	}
	cause := context.Cause(ctx)
	return cause
}

// Close implements Backing.Close().
func (p *Backing[T]) Close(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClose, options)
	if err != nil {
		return err
	}
	p.lk.Lock()
	if p.closed {
		err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock)
		p.lk.Unlock()
		return err
	}
	// The side effect runs before the close takes effect; a failure aborts the close and
	// the backing stays open.
	if err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock); err != nil {
		p.lk.Unlock()
		return err
	}
	if p.backup != nil {
		// The close below happens whatever the Backup reported — its error is returned, not
		// obeyed — and everyone parked is then woken to find out. An unwind owes the same, or a
		// Backup that panics leaves the queue open with its waiters still parked, which is not
		// what any other way of failing here does.
		closeAndWake := func() {
			p.closed = true
			p.lk.Unlock()
			p.notEmpty.Signal()
			p.notFull.Signal()
		}
		err = core.RunBackup(func() error { return p.backup.Close(ctx) }, closeAndWake)
	}
	p.closed = true
	p.lk.Unlock()
	p.notEmpty.Signal()
	p.notFull.Signal()
	return err
}

// Clear implements Backing.Clear().
func (p *Backing[T]) Clear(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClear, options)
	if err != nil {
		return err
	}
	p.lk.Lock()
	defer p.lk.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	if p.h.Len() == 0 {
		return core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup clear both run before the items are dropped, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return err
	}
	if p.backup != nil {
		if err := p.backup.Clear(ctx); err != nil {
			return core.WrapBackup(err)
		}
	}
	var zero core.SeqItem[T]
	for i := range p.h.items {
		p.h.items[i] = zero
	}
	p.h.items = p.h.items[:0]
	p.hydrated = 0
	// Freed capacity: gated on HasWaiters; see Pop.
	if p.notFull.HasWaiters() {
		p.notFull.Signal()
	}
	return nil
}

func (p *Backing[T]) sortedSnapshot() ([]core.SeqItem[T], bool) {
	if p.closed {
		return nil, false
	}
	items := make([]core.SeqItem[T], len(p.h.items))
	copy(items, p.h.items)
	sort.SliceStable(items, func(i, j int) bool {
		return core.PrioritySeqLess(items[i], items[j])
	})
	return items, true
}

// All implements Backing.All(). Unlike the other backings it holds the write lock for the
// whole iteration, not the read lock: yielding in priority order requires sorting, and
// sorting the heap array ascending in place leaves a valid min-heap (a node's children are
// always at higher indices), so Push/Pop keep working with no snapshot copy. AllCOW is the
// read-lock, snapshot variant.
func (p *Backing[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		p.lk.Lock()
		defer p.lk.Unlock()
		var zero T
		if p.closed {
			yield(zero, core.ErrClosed)
			return
		}
		sort.SliceStable(p.h.items, func(i, j int) bool {
			return core.PrioritySeqLess(p.h.items[i], p.h.items[j])
		})
		for i := range p.h.items {
			select {
			case <-ctx.Done():
				yield(zero, context.Cause(ctx))
				return
			default:
			}
			if !yield(p.h.items[i].Item, nil) {
				return
			}
		}
	}
}

// AllCOW implements Backing.AllCOW().
func (p *Backing[T]) AllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		p.lk.RLock()
		// sortedSnapshot sorts through the caller's Item.Less, and the read lock is released
		// explicitly below (it must go before any yield). An unwind out of Less would carry it away
		// and block every later writer for the life of the process. The flag keeps the explicit
		// release while the defer covers the unwind.
		held := true
		release := func() {
			if held {
				held = false
				p.lk.RUnlock()
			}
		}
		defer release()
		items, ok := p.sortedSnapshot()
		release()
		if !ok {
			yield(zero, core.ErrClosed)
			return
		}
		for _, it := range items {
			select {
			case <-ctx.Done():
				yield(zero, context.Cause(ctx))
				return
			default:
			}
			if !yield(it.Item, nil) {
				return
			}
		}
	}
}

// NotFullSignal implements core.Signals.
func (p *Backing[T]) NotFullSignal() *core.Signal { return p.notFull }

// NotEmptySignal implements core.Signals.
func (p *Backing[T]) NotEmptySignal() *core.Signal { return p.notEmpty }
