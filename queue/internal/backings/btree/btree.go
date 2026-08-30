package btree

import (
	"fmt"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
	"iter"

	"github.com/gostdlib/base/context"
	"github.com/tidwall/btree"
)

// btreeIndex maps Item.Hash() to the exact core.SeqItem locators stored in the tree, so
// Exists/Del do a bucket lookup + Equal scan instead of a full tree scan. nil when WithIndex
// is not set. The locator (core.SeqItem) is the precise tree key, so tree.Delete is O(log n).
type btreeIndex[T core.Item[T]] struct {
	m map[uint64][]core.SeqItem[T]
}

func newBtreeIndex[T core.Item[T]]() *btreeIndex[T] {
	return &btreeIndex[T]{m: map[uint64][]core.SeqItem[T]{}}
}

// add records si under hash. The hash is passed in rather than taken from si.Item here, so a caller
// mutating the tree can take every Item.Hash for its batch before the first insert: Hash is caller
// code and an unwind out of it between a tree insert and this call leaves the entry in the tree and
// absent from the index. remove takes its hash the same way and for the same reason.
func (x *btreeIndex[T]) add(hash uint64, si core.SeqItem[T]) {
	x.m[hash] = append(x.m[hash], si)
}

func (x *btreeIndex[T]) remove(hash uint64, seq uint64) {
	s := x.m[hash]
	for i := range s {
		if s[i].Seq == seq {
			s[i] = s[len(s)-1]
			x.m[hash] = s[:len(s)-1]
			break
		}
	}
	if len(x.m[hash]) == 0 {
		delete(x.m, hash)
	}
}

func (x *btreeIndex[T]) bucket(hash uint64) []core.SeqItem[T] {
	return x.m[hash]
}

// btreeBacking is an in-memory queue backed by github.com/tidwall/btree, used for both
// FIFO (keyed by insert sequence) and priority (keyed by Item.Less with insert sequence as
// tiebreak). The variant is selected by the less function passed to the constructor; the
// rest of the implementation is identical. The btree avoids the slice/heap backings'
// shift/reheapify cost, which matters for large or unbounded queues.
//
// Semantics (blocking, hydration, backup mirror) mirror fifo. lk and maxSize are injected
// by New via SetQueueLock and SetMaxSize before any other use.
type Backing[T core.Item[T]] struct {
	core.Seal
	lk      *core.QLock
	tree    *btree.BTreeG[core.SeqItem[T]]
	idx     *btreeIndex[T]
	nextSeq uint64
	maxSize int
	// hydrateEnd is the exclusive seq boundary of the restore: an entry is a restored one exactly
	// when its seq is below this. seq is assigned monotonically and Hydrate runs before the queue
	// is visible to anyone, so the boundary classifies every entry for the life of the queue and
	// hydrated can be adjusted by what actually left rather than by how much did.
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
	priority bool
	notFull  *core.Signal
	notEmpty *core.Signal
	closed   bool
	backup   core.Backup[T]
}

// NewFIFO returns a keyed B-Tree FIFO Backing: a tidwall/btree keyed by insert sequence plus a
// hash index, giving O(log n) Del and O(1) Exists — required for delete-heavy workloads that scan
// RangeAll and Del/Exists each matching entry. The unindexed FIFO is the positional btype tree,
// which queue.NewBTreeFIFO dispatches to instead; options are already resolved by the caller.
func NewFIFO[T core.Item[T]](o core.BackingOpts) (core.Backing[T], error) {
	return newBacking[T](o, core.FifoSeqLess[T], false)
}

// NewPriority returns a keyed B-Tree priority Backing, ordered by Item.Less with the insert
// sequence as the tiebreak. Options are already resolved by the caller.
func NewPriority[T core.Item[T]](o core.BackingOpts) (core.Backing[T], error) {
	return newBacking[T](o, core.PrioritySeqLess[T], true)
}

func newBacking[T core.Item[T]](o core.BackingOpts, less func(a, b core.SeqItem[T]) bool, priority bool) (core.Backing[T], error) {
	// Unconditional: applyBackingOptions seeds the default width, so a zero here means the caller
	// asked for zero rather than that nothing was set. Patching the default in at this point is
	// what let WithBTreeWidth(0) through while rejecting 1 and -1.
	//
	// WithBTreeWidth now rejects the same range in its own closure, which is what covers the
	// NewBTreeFIFO variant that returns before ever reaching here. This stays as the invariant for
	// the struct itself: nothing but a validated option should be able to reach the tree, and a
	// future constructor that builds core.BackingOpts by hand would otherwise reach it unchecked.
	if o.Width < 2 {
		return nil, fmt.Errorf("%w: WithBTreeWidth must be at least 2, got %d", core.ErrBadOption, o.Width)
	}
	b := &Backing[T]{
		lk: &core.QLock{},
		tree: btree.NewBTreeGOptions(less, btree.Options{
			Degree:  o.Width,
			NoLocks: true,
		}),
		priority: priority,
		notFull:  core.NewSignal(),
		notEmpty: core.NewSignal(),
	}
	if o.Index {
		b.idx = newBtreeIndex[T]()
	}
	return b, nil
}

func (b *Backing[T]) SetQueueLock(lk *core.QLock) { b.lk = lk }

func (b *Backing[T]) SetMaxBatch(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: max batch must be at least 1, got %d", core.ErrBadOption, n)
	}
	return nil
}

func (b *Backing[T]) SetMaxSize(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: max size must be at least 0, got %d", core.ErrBadOption, n)
	}
	b.maxSize = n
	return nil
}

// Hydrate implements Backing.Hydrate().
func (b *Backing[T]) Hydrate(ctx context.Context, bu core.Backup[T]) error {
	b.lk.Lock()
	defer b.lk.Unlock()
	// Set on every exit, not just the happy one: a partial load that errors out would otherwise
	// leave hydrated > 0 with the boundary at 0, and an exemption that can never decay.
	defer func() { b.hydrateEnd = b.nextSeq }()
	for v, err := range bu.RangeAll(ctx) {
		if err != nil {
			return core.WrapBackup(err)
		}
		if err := core.ValidateKindOne(b.priority, v); err != nil {
			return err
		}
		if err := bu.OnLoad(ctx, v); err != nil {
			return core.WrapBackup(err)
		}
		si := core.SeqItem[T]{Seq: b.nextSeq, Item: v}
		// The hash comes before the insert, the same way Push takes its batch's hashes up front:
		// Item.Hash is caller code, and an unwind out of it after tree.Load has landed leaves the
		// entry in the tree and absent from the index.
		var hash uint64
		if b.idx != nil {
			hash = v.Hash()
		}
		// Load is an O(1) append for ascending input; seq is strictly increasing
		// (FIFO) and unique (priority tiebreak), so it is correct and much cheaper
		// than Set's full descent.
		b.tree.Load(si)
		b.nextSeq++
		if b.idx != nil {
			b.idx.add(hash, si)
		}
		b.hydrated++
	}
	b.backup = bu
	return nil
}

// Push implements Backing.Push().
func (b *Backing[T]) Push(ctx context.Context, vs []T, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallPush, options)
	if err != nil {
		return err
	}
	if err := core.ValidateKind(b.priority, vs); err != nil {
		return err
	}
	for {
		b.lk.Lock()
		if b.closed {
			b.lk.Unlock()
			return core.ErrClosed
		}
		if b.maxSize > 0 && len(vs) > b.maxSize {
			b.lk.Unlock()
			return core.ErrBatchTooLarge
		}
		if b.maxSize == 0 || b.tree.Len()-b.hydrated+b.inflight+len(vs) <= b.maxSize {
			if opts.OnAdmit != nil {
				// Reserve the capacity, drop the lock for the caller's hook, then retake it
				// to insert. The reservation is what lets the lock go: concurrent Pushes and
				// NotFull count inflight, so the bound holds even with nothing yet inserted.
				// runOnAdmit releases the reservation on every exit, a panicking hook
				// included, and returns holding the lock.
				if err := core.RunOnAdmit(b.lk, &b.inflight, len(vs), b.notFull, opts.OnAdmit); err != nil {
					b.lk.Unlock()
					return err
				}
				if b.closed {
					// Closed while the hook ran. The hook cannot be un-run, the same as a
					// backup mirror failing after a side effect. Close already broadcasts to
					// every parked waiter, so this signal is redundant — it is here so that
					// every exit which gives a reservation back signals, checkable by reading
					// Push alone rather than by reasoning about Close.
					opts.ReleaseReservation(b.notFull)
					b.lk.Unlock()
					return core.ErrClosed
				}
			}
			// The unwind callback owes the same debt the error return below does: if the side
			// effect panics or calls runtime.Goexit, the reservation is handed back with nothing
			// inserted, and a producer parked on exactly that capacity has to be woken. Releasing
			// only the lock leaves it parked on a queue with room in it.
			if err := core.RunSideEffectLocked(opts.SideEffect, func() { opts.ReleaseReservation(b.notFull); b.lk.Unlock() }); err != nil {
				opts.ReleaseReservation(b.notFull)
				b.lk.Unlock()
				return err
			}
			if b.backup != nil {
				// A Backup is caller-supplied code holding the queue's lock, so it gets the
				// same unwind guard the side effect does, owing the same reservation debt.
				push := func() error { return b.backup.Push(ctx, vs) }
				if err := core.RunBackup(push, func() { opts.ReleaseReservation(b.notFull); b.lk.Unlock() }); err != nil {
					opts.ReleaseReservation(b.notFull)
					b.lk.Unlock()
					return err
				}
			}
			wasEmpty := b.tree.Len() == 0
			// tree.Load reaches the caller's Item.Less through the tree comparator (on the priority
			// variant) and idx.add needs the caller's Item.Hash. This path unlocks explicitly, so an
			// unwind out of either would carry the lock away and wedge every later operation on the
			// queue. The release owes the same reservation debt the abandon paths above owe.
			core.RunLockedNoErr(func() {
				// Every hash for the batch is taken before the first insert. Taking each one beside
				// its own Load instead left a Hash that unwound with the entry already in the tree
				// and never in the index: Exists reported false for it, Del removed nothing, and
				// nothing but Pop or Clear could ever get it out again. The index cannot be repaired
				// after the fact either — putting the entry back means re-running Hash, which is the
				// code that just failed.
				var hashes []uint64
				if b.idx != nil {
					hashes = make([]uint64, len(vs))
					for i, v := range vs {
						hashes[i] = v.Hash()
					}
				}
				for i, v := range vs {
					si := core.SeqItem[T]{Seq: b.nextSeq, Item: v}
					// Load: O(1) append for the strictly-increasing seq key.
					b.tree.Load(si)
					b.nextSeq++
					if b.idx != nil {
						b.idx.add(hashes[i], si)
					}
				}
			}, func() { opts.ReleaseReservation(b.notFull); b.lk.Unlock() })
			if wasEmpty && b.notEmpty.HasWaiters() {
				b.notEmpty.Signal()
			}
			b.lk.Unlock()
			return nil
		}
		if err := b.notFull.Wait(ctx, b.lk.Unlock); err != nil {
			return b.ClosedOrCause(ctx)
		}
	}
}

// Pop implements Backing.Pop().
func (b *Backing[T]) Pop(ctx context.Context, n int, options ...core.OpOption) ([]T, error) {
	opts, err := core.ResolveOpOptions(core.CallPop, options)
	if err != nil {
		return nil, err
	}
	for {
		b.lk.Lock()
		if b.closed {
			b.lk.Unlock()
			return nil, core.ErrClosed
		}
		if b.tree.Len() > 0 {
			k := n
			if k > b.tree.Len() {
				k = b.tree.Len()
			}
			// The side effect runs before anything is removed, so a failure aborts
			// with nothing removed.
			if err := core.RunSideEffectLocked(opts.SideEffect, b.lk.Unlock); err != nil {
				b.lk.Unlock()
				return nil, err
			}
			out := make([]T, 0, k)
			// One ordered peek serves the backup mirror and the index alike. Scan walks the tree in
			// pop order without mutating it and without consulting the comparator, so both the items
			// to mirror and every index key are settled before the first PopMin.
			//
			// The hashes are what forced this. idx.remove needs Item.Hash, which is caller code, and
			// taking it beside its own PopMin left an unwinding Hash with the entry already out of
			// the tree and still in the index: Exists reported a permanent false positive for it and
			// Del counted a removal that did not happen. Nor can the pop be undone — putting an entry
			// back runs the tree comparator, which on the priority variant is caller code too.
			peeked := b.idx != nil || b.backup != nil
			var hashes []uint64
			if peeked {
				if b.idx != nil {
					hashes = make([]uint64, 0, k)
				}
				core.RunLockedNoErr(func() {
					b.tree.Scan(func(it core.SeqItem[T]) bool {
						out = append(out, it.Item)
						if b.idx != nil {
							hashes = append(hashes, it.Item.Hash())
						}
						return len(out) < k
					})
				}, b.lk.Unlock)
			}
			if b.backup != nil {
				del := func() error { return b.backup.Del(ctx, out) }
				if err := core.RunBackup(del, b.lk.Unlock); err != nil {
					b.lk.Unlock()
					return nil, err
				}
			}
			// Nothing below reaches caller code: PopMin removes the leftmost directly — no
			// Delete-by-key search and no comparisons — and idx.remove works from the keys taken
			// above. So no guard, and no half-mutated state for one to have to describe.
			for i := 0; i < k; i++ {
				mn, _ := b.tree.PopMin()
				if !peeked {
					out = append(out, mn.Item)
				}
				if b.idx != nil {
					b.idx.remove(hashes[i], mn.Seq)
				}
				b.dropHydrated(mn.Seq)
			}
			// Freed capacity: gated on HasWaiters; see Pop.
			if b.notFull.HasWaiters() {
				b.notFull.Signal()
			}
			b.lk.Unlock()
			return out, nil
		}
		if err := b.notEmpty.Wait(ctx, b.lk.Unlock); err != nil {
			return nil, b.ClosedOrCause(ctx)
		}
	}
}

// Peek implements Backing.Peek().
func (b *Backing[T]) Peek(ctx context.Context, options ...core.OpOption) (T, bool, error) {
	var zero T
	opts, err := core.ResolveOpOptions(core.CallPeek, options)
	if err != nil {
		return zero, false, err
	}
	b.lk.RLock()
	defer b.lk.RUnlock()
	if b.closed {
		return zero, false, core.ErrClosed
	}
	min, ok := b.tree.Min()
	if !ok {
		return zero, false, core.RunSideEffect(opts.SideEffect)
	}
	return min.Item, true, core.RunSideEffect(opts.SideEffect)
}

// Exists implements Backing.Exists().
func (b *Backing[T]) Exists(ctx context.Context, v T, options ...core.OpOption) (bool, error) {
	opts, err := core.ResolveOpOptions(core.CallExists, options)
	if err != nil {
		return false, err
	}
	b.lk.RLock()
	defer b.lk.RUnlock()
	if b.closed {
		return false, core.ErrClosed
	}
	if b.idx != nil {
		for _, si := range b.idx.bucket(v.Hash()) {
			if si.Item.Equal(v) {
				return true, core.RunSideEffect(opts.SideEffect)
			}
		}
		return false, core.RunSideEffect(opts.SideEffect)
	}
	found := false
	b.tree.Scan(func(it core.SeqItem[T]) bool {
		if it.Item.Equal(v) {
			found = true
			return false
		}
		return true
	})
	return found, core.RunSideEffect(opts.SideEffect)
}

// dropHydrated reduces the hydration exemption by the number of restored entries among seqs. Only
// restored entries may reduce it: an admitted entry leaving has to give its capacity back, and
// charging that removal to the surplus instead would understate the bound for as long as the
// restore lasts — the queue would keep refusing work it has room for.
func (b *Backing[T]) dropHydrated(seqs ...uint64) {
	for _, seq := range seqs {
		if seq < b.hydrateEnd && b.hydrated > 0 {
			b.hydrated--
		}
	}
}

// Del implements Backing.Del().
func (b *Backing[T]) Del(ctx context.Context, v []T, options ...core.OpOption) (int, error) {
	opts, err := core.ResolveOpOptions(core.CallDel, options)
	if err != nil {
		return 0, err
	}
	b.lk.Lock()
	defer b.lk.Unlock()
	if b.closed {
		return 0, core.ErrClosed
	}
	var toDel []core.SeqItem[T]
	if b.idx != nil {
		// Scan only the buckets for the distinct hashes in v; dedup collected items
		// by seq so a slot matched by duplicate/same-hash elements of v is removed once.
		seenHash := make(map[uint64]struct{}, len(v))
		seenSeq := make(map[uint64]struct{})
		for e := range v {
			h := v[e].Hash()
			if _, ok := seenHash[h]; ok {
				continue
			}
			seenHash[h] = struct{}{}
			for _, si := range b.idx.bucket(h) {
				if _, ok := seenSeq[si.Seq]; ok {
					continue
				}
				if core.MatchesAny(si.Item, v) {
					seenSeq[si.Seq] = struct{}{}
					toDel = append(toDel, si)
				}
			}
		}
	} else {
		b.tree.Scan(func(it core.SeqItem[T]) bool {
			if core.MatchesAny(it.Item, v) {
				toDel = append(toDel, it)
			}
			return true
		})
	}
	if len(toDel) == 0 {
		return 0, core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup mirror both run before the deletion is applied, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return 0, err
	}
	if b.backup != nil {
		removed := make([]T, 0, len(toDel))
		for _, it := range toDel {
			removed = append(removed, it.Item)
		}
		if err := b.backup.Del(ctx, removed); err != nil {
			return 0, core.WrapBackup(err)
		}
	}
	for _, it := range toDel {
		b.tree.Delete(it)
		if b.idx != nil {
			b.idx.remove(it.Item.Hash(), it.Seq)
		}
	}
	for _, it := range toDel {
		b.dropHydrated(it.Seq)
	}
	// Freed capacity: gated on HasWaiters; see Pop.
	if b.notFull.HasWaiters() {
		b.notFull.Signal()
	}
	return len(toDel), nil
}

// NotEmpty implements Backing.NotEmpty().
func (b *Backing[T]) NotEmpty(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotEmpty, options)
	if err != nil {
		return err
	}
	for {
		b.lk.RLock()
		if b.closed {
			b.lk.RUnlock()
			return core.ErrClosed
		}
		if b.tree.Len() > 0 {
			err := core.RunSideEffectLocked(opts.SideEffect, b.lk.RUnlock)
			b.lk.RUnlock()
			return err
		}
		if err := b.notEmpty.Wait(ctx, b.lk.RUnlock); err != nil {
			return b.ClosedOrCause(ctx)
		}
	}
}

// NotFull implements Backing.NotFull().
func (b *Backing[T]) NotFull(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotFull, options)
	if err != nil {
		return err
	}
	for {
		b.lk.RLock()
		if b.closed {
			b.lk.RUnlock()
			return core.ErrClosed
		}
		if b.maxSize == 0 || b.tree.Len()-b.hydrated+b.inflight < b.maxSize {
			err := core.RunSideEffectLocked(opts.SideEffect, b.lk.RUnlock)
			b.lk.RUnlock()
			return err
		}
		if err := b.notFull.Wait(ctx, b.lk.RUnlock); err != nil {
			return b.ClosedOrCause(ctx)
		}
	}
}

// Len implements Backing.Len().
func (b *Backing[T]) Len() int64 {
	b.lk.RLock()
	defer b.lk.RUnlock()
	return int64(b.tree.Len())
}

// ClosedOrCause returns core.ErrClosed if the backing has been closed, else the ctx cause.
// Used in the ctx.Done() arm of a blocked wait so Close deterministically wins a race
// with ctx cancellation.
func (b *Backing[T]) ClosedOrCause(ctx context.Context) error {
	b.lk.RLock()
	c := b.closed
	b.lk.RUnlock()
	if c {
		return core.ErrClosed
	}
	cause := context.Cause(ctx)
	return cause
}

// Close implements Backing.Close().
func (b *Backing[T]) Close(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClose, options)
	if err != nil {
		return err
	}
	b.lk.Lock()
	if b.closed {
		err := core.RunSideEffectLocked(opts.SideEffect, b.lk.Unlock)
		b.lk.Unlock()
		return err
	}
	// The side effect runs before the close takes effect; a failure aborts the close and
	// the backing stays open.
	if err := core.RunSideEffectLocked(opts.SideEffect, b.lk.Unlock); err != nil {
		b.lk.Unlock()
		return err
	}
	if b.backup != nil {
		// The close below happens whatever the Backup reported — its error is returned, not
		// obeyed — and everyone parked is then woken to find out. An unwind owes the same, or a
		// Backup that panics leaves the queue open with its waiters still parked, which is not
		// what any other way of failing here does.
		closeAndWake := func() {
			b.closed = true
			b.lk.Unlock()
			b.notEmpty.Signal()
			b.notFull.Signal()
		}
		err = core.RunBackup(func() error { return b.backup.Close(ctx) }, closeAndWake)
	}
	b.closed = true
	b.lk.Unlock()
	b.notEmpty.Signal()
	b.notFull.Signal()
	return err
}

// Clear implements Backing.Clear().
func (b *Backing[T]) Clear(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClear, options)
	if err != nil {
		return err
	}
	b.lk.Lock()
	defer b.lk.Unlock()
	if b.closed {
		return core.ErrClosed
	}
	if b.tree.Len() == 0 {
		return core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup clear both run before the items are dropped, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return err
	}
	if b.backup != nil {
		if err := b.backup.Clear(ctx); err != nil {
			return core.WrapBackup(err)
		}
	}
	b.tree.Clear()
	if b.idx != nil {
		b.idx = newBtreeIndex[T]()
	}
	b.hydrated = 0
	// Freed capacity: gated on HasWaiters; see Pop.
	if b.notFull.HasWaiters() {
		b.notFull.Signal()
	}
	return nil
}

// All implements Backing.All().
func (b *Backing[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		b.lk.RLock()
		defer b.lk.RUnlock()
		if b.closed {
			var zero T
			yield(zero, core.ErrClosed)
			return
		}
		var zero T
		b.tree.Scan(func(it core.SeqItem[T]) bool {
			select {
			case <-ctx.Done():
				yield(zero, context.Cause(ctx))
				return false
			default:
			}
			return yield(it.Item, nil)
		})
	}
}

// AllCOW implements Backing.AllCOW().
func (b *Backing[T]) AllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		b.lk.CowEnter()
		defer b.lk.CowExit()
		b.lk.RLock()
		// The read lock is released on each exit below, which a panic out of the caller's iterator
		// body — or out of the caller's codec — skips entirely, leaking it for the life of the
		// process and blocking every later writer. The flag keeps the explicit releases (they
		// must happen before yielding unlocked) while the defer covers the unwind.
		held := true
		release := func() {
			if held {
				held = false
				b.lk.RUnlock()
			}
		}
		defer release()
		if b.closed {
			release()
			yield(zero, core.ErrClosed)
			return
		}
		// Yield in order while no writer waits. Once a writer is waiting (or after the
		// first such item), collect the remainder under the read lock without yielding,
		// then release and finish from the copy so the writer can proceed.
		var snap []T
		contended := false
		stop := false
		// cerr carries a cancellation out of the Scan callback instead of yielding from inside it:
		// the callback runs under the read lock, and an error yield must reach the caller's loop
		// body with no lock held, like every other error exit here.
		var cerr error
		b.tree.Scan(func(it core.SeqItem[T]) bool {
			if contended || b.lk.WriteWanted() {
				contended = true
				snap = append(snap, it.Item)
				return true
			}
			select {
			case <-ctx.Done():
				cerr = context.Cause(ctx)
				stop = true
				return false
			default:
			}
			if !yield(it.Item, nil) {
				stop = true
				return false
			}
			return true
		})
		release()
		if cerr != nil {
			yield(zero, cerr)
			return
		}
		if stop {
			return
		}
		for _, v := range snap {
			select {
			case <-ctx.Done():
				yield(zero, context.Cause(ctx))
				return
			default:
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}

// NotFullSignal implements core.Signals.
func (b *Backing[T]) NotFullSignal() *core.Signal { return b.notFull }

// NotEmptySignal implements core.Signals.
func (b *Backing[T]) NotEmptySignal() *core.Signal { return b.notEmpty }
