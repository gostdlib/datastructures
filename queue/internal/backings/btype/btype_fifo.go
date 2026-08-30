package btype

import (
	"fmt"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
	"iter"

	"github.com/gostdlib/base/context"
)

// btypeFIFO is an in-memory FIFO queue backed by the positional copy-on-write B-tree in
// btype_btree.go (PushBack/PopFront), which avoids the comparator descent and core.SeqItem
// wrapper of the tidwall/btree-based FIFO. Semantics (blocking, hydration, backup mirror)
// mirror fifo. lk and maxSize are injected by New via SetQueueLock and SetMaxSize before
// any other use.
type Backing[T core.Item[T]] struct {
	core.Seal
	lk      *core.QLock
	t       tree[omit, T]
	maxSize int
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

// newBtypeFIFO returns an in-memory FIFO Backing backed by the positional B-tree. It is the
// backing NewBTreeFIFO uses when WithIndex is not set: unbounded-friendly with cheap
// push/pop and O(n) Exists/Del.
func New[T core.Item[T]]() (core.Backing[T], error) {
	return &Backing[T]{
		lk:       &core.QLock{},
		notFull:  core.NewSignal(),
		notEmpty: core.NewSignal(),
	}, nil
}

func (f *Backing[T]) SetQueueLock(lk *core.QLock) { f.lk = lk }

func (f *Backing[T]) SetMaxBatch(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: max batch must be at least 1, got %d", core.ErrBadOption, n)
	}
	return nil
}

func (f *Backing[T]) SetMaxSize(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: max size must be at least 0, got %d", core.ErrBadOption, n)
	}
	f.maxSize = n
	return nil
}

// Hydrate implements Backing.Hydrate().
func (f *Backing[T]) Hydrate(ctx context.Context, b core.Backup[T]) error {
	f.lk.Lock()
	defer f.lk.Unlock()
	for v, err := range b.RangeAll(ctx) {
		if err != nil {
			return core.WrapBackup(err)
		}
		if err := core.ValidateKindOne(false, v); err != nil {
			return err
		}
		if err := b.OnLoad(ctx, v); err != nil {
			return core.WrapBackup(err)
		}
		f.t.PushBack(omit{}, v)
		f.hydrated++
	}
	f.backup = b
	return nil
}

// Push implements Backing.Push().
func (f *Backing[T]) Push(ctx context.Context, vs []T, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallPush, options)
	if err != nil {
		return err
	}
	if err := core.ValidateKind(false, vs); err != nil {
		return err
	}
	for {
		f.lk.Lock()
		if f.closed {
			f.lk.Unlock()
			return core.ErrClosed
		}
		if f.maxSize > 0 && len(vs) > f.maxSize {
			f.lk.Unlock()
			return core.ErrBatchTooLarge
		}
		if f.maxSize == 0 || f.t.Len()-f.hydrated+f.inflight+len(vs) <= f.maxSize {
			if opts.OnAdmit != nil {
				// Reserve the capacity, drop the lock for the caller's hook, then retake it
				// to insert. The reservation is what lets the lock go: concurrent Pushes and
				// NotFull count inflight, so the bound holds even with nothing yet inserted.
				// runOnAdmit releases the reservation on every exit, a panicking hook
				// included, and returns holding the lock.
				if err := core.RunOnAdmit(f.lk, &f.inflight, len(vs), f.notFull, opts.OnAdmit); err != nil {
					f.lk.Unlock()
					return err
				}
				if f.closed {
					// Closed while the hook ran. The hook cannot be un-run, the same as a
					// backup mirror failing after a side effect. Close already broadcasts to
					// every parked waiter, so this signal is redundant — it is here so that
					// every exit which gives a reservation back signals, checkable by reading
					// Push alone rather than by reasoning about Close.
					opts.ReleaseReservation(f.notFull)
					f.lk.Unlock()
					return core.ErrClosed
				}
			}
			// The unwind callback owes the same debt the error return below does: if the side
			// effect panics or calls runtime.Goexit, the reservation is handed back with nothing
			// inserted, and a producer parked on exactly that capacity has to be woken. Releasing
			// only the lock leaves it parked on a queue with room in it.
			if err := core.RunSideEffectLocked(opts.SideEffect, func() { opts.ReleaseReservation(f.notFull); f.lk.Unlock() }); err != nil {
				opts.ReleaseReservation(f.notFull)
				f.lk.Unlock()
				return err
			}
			if f.backup != nil {
				// A Backup is caller-supplied code holding the queue's lock, so it gets the
				// same unwind guard the side effect does, owing the same reservation debt.
				push := func() error { return f.backup.Push(ctx, vs) }
				if err := core.RunBackup(push, func() { opts.ReleaseReservation(f.notFull); f.lk.Unlock() }); err != nil {
					opts.ReleaseReservation(f.notFull)
					f.lk.Unlock()
					return err
				}
			}
			wasEmpty := f.t.Len() == 0
			for _, v := range vs {
				f.t.PushBack(omit{}, v)
			}
			if wasEmpty && f.notEmpty.HasWaiters() {
				f.notEmpty.Signal()
			}
			f.lk.Unlock()
			return nil
		}
		if err := f.notFull.Wait(ctx, f.lk.Unlock); err != nil {
			return f.ClosedOrCause(ctx)
		}
	}
}

// Pop implements Backing.Pop().
func (f *Backing[T]) Pop(ctx context.Context, n int, options ...core.OpOption) ([]T, error) {
	opts, err := core.ResolveOpOptions(core.CallPop, options)
	if err != nil {
		return nil, err
	}
	for {
		f.lk.Lock()
		if f.closed {
			f.lk.Unlock()
			return nil, core.ErrClosed
		}
		if f.t.Len() > 0 {
			k := n
			if k > f.t.Len() {
				k = f.t.Len()
			}
			// The side effect runs before anything is removed, so a failure aborts
			// with nothing removed.
			if err := core.RunSideEffectLocked(opts.SideEffect, f.lk.Unlock); err != nil {
				f.lk.Unlock()
				return nil, err
			}
			out := make([]T, 0, k)
			if f.backup != nil {
				// Peek the first k in order without mutating, so the backup is
				// mirrored before anything is removed.
				for _, v := range f.t.All() {
					out = append(out, v)
					if len(out) == k {
						break
					}
				}
				del := func() error { return f.backup.Del(ctx, out) }
				if err := core.RunBackup(del, f.lk.Unlock); err != nil {
					f.lk.Unlock()
					return nil, err
				}
				for i := 0; i < k; i++ {
					f.t.PopFront()
				}
			} else {
				for i := 0; i < k; i++ {
					_, v, _ := f.t.PopFront()
					out = append(out, v)
				}
			}
			// Pop takes from the front, which is exactly where the restored entries are.
			f.dropHydrated(min(k, f.hydrated))
			// Freed capacity: gated on HasWaiters; see Pop.
			if f.notFull.HasWaiters() {
				f.notFull.Signal()
			}
			f.lk.Unlock()
			return out, nil
		}
		if err := f.notEmpty.Wait(ctx, f.lk.Unlock); err != nil {
			return nil, f.ClosedOrCause(ctx)
		}
	}
}

// Peek implements Backing.Peek().
func (f *Backing[T]) Peek(ctx context.Context, options ...core.OpOption) (T, bool, error) {
	var zero T
	opts, err := core.ResolveOpOptions(core.CallPeek, options)
	if err != nil {
		return zero, false, err
	}
	f.lk.RLock()
	defer f.lk.RUnlock()
	if f.closed {
		return zero, false, core.ErrClosed
	}
	if f.t.Len() == 0 {
		return zero, false, core.RunSideEffect(opts.SideEffect)
	}
	_, v, _ := f.t.Front()
	return v, true, core.RunSideEffect(opts.SideEffect)
}

// Exists implements Backing.Exists().
func (f *Backing[T]) Exists(ctx context.Context, v T, options ...core.OpOption) (bool, error) {
	opts, err := core.ResolveOpOptions(core.CallExists, options)
	if err != nil {
		return false, err
	}
	f.lk.RLock()
	defer f.lk.RUnlock()
	if f.closed {
		return false, core.ErrClosed
	}
	for _, it := range f.t.All() {
		if it.Equal(v) {
			return true, core.RunSideEffect(opts.SideEffect)
		}
	}
	return false, core.RunSideEffect(opts.SideEffect)
}

// dropHydrated reduces the hydration exemption by n, the number of restored entries removed. Only
// restored entries may reduce it: an admitted entry leaving has to give its capacity back, and
// charging that removal to the surplus instead would understate the bound for as long as the
// restore lasts — the queue would keep refusing work it has room for.
//
// This backing is FIFO and only ever appends, so the restored entries are exactly the first
// f.hydrated positions. That is what lets callers classify a removal by index rather than by
// carrying a marker on every entry.
func (f *Backing[T]) dropHydrated(n int) {
	if n >= f.hydrated {
		f.hydrated = 0
		return
	}
	f.hydrated -= n
}

// Del implements Backing.Del().
func (f *Backing[T]) Del(ctx context.Context, v []T, options ...core.OpOption) (int, error) {
	opts, err := core.ResolveOpOptions(core.CallDel, options)
	if err != nil {
		return 0, err
	}
	f.lk.Lock()
	defer f.lk.Unlock()
	if f.closed {
		return 0, core.ErrClosed
	}
	var rmIdx []int
	var removed []T
	i := 0
	for _, it := range f.t.All() {
		if core.MatchesAny(it, v) {
			rmIdx = append(rmIdx, i)
			removed = append(removed, it)
		}
		i++
	}
	if len(removed) == 0 {
		return 0, core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup mirror both run before the deletion is applied, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return 0, err
	}
	if f.backup != nil {
		if err := f.backup.Del(ctx, removed); err != nil {
			return 0, core.WrapBackup(err)
		}
	}
	// Delete from highest index down so earlier indices stay valid.
	// Positions below f.hydrated hold restored entries; count how many of those actually go.
	removedHydrated := 0
	for _, idx := range rmIdx {
		if idx < f.hydrated {
			removedHydrated++
		}
	}
	for j := len(rmIdx) - 1; j >= 0; j-- {
		f.t.DeleteAt(rmIdx[j])
	}
	f.dropHydrated(removedHydrated)
	// Freed capacity: gated on HasWaiters; see Pop.
	if f.notFull.HasWaiters() {
		f.notFull.Signal()
	}
	return len(removed), nil
}

// NotEmpty implements Backing.NotEmpty().
func (f *Backing[T]) NotEmpty(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotEmpty, options)
	if err != nil {
		return err
	}
	for {
		f.lk.RLock()
		if f.closed {
			f.lk.RUnlock()
			return core.ErrClosed
		}
		if f.t.Len() > 0 {
			err := core.RunSideEffectLocked(opts.SideEffect, f.lk.RUnlock)
			f.lk.RUnlock()
			return err
		}
		if err := f.notEmpty.Wait(ctx, f.lk.RUnlock); err != nil {
			return f.ClosedOrCause(ctx)
		}
	}
}

// NotFull implements Backing.NotFull().
func (f *Backing[T]) NotFull(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallNotFull, options)
	if err != nil {
		return err
	}
	for {
		f.lk.RLock()
		if f.closed {
			f.lk.RUnlock()
			return core.ErrClosed
		}
		if f.maxSize == 0 || f.t.Len()-f.hydrated+f.inflight < f.maxSize {
			err := core.RunSideEffectLocked(opts.SideEffect, f.lk.RUnlock)
			f.lk.RUnlock()
			return err
		}
		if err := f.notFull.Wait(ctx, f.lk.RUnlock); err != nil {
			return f.ClosedOrCause(ctx)
		}
	}
}

// Len implements Backing.Len().
func (f *Backing[T]) Len() int64 {
	f.lk.RLock()
	defer f.lk.RUnlock()
	return int64(f.t.Len())
}

// ClosedOrCause returns core.ErrClosed if the backing has been closed, else the ctx cause.
// Used in the ctx.Done() arm of a blocked wait so Close deterministically wins a race
// with ctx cancellation.
func (f *Backing[T]) ClosedOrCause(ctx context.Context) error {
	f.lk.RLock()
	c := f.closed
	f.lk.RUnlock()
	if c {
		return core.ErrClosed
	}
	cause := context.Cause(ctx)
	return cause
}

// Close implements Backing.Close().
func (f *Backing[T]) Close(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClose, options)
	if err != nil {
		return err
	}
	f.lk.Lock()
	if f.closed {
		err := core.RunSideEffectLocked(opts.SideEffect, f.lk.Unlock)
		f.lk.Unlock()
		return err
	}
	// The side effect runs before the close takes effect; a failure aborts the close and
	// the backing stays open.
	if err := core.RunSideEffectLocked(opts.SideEffect, f.lk.Unlock); err != nil {
		f.lk.Unlock()
		return err
	}
	if f.backup != nil {
		// The close below happens whatever the Backup reported — its error is returned, not
		// obeyed — and everyone parked is then woken to find out. An unwind owes the same, or a
		// Backup that panics leaves the queue open with its waiters still parked, which is not
		// what any other way of failing here does.
		closeAndWake := func() {
			f.closed = true
			f.lk.Unlock()
			f.notEmpty.Signal()
			f.notFull.Signal()
		}
		err = core.RunBackup(func() error { return f.backup.Close(ctx) }, closeAndWake)
	}
	f.closed = true
	f.lk.Unlock()
	f.notEmpty.Signal()
	f.notFull.Signal()
	return err
}

// Clear implements Backing.Clear().
func (f *Backing[T]) Clear(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClear, options)
	if err != nil {
		return err
	}
	f.lk.Lock()
	defer f.lk.Unlock()
	if f.closed {
		return core.ErrClosed
	}
	if f.t.Len() == 0 {
		return core.RunSideEffect(opts.SideEffect)
	}
	// The side effect and backup clear both run before the items are dropped, so a
	// failure of either aborts with nothing removed.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return err
	}
	if f.backup != nil {
		if err := f.backup.Clear(ctx); err != nil {
			return core.WrapBackup(err)
		}
	}
	f.t.Clear()
	f.hydrated = 0
	// Freed capacity: gated on HasWaiters; see Pop.
	if f.notFull.HasWaiters() {
		f.notFull.Signal()
	}
	return nil
}

// All implements Backing.All().
func (f *Backing[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		f.lk.RLock()
		defer f.lk.RUnlock()
		if f.closed {
			var zero T
			yield(zero, core.ErrClosed)
			return
		}
		for _, v := range f.t.All() {
			select {
			case <-ctx.Done():
				var zero T
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

// AllCOW implements Backing.AllCOW().
func (f *Backing[T]) AllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		// Take the write lock only for the O(1) copy-on-write snapshot: CopyInto
		// flips the source's copied flag (a non-atomic field), so it must be
		// exclusive of readers and writers. The lock is released before any yield,
		// so writers never block during iteration and the loop may safely call
		// mutating Queue methods.
		f.lk.Lock()
		if f.closed {
			f.lk.Unlock()
			yield(zero, core.ErrClosed)
			return
		}
		var snap tree[omit, T]
		f.t.CopyInto(&snap)
		f.lk.Unlock()
		defer snap.Release()

		for _, v := range snap.All() {
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
func (f *Backing[T]) NotFullSignal() *core.Signal { return f.notFull }

// NotEmptySignal implements core.Signals.
func (f *Backing[T]) NotEmptySignal() *core.Signal { return f.notEmpty }
