package fifo

import (
	"fmt"
	"iter"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// Backing is an in-memory FIFO queue backed by a slice.
//
// notFull and notEmpty are signal primitives; mutators call Signal() only when
// HasWaiters() reports a parked waiter, so the steady-state case (no waiters)
// is allocation-free.
type Backing[T core.Item[T]] struct {
	core.Seal
	lk *core.QLock
	// buf holds the queue's storage. The live contents are buf[head:]; everything below head is
	// slots Pop has already handed back, kept only so Push can reuse the space rather than letting
	// append walk the slice forward through memory. Read the contents through live().
	buf []T
	// head is where the live contents start. Pop advances it instead of reslicing buf, which is
	// what makes the space it frees reusable: a reslice moves the base pointer and abandons the
	// prefix for good, so append ran out of tail and reallocated on very nearly every Push.
	head    int
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

// New returns an in-memory FIFO Backing backed by a slice. Use this when queue size is
// going to be < 10K items.
func New[T core.Item[T]]() (core.Backing[T], error) {
	return &Backing[T]{
		lk:       &core.QLock{},
		notFull:  core.NewSignal(),
		notEmpty: core.NewSignal(),
	}, nil
}

// live is the queue's contents: buf minus the prefix Pop has already handed back. Every read of
// the queue goes through here, so "index into the contents" and "index into storage" cannot be
// confused at a call site — which matters most for the positional hydration accounting in Del,
// where i < f.hydrated has to mean the i-th live item.
func (f *Backing[T]) live() []T { return f.buf[f.head:] }

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
		// head is still 0 here: Hydrate runs before the queue is visible to anyone, so nothing
		// has popped yet and buf is the live contents.
		f.buf = append(f.buf, v)
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
		if f.maxSize == 0 || len(f.live())-f.hydrated+f.inflight+len(vs) <= f.maxSize {
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
			wasEmpty := len(f.live()) == 0
			// Reclaim the prefix Pop left behind. head is what makes that space reachable, but
			// nothing reuses it on its own: append extends buf past the dead prefix, so len and
			// head both climb forever and the storage grows with total throughput rather than with
			// queue depth. A queue holding one item would sit on an array proportional to every
			// item that had ever passed through it. Sliding the live contents down over the dead
			// prefix is what bounds that, and it is a different job from the one head does —
			// head stops the per-Push reallocation, this stops the unbounded growth.
			//
			// The slide runs only when append would have grown the array anyway, so it costs
			// nothing while there is room and one copy of the live contents instead of a
			// reallocation when there is not.
			if f.head > 0 && len(f.buf)+len(vs) > cap(f.buf) {
				n := copy(f.buf, f.live())
				// The slide leaves duplicates of live items above n. They would pin those items
				// past their own removal, so they go the same way as every other released slot.
				clear(f.buf[n:])
				f.buf = f.buf[:n]
				f.head = 0
			}
			f.buf = append(f.buf, vs...)
			if wasEmpty && f.notEmpty.HasWaiters() {
				f.notEmpty.Signal()
			}
			f.lk.Unlock()
			return nil
		}
		// f.lk is released by Wait, but only AFTER it has synchronously
		// registered as a waiter (Mesa invariant).
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
		if len(f.live()) > 0 {
			k := n
			if k > len(f.live()) {
				k = len(f.live())
			}
			out := make([]T, k)
			copy(out, f.live()[:k])
			// The side effect and backup mirror both run before anything is removed
			// from the queue, so a failure of either aborts with nothing removed.
			if err := core.RunSideEffectLocked(opts.SideEffect, f.lk.Unlock); err != nil {
				f.lk.Unlock()
				return nil, err
			}
			if f.backup != nil {
				del := func() error { return f.backup.Del(ctx, out) }
				if err := core.RunBackup(del, f.lk.Unlock); err != nil {
					f.lk.Unlock()
					return nil, err
				}
			}
			// Zero what is leaving so the array does not pin it, then advance head. Advancing
			// rather than reslicing is the whole point: a reslice moves buf's base pointer and
			// the space is gone for good, where head leaves it there for Push to slide back into.
			clear(f.live()[:k])
			f.head += k
			// Pop takes from the front, which is exactly where the restored entries are.
			f.dropHydrated(min(k, f.hydrated))
			// Freed capacity: wake any parked producer. Gated on HasWaiters so
			// the steady-state case (no producer waiting) skips the Signal.
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
	if len(f.live()) == 0 {
		return zero, false, core.RunSideEffect(opts.SideEffect)
	}
	return f.live()[0], true, core.RunSideEffect(opts.SideEffect)
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
	for i := range f.live() {
		if f.live()[i].Equal(v) {
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
// f.hydrated positions of f.live(). That is what lets callers classify a removal by index rather
// than by carrying a marker on every entry.
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
	kept := make([]T, 0, len(f.live()))
	var removed []T
	// Positions below f.hydrated hold restored entries; count how many of those actually go.
	removedHydrated := 0
	for i, item := range f.live() {
		if core.MatchesAny(item, v) {
			removed = append(removed, item)
			if i < f.hydrated {
				removedHydrated++
			}
			continue
		}
		kept = append(kept, item)
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
	f.buf, f.head = kept, 0
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
		if len(f.live()) > 0 {
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
		if f.maxSize == 0 || len(f.live())-f.hydrated+f.inflight < f.maxSize {
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
	return int64(len(f.live()))
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
	// Wake any parked Wait callers; they re-acquire f.lk, see closed, return core.ErrClosed.
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
	if len(f.live()) == 0 {
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
	// Only the live contents need zeroing; Pop already zeroed everything below head.
	clear(f.live())
	f.buf, f.head = f.buf[:0], 0
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
		for i := range f.live() {
			select {
			case <-ctx.Done():
				var zero T
				yield(zero, context.Cause(ctx))
				return
			default:
			}
			if !yield(f.live()[i], nil) {
				return
			}
		}
	}
}

// AllCOW implements Backing.AllCOW().
func (f *Backing[T]) AllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		f.lk.CowEnter()
		defer f.lk.CowExit()
		f.lk.RLock()
		// The read lock is released on each exit below, which a panic out of the caller's iterator
		// body — or out of the caller's codec — skips entirely, leaking it for the life of the
		// process and blocking every later writer. The flag keeps the explicit releases (which
		// must happen before yielding unlocked) while the defer covers the unwind.
		held := true
		release := func() {
			if held {
				held = false
				f.lk.RUnlock()
			}
		}
		defer release()
		if f.closed {
			release()
			yield(zero, core.ErrClosed)
			return
		}
		for i := 0; i < len(f.live()); i++ {
			if f.lk.WriteWanted() {
				snap := append([]T(nil), f.live()[i:]...)
				release()
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
				return
			}
			select {
			case <-ctx.Done():
				release()
				yield(zero, context.Cause(ctx))
				return
			default:
			}
			if !yield(f.live()[i], nil) {
				release()
				return
			}
		}
		release()
	}
}

// NotFullSignal implements core.Signals.
func (f *Backing[T]) NotFullSignal() *core.Signal { return f.notFull }

// NotEmptySignal implements core.Signals.
func (f *Backing[T]) NotEmptySignal() *core.Signal { return f.notEmpty }
