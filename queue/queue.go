package queue

import (
	"errors"
	"fmt"
	"iter"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/telemetry/otel/metrics"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// Queue is a generic queue that can be used to store any type of item. The specific implementation of the
// queue depends on the Backing passed to New(). Queue is thread-safe.
type Queue[T Item[T]] struct {
	lk       *core.QLock
	backing  Backing[T]
	backup   Backup[T]
	maxBatch int
	// name labels OTEL spans/metrics for this queue (the queue.name attribute). An
	// empty name disables telemetry entirely: no spans or metrics are recorded.
	name string
	// met holds the OTEL instruments. It is nil when name == "" (telemetry disabled);
	// otherwise non-nil. The instruments are also no-op safe when no exporter is
	// configured.
	met *queueMetrics
}

type queueOptions struct {
	maxBatch int
	backup   any
}

func (o queueOptions) defaults() queueOptions {
	o.maxBatch = 1000
	return o
}

func validateOptions[T Item[T]](o queueOptions) (Backup[T], error) {
	if o.maxBatch < 1 {
		return nil, fmt.Errorf("%w: WithMaxBatch must be at least 1, got %d", ErrBadOption, o.maxBatch)
	}

	if o.backup == nil {
		return nil, nil
	}
	backup, ok := o.backup.(Backup[T])
	if !ok {
		return nil, fmt.Errorf("%w: WithBackup must be given a Backup[T] for the queue's item type", ErrBadOption)
	}

	return backup, nil
}

// Option is optional arguments for New().
type Option func(o queueOptions) queueOptions

// WithMaxBatch sets the maximum number of items a single Push may contain (default 1000).
// A Push with more items than this returns ErrBatchTooLarge. For the on-disk backing this
// is also the size of the write-staging buffer, so larger values trade memory for fewer,
// larger group commits. n must be >= 1.
func WithMaxBatch(n int) Option {
	return func(o queueOptions) queueOptions {
		o.maxBatch = n
		return o
	}
}

// WithBackup configures the queue to use the provided backup. b must be a Backup for the queue type. This allows
// for recovery in the event of a crash or other failure. This can be useful even if using on disk queues,
// as this allows recovery from off disk backup. If WithBackup is used, the queue will be updated to
// a starting state that matches the backup, and all operations on the queue will be reflected in the backup.
func WithBackup(b any) Option {
	return func(o queueOptions) queueOptions {
		o.backup = b
		return o
	}
}

// New creates a new Queue backed by a Backing. name is used for OTEL metric and traces, if empty string no telemetry
// is recorded for the queue. maxSize is the maximum number of items the queue can hold;
// a value < 1 (or const Unlimited) makes the queue unbounded.
func New[T Item[T]](ctx context.Context, name string, b Backing[T], maxSize int, options ...Option) (*Queue[T], error) {
	if b == nil {
		return nil, fmt.Errorf("%w: backing cannot be nil", ErrBadOption)
	}

	opts := queueOptions{}.defaults()
	for _, option := range options {
		opts = option(opts)
	}

	backup, err := validateOptions[T](opts)
	if err != nil {
		return nil, err
	}

	if maxSize < 1 {
		maxSize = 0
	}

	lk := &core.QLock{}
	b.SetQueueLock(lk)
	if err := b.SetMaxBatch(opts.maxBatch); err != nil {
		return nil, err
	}
	if err := b.SetMaxSize(maxSize); err != nil {
		return nil, err
	}

	q := &Queue[T]{
		backing:  b,
		backup:   backup,
		lk:       lk,
		maxBatch: opts.maxBatch,
		name:     name,
	}
	if name != "" {
		meter := context.MeterProvider(ctx).Meter(metrics.MeterName(2))
		q.met = newQueueMetrics(meter, name)
	}
	if backup != nil {
		// Hydrate runs caller code on this goroutine — Backup.RangeAll and Backup.OnLoad, the item
		// codec, and Item.Hash through the index — so it can end this frame without returning. The
		// error return below was the only cleanup, which meant a panic or a runtime.Goexit out of
		// any of those leaked both the backing (for the on-disk one, the bolt file lock, held for
		// the life of the process with no queue in existence to Close) and the caller's Backup,
		// which nothing else is ever going to close. The release performs the same two closes the
		// error path does, best-effort: there is no error return left to report them on.
		//
		// runLocked is used for its completion flag, not for a lock — recover() reports nothing
		// during a Goexit, so a guard written around it covers only half the cases. It fires only
		// when Hydrate does not return, so the error path below still owns its own closes.
		release := func() {
			b.Close(ctx)
			backup.Close(ctx)
		}
		if err := core.RunLocked(func() error { return b.Hydrate(ctx, backup) }, release); err != nil {
			// Joined, not discarded: this is the one path where the queue is guaranteed not to
			// exist for the caller to retry with, so a Backup that also failed to close — or a
			// leaked bolt handle — would be invisible.
			//
			// The Backup is closed here rather than left to b.Close, because b.Close cannot reach
			// it: every backing attaches the Backup as the last statement of a successful Hydrate,
			// so on this path the backing's own field is still nil and its Close skips the Backup
			// entirely. That is deliberate — it is what structurally guarantees Hydrate's "items
			// loaded during hydrate do not mirror back to the backup" — so the close belongs to
			// the one caller that knows a Backup was handed over and no queue is coming back.
			// Backing is sealed (private()), so those are all the implementations there are and
			// nothing here can double-close. Ordered after b.Close so the backing is finished with
			// the queue before the caller's Backup is told it is done.
			return nil, errors.Join(err, b.Close(ctx), core.WrapBackup(backup.Close(ctx)))
		}
	}
	// Seed the depth counter and lastDepth from any hydrated items so the
	// queue.depth UpDownCounter reflects the true absolute depth from t0.
	// (lastDepth is still at its zero value here, so this emits +Len() and
	// stores Len(); skipping it would under-report depth by the hydrated
	// count and let the counter go negative if those items drain first.)
	q.recordDepth(ctx)
	return q, nil
}

// WithSideEffect adds a side effect that runs while the queue's lock is still held, so no other queue
// operation can interleave between the operation and its side effect. Mutating operations
// (Push/Pop/Del/Clear/Close) run it under the write lock once the operation is otherwise guaranteed to
// succeed; if the side effect returns an error the operation is rolled back (nothing is mutated) and the
// error is returned to the caller. Read operations (Peek/Exists/NotEmpty/NotFull) run it under the read
// lock, so side effects of concurrent readers may run concurrently with each other. If the side effect
// succeeds but the operation subsequently fails (a backup mirror or on-disk commit failure), the
// operation is rolled back but the side effect cannot be un-run; for the on-disk backing a Push side
// effect runs at buffer-admission time, so a later commit failure is reported by Push with the side
// effect already run.
//
// The side effect must not call methods on the same queue.
//
// One backing does not run the side effect on the caller's goroutine: bbolt's Clear hands it to the
// queue's flusher. A panic there is reported to the Clear caller as ErrSideEffectFailed rather than
// reaching the caller's own recover, and a runtime.Goexit ends the flusher, which closes the queue.
//
// The error a side effect returns is wrapped in ErrSideEffectFailed, so a caller can tell its own
// failure from one the queue produced; the original stays reachable through errors.Is/As.
//
// A nil f is an error (ErrNilSideEffect), not a no-op: passing one means the caller believed a side
// effect was configured when none was, and the operation would otherwise run without it. Callers
// with an optional hook should omit the option rather than pass a nil func.
func WithSideEffect(f func() error) OpOption {
	return func(o core.OpOptions) (core.OpOptions, error) {
		if f == nil {
			return o, ErrNilSideEffect
		}
		o.SideEffect = f
		return o, nil
	}
}

// WithOnAdmit adds a hook that Push runs after it has reserved capacity for the batch but before it
// inserts anything, with the queue's lock released. It exists for the work a caller must do between
// "there is room for this" and "this is queued" — writing the item to durable storage, most often —
// which cannot go in WithSideEffect because that runs under the write lock, where a slow or hung
// call stalls every other operation on the queue.
//
// The reservation is what makes this safe to do unlocked: the capacity is counted against maxSize
// for as long as the hook runs, so concurrent Pushes and NotFull see the queue as already holding
// these items and cannot overshoot the bound. If the hook returns an error the reservation is
// released, nothing is inserted, and the error is returned to the caller wrapped in
// ErrOnAdmitFailed, so a hook failure is distinguishable from a queue failure.
//
// Two consequences follow from the lock being released. Other operations can interleave between
// admission and insertion, so unlike WithSideEffect this is explicitly not atomic with the Push;
// and insertion order is decided after the hook returns, so concurrent Pushes may be ordered
// differently than they were admitted. For a priority queue that affects only the tiebreak among
// items of equal priority.
//
// If the queue is closed while the hook runs, Push returns ErrClosed with the hook already run —
// the same shape as a backup mirror failing after a side effect. The same holds if the side effect
// or the backup mirror fails after the hook succeeded. If the hook panics the reservation is still
// released before the panic continues, so a panicking hook does not shrink the queue's capacity.
//
// Backings that have no point corresponding to "reserved but not written" reject this option with
// ErrOnAdmitUnsupported rather than ignoring it, and passing it to any operation other than Push is
// ErrOnAdmitNotPush rather than a silent no-op. A nil f is ErrNilOnAdmit. An empty batch is admitted
// without reaching a backing, so the hook does not run: there is nothing to admit and nothing for
// the caller to make durable.
//
// The hook must not call methods on the same queue.
func WithOnAdmit(f func() error) OpOption {
	return func(o core.OpOptions) (core.OpOptions, error) {
		if f == nil {
			return o, ErrNilOnAdmit
		}
		if o.Call != core.CallPush {
			return o, fmt.Errorf("%w: got %s", ErrOnAdmitNotPush, o.Call)
		}
		o.OnAdmit = f
		return o, nil
	}
}

// Close closes the queue and releases any resources associated with it. After calling Close,
// the queue should not be used. Any Push/Pop/NotEmpty/NotFull blocked on a full/empty queue
// unblocks and returns ErrClosed, which takes precedence over a simultaneous context
// cancellation. If a side effect is configured it runs under the lock before the close takes
// effect; if it fails the close is rolled back (the queue stays open) and the error returned.
func (q *Queue[T]) Close(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "Close")
	completed := false
	defer done.end(&err, &completed)

	err = q.backing.Close(ctx, options...)
	completed = true
	return err
}

// NotEmpty waits until the queue is not empty or the context is cancelled. If the queue
// is closed while blocked it returns ErrClosed (which takes precedence over context
// cancellation).
func (q *Queue[T]) NotEmpty(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "NotEmpty")
	completed := false
	defer done.end(&err, &completed)

	err = q.backing.NotEmpty(ctx, options...)
	completed = true
	return err
}

// NotFull waits until the queue is not full or the context is cancelled. If the queue is
// closed while blocked it returns ErrClosed (which takes precedence over context
// cancellation).
func (q *Queue[T]) NotFull(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "NotFull")
	completed := false
	defer done.end(&err, &completed)

	err = q.backing.NotFull(ctx, options...)
	completed = true
	return err
}

// Push pushes a batch of items onto the queue as a unit: either all items are pushed or
// none are. An empty or nil batch is a no-op that returns (true, nil); it does not
// consult the backing, so this holds even on a closed queue. A batch with more
// than the configured max batch size (WithMaxBatch, default 1000) returns ErrBatchTooLarge,
// as does a batch larger than a bounded queue's maximum size. Otherwise Push blocks until
// the whole batch fits or the context is canceled, in which case context.Cause(ctx) is
// returned. If the queue is closed while a Push is blocked it returns (false, ErrClosed);
// ErrClosed takes precedence over a simultaneous context cancellation. If a side effect is
// configured it runs under the lock; if it fails the push is rolled back (nothing is
// pushed) and (false, err) is returned. The second return value reports whether the batch
// is in the queue: it is true exactly when err == nil.
//
// Push is the only operation that accepts WithOnAdmit; see that option for what it runs and when.
// A backing with no point corresponding to "capacity reserved, nothing written" — the on-disk one —
// returns ErrOnAdmitUnsupported instead of running it. An empty batch never reaches a backing, so
// it neither runs the hook nor reports that error.
func (q *Queue[T]) Push(ctx context.Context, vs []T, options ...OpOption) (ok bool, err error) {
	ctx, done := q.instrument(ctx, "Push")
	completed := false
	defer done.end(&err, &completed)

	ok, err = q.push(ctx, vs, options...)
	completed = true
	return ok, err
}

// push is Push without the instrumentation. It is a separate function only so Push has exactly one
// place to mark the operation completed: with the body inline, the completion flag would have to be
// set on each of five return paths, and the one a future edit forgot would silently record a
// successful operation as an abnormal unwind.
func (q *Queue[T]) push(ctx context.Context, vs []T, options ...OpOption) (ok bool, err error) {
	if len(vs) == 0 {
		var opts core.OpOptions
		opts, err = core.ResolveOpOptions(core.CallPush, options)
		if err != nil {
			return false, err
		}
		if opts.SideEffect != nil {
			q.lk.Lock()
			err = core.RunSideEffectLocked(opts.SideEffect, q.lk.Unlock)
			q.lk.Unlock()
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	if len(vs) > q.maxBatch {
		err = ErrBatchTooLarge
		return false, err
	}

	if err = q.backing.Push(ctx, vs, options...); err != nil {
		return false, err
	}
	q.recordDepth(ctx)
	return true, nil
}

// Pop removes and returns up to n items from the front of the queue. n must be >= 1 or
// this will panic. Pop blocks until at least one item is available (or the context
// is canceled, returning context.Cause(ctx)), then returns between 1 and n items —
// whatever is available without further blocking. If the queue is closed while a Pop is
// blocked it returns ErrClosed; ErrClosed takes precedence over a simultaneous context
// cancellation. The returned slice is non-empty on a nil error. If a side effect is
// configured it runs under the lock; if it fails the pop is rolled back (no items are
// removed) and (nil, err) is returned.
func (q *Queue[T]) Pop(ctx context.Context, n int, options ...OpOption) (items []T, err error) {
	if n < 1 {
		panic("invalid argument: n must be >= 1")
	}
	ctx, done := q.instrument(ctx, "Pop")
	completed := false
	defer done.end(&err, &completed)

	items, err = q.backing.Pop(ctx, n, options...)
	if err != nil {
		completed = true
		return nil, err
	}
	q.recordDepth(ctx)
	completed = true
	return items, nil
}

// Peek returns the item at the front of the queue without removing it. If the queue is empty the
// second return value will be false. If the queue is not empty, the second return value will be true
// and the first return value will be the item at the front of the queue.
func (q *Queue[T]) Peek(ctx context.Context, options ...OpOption) (v T, ok bool, err error) {
	ctx, done := q.instrument(ctx, "Peek")
	completed := false
	defer done.end(&err, &completed)

	v, ok, err = q.backing.Peek(ctx, options...)
	completed = true
	return v, ok, err
}

// Exists returns true if the item exists in the queue. This is useful for checking if an item is in the queue before
// pushing it onto the queue. If we have an index configured, this will use that. If its a btree, this will be
// O log(n). If standard array, this will be O(n), so if your list is large this can be problematic.
func (q *Queue[T]) Exists(ctx context.Context, v T, options ...OpOption) (exists bool, err error) {
	ctx, done := q.instrument(ctx, "Exists")
	completed := false
	defer done.end(&err, &completed)

	exists, err = q.backing.Exists(ctx, v, options...)
	completed = true
	return exists, err
}

// Del removes every item from the queue that returns Item.Equal(e) == true for any element e of v
// (all matches, not just one). Duplicate elements in v are idempotent and an empty v is a no-op.
// If no items match, this returns a nil error. If a side effect is configured it runs under the
// lock; if it fails the deletion is rolled back (nothing is removed) and the error returned.
//
// count is the number of entries removed, not the number of elements of v that matched: one query
// matching three queued entries reports 3, a duplicated query does not double-count, and a query
// matching nothing reports 0 with a nil error. On any error count is 0 — no partial deletion is
// ever reported, because every backing rolls back rather than removing part of a batch.
func (q *Queue[T]) Del(ctx context.Context, v []T, options ...OpOption) (count int, err error) {
	ctx, done := q.instrument(ctx, "Del")
	completed := false
	defer done.end(&err, &completed)

	count, err = q.backing.Del(ctx, v, options...)
	if err != nil {
		completed = true
		return 0, err
	}
	q.recordDepth(ctx)
	completed = true
	return count, nil
}

// Len returns the number of items in the queue.
func (q *Queue[T]) Len() int64 {
	return q.backing.Len()
}

// Clear removes all items from the queue. If a side effect is configured it runs under the
// lock; if it fails the clear is rolled back (nothing is removed) and the error returned.
func (q *Queue[T]) Clear(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "Clear")
	completed := false
	defer done.end(&err, &completed)

	if err = q.backing.Clear(ctx, options...); err != nil {
		completed = true
		return err
	}
	q.recordDepth(ctx)
	completed = true
	return nil
}

// RangeAll ranges over the items in the queue. It holds the
// read lock for the entire iteration, so writers (Push/Pop/Del/Clear) block until the
// sequence is fully consumed or abandoned (early loop exit or context cancellation).
// Do not call a mutating Queue method from inside the loop on the same goroutine — that
// self-deadlocks. RangeAllCOW lets writers on OTHER goroutines make progress during the
// iteration, which is a different thing: on most backings it does not make a mutation from
// inside the loop body safe either. See RangeAllCOW for which ones do.
func (q *Queue[T]) RangeAll(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		ctx, done := q.instrument(ctx, "RangeAll")
		var rangeErr error
		completed := false
		defer done.end(&rangeErr, &completed)
		// The loop is a closure so an early exit (the consumer abandoning the range) is a normal
		// completion of the iteration, and only an unwind out of the consumer's loop body or the
		// backing leaves completed false.
		func() {
			for v, err := range q.backing.All(ctx) {
				if err != nil {
					rangeErr = err
				}
				if !yield(v, err) {
					return
				}
			}
		}()
		completed = true
	}
}

// RangeAllCOW is like RangeAll but does not block writers for the whole iteration. It
// holds the read lock only until a writer is waiting; at that point it copies the
// remaining items into a slice, releases the lock, and finishes iterating over the
// copy while the writer proceeds. The snapshot is taken at the point of contention, so
// items yielded after that reflect the queue state at that moment, not later mutations.
// For on-disk backings the remainder is also copied into memory, which can be large.
//
// "A writer is waiting" means a writer on another goroutine. Calling a mutating Queue method
// from inside the loop body on the iterating goroutine is not that, and on most backings it
// self-deadlocks just as it does under RangeAll: the read lock this loop is holding is what the
// mutation's write lock is waiting for, and the only thing that would release it is this loop
// getting to its next step. Two backings take their whole snapshot up front and yield with no
// lock held at all, so on those — and only those — a mutation from inside the body is safe:
//
//   - NewPriority (the in-memory priority heap), which sorts a copy under the read lock and
//     releases it before the first yield.
//   - NewBTreeFIFO without WithIndex (the positional copy-on-write B-tree), which takes an
//     O(1) copy-on-write snapshot under the write lock and releases it before the first yield.
//
// Every other backing — NewFIFO, NewBTreeFIFO with WithIndex, NewBTreePriority, and both
// on-disk backings with or without WithIndex — holds the read lock across the yields until
// another goroutine contends, and deadlocks on a same-goroutine mutation. Do not rely on the
// two exceptions unless the backing is fixed at the call site; collect what you want to change
// during the loop and apply it after the loop ends, which is correct on all of them.
//
// An error yield is the exception to all of that. Whether the error is ErrClosed, a context
// cancellation, or a codec failure, every backing releases before handing it over, so that loop
// body runs with no lock held and may mutate the queue freely. It is also the last yield of the
// iteration: the sequence ends there.
func (q *Queue[T]) RangeAllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		ctx, done := q.instrument(ctx, "RangeAllCOW")
		var rangeErr error
		completed := false
		defer done.end(&rangeErr, &completed)
		// The loop is a closure so an early exit (the consumer abandoning the range) is a normal
		// completion of the iteration, and only an unwind out of the consumer's loop body or the
		// backing leaves completed false.
		func() {
			for v, err := range q.backing.AllCOW(ctx) {
				if err != nil {
					rangeErr = err
				}
				if !yield(v, err) {
					return
				}
			}
		}()
		completed = true
	}
}
