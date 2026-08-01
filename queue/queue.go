package queue

import (
	"errors"
	"fmt"
	"iter"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/telemetry/otel/metrics"
)

// qlock is the shared lock for a Queue and its backing. The mutex is only ever held for
// short critical sections — never across a channel/ctx wait. pending counts writers
// currently blocked on the write lock; a copy-on-write RangeAll watches it to decide
// when to snapshot the remainder and release so a waiting writer can proceed. cowActive
// counts running COW iterators so the common case (no COW active) skips the pending
// atomics in lock() entirely.
type qlock struct {
	mu        sync.RWMutex
	pending   atomic.Int32
	cowActive atomic.Int32
}

// lock acquires the write lock. When a COW iterator is active it bumps pending so
// writeWanted observes the blocked writer; otherwise it skips the two atomics because
// nobody is looking at writeWanted.
func (l *qlock) lock() {
	if l.cowActive.Load() > 0 {
		l.pending.Add(1)
		l.mu.Lock()
		l.pending.Add(-1)
		return
	}
	l.mu.Lock()
}

// unlock releases the write lock.
func (l *qlock) unlock() { l.mu.Unlock() }

// rlock acquires the read lock.
func (l *qlock) rlock() { l.mu.RLock() }

// runlock releases the read lock.
func (l *qlock) runlock() { l.mu.RUnlock() }

// cowEnter / cowExit bracket an AllCOW iterator that intends to observe writeWanted.
// While at least one is active, lock() takes the guarded (pending-bump) path so the
// COW can detect a blocked writer.
func (l *qlock) cowEnter() { l.cowActive.Add(1) }
func (l *qlock) cowExit()  { l.cowActive.Add(-1) }

// writeWanted reports whether a writer is currently blocked waiting for the write lock.
// While a copy-on-write RangeAll holds the read lock a blocked writer cannot acquire, so
// pending stays > 0 until the reader releases — the signal cannot be missed. Only valid
// for COW iterators that have bracketed themselves with cowEnter / cowExit; outside that
// bracket pending is not maintained.
func (l *qlock) writeWanted() bool { return l.pending.Load() > 0 }

const (
	// Unlimited is a constant that can be used to indicate that the queue should be unbounded.
	// This is the default if maxSize is < 1.
	Unlimited = 0

	// hydrateBatch is the number of items the on-disk backing accumulates before flushing
	// them to storage as one batch (one bbolt txn) during hydrate.
	hydrateBatch = 100
)

var (
	// ErrEmpty is returned when the queue is empty and there are no items to pop.
	ErrEmpty = errors.New("queue is empty")
	// ErrClosed is returned when an operation is attempted on a closed queue.
	ErrClosed = errors.New("queue is closed")
	// ErrBatchTooLarge is returned by Push when the batch exceeds the configured max
	// batch size (WithMaxBatch) or a bounded queue's maximum size.
	ErrBatchTooLarge = errors.New("batch larger than allowed maximum")
	// ErrPriorityRequired is returned when an item with Priority() == 0 is pushed onto a priority queue.
	ErrPriorityRequired = errors.New("priority queue requires items with Priority() > 0")
	// ErrPriorityNotAllowed is returned when an item with Priority() > 0 is pushed onto a FIFO queue.
	ErrPriorityNotAllowed = errors.New("FIFO queue requires items with Priority() == 0")
	// ErrCodecRequired is returned when a Value without both Encoder and Decoder set is
	// used with an on-disk (bbolt) backing.
	ErrCodecRequired = errors.New("on-disk queue requires Value.Encoder and Value.Decoder to be set")
)

// Item is type constraint for items that can be stored in the queue.
// There are built in backing implementations that support Item for numeric types, string/[]byte types
// and for generic value types.
type Item[T any] interface {
	// Less returns true if the item is less than the other item.
	Less(T) bool
	// Equal returns true if the item is equal to the other item.
	Equal(T) bool
	// Priority returns the priority sort key. It must be order-consistent with Less:
	// if a.Less(b) then a.Priority() < b.Priority(); items with equal Priority are
	// ordered by insert sequence. Items pushed onto a priority queue must return a
	// value > 0; items pushed onto a FIFO queue must return 0 (the queue rejects a
	// Push that violates this). Only consulted by the on-disk priority backing
	// (NewBboltPriority); other backings sort via Less directly.
	Priority() uint64
	// Hash returns a value-derived bucket key for the WithIndex option. It must be
	// consistent with Equal: if a.Equal(b) then a.Hash() == b.Hash(). Collisions are
	// allowed; Equal still confirms a match.
	Hash() uint64
}

// Backup provides an implementation of a backup for the running queue. As items are written and removed from the
// queue, the backup will be updated to reflect the current state of the queue. This allows for recovery in the
// event of a crash or other failure. This can be useful even if using on disk queues, as this allows recovery
// from off disk backup. Backup calls are made before the main operation is completed. If the backup call fails,
// the main operation will not be attempted and the error will be returned to the caller. If the backup call succeeds
// but the main operation fails, we will attempt a rollback. It is important that the backup implementation have
// typed errors so that you can tell what to do in the event of a failure. Less important for in-memory queues,
// but for on-disk queues you can end in an inconsistent state if the backup succeeds but the main operation fails
// and we cannot roll back. In those cases, it is a good idea to either somehow deal with that the backup a
// queue items that on-disk does not or panic the server and restore from backup to get back to a consistent state.
type Backup[T Item[T]] interface {
	// Push pushes a batch of items onto the backup, mirroring a queue Push. Non-blocking:
	// the backup has no maximum size and cannot be full. Returns an error only on a write
	// failure.
	Push(ctx context.Context, vs []T) error
	// Del removes the given items from the backup, exactly one matching (Item.Equal)
	// occurrence per element of vs. It is called with the precise items removed from the
	// queue — those popped by a Pop and those deleted by a Del — so the backup stays a
	// true mirror regardless of the backing's ordering. An element with no match is a
	// no-op for that element.
	Del(ctx context.Context, vs []T) error
	// Restore re-inserts vs at the front of the backup, in vs order, undoing a Del whose
	// corresponding queue mutation then failed (the on-disk delete or its commit did not
	// land). It is the compensating counterpart of Del so the backup remains a true
	// mirror. For items removed from the head (a Pop) this restores order exactly; for
	// interior items removed by a Del the relative order of vs is preserved at the head
	// (exact positional restore is not possible).
	Restore(ctx context.Context, vs []T) error
	// Len returns the number of items in the queue.
	Len() int64
	// Close closes the queue and releases any resources associated with it. After calling Close, the queue should not be used.
	Close(ctx context.Context) error
	// Clear removes all items from the queue.
	Clear(ctx context.Context) error
	// RangeAll returns an iter.Seq2 that will range over the items in the queue. This should
	// be in the same order as the queue's RangeAll.
	RangeAll(ctx context.Context) iter.Seq2[T, error]
	// OnLoad is called once for each item the queue is hydrated with, in backing order:
	// the items restored from the backup or, for an on-disk backing restarting against an
	// already-populated store, the items recovered from durable storage. This allows
	// restoration to also do side effects such as adding entries to maps or other data
	// structures. A returned error aborts hydration (and thus New). It runs during New,
	// before the queue exists, so it must not perform operations on this queue or its
	// backing store; restrict it to external state.
	OnLoad(ctx context.Context, v T) error
}

// Queue is a generic queue that can be used to store any type of item. The specific implementation of the
// queue depends on the Backing passed to New(). Queue is thread-safe.
type Queue[T Item[T]] struct {
	lk       *qlock
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
	if o.maxBatch == 0 {
		o.maxBatch = 1000
	}
	return o
}

func validateOptions[T Item[T]](o queueOptions) (Backup[T], error) {
	if o.maxBatch < 1 {
		return nil, errors.New("max batch must be at least 1")
	}

	if o.backup == nil {
		return nil, nil
	}
	backup, ok := o.backup.(Backup[T])
	if !ok {
		return nil, errors.New("backup must implement Backup[T]")
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

// validateKind applies validateKindOne to every item in vs. Used by Backings.
func validateKind[T Item[T]](priority bool, vs []T) error {
	for _, v := range vs {
		if err := validateKindOne(priority, v); err != nil {
			return err
		}
	}
	return nil
}

// validateKindOne checks that an item's Priority() matches the backing kind: a priority
// backing requires Priority() > 0, a FIFO backing requires Priority() == 0.
func validateKindOne[T Item[T]](priority bool, v T) error {
	switch {
	case priority && v.Priority() == 0:
		return ErrPriorityRequired
	case !priority && v.Priority() != 0:
		return ErrPriorityNotAllowed
	}
	return nil
}

// New creates a new Queue backed by a Backing. name is used for OTEL metric and traces, if empty string no telemetry
// is recorded for the queue. maxSize is the maximum number of items the queue can hold;
// a value < 1 (or const Unlimited) makes the queue unbounded.
func New[T Item[T]](ctx context.Context, name string, b Backing[T], maxSize int, options ...Option) (*Queue[T], error) {
	if b == nil {
		return nil, fmt.Errorf("backing cannot be nil")
	}

	opts := queueOptions{}
	for _, option := range options {
		opts = option(opts)
	}
	opts = opts.defaults()

	backup, err := validateOptions[T](opts)
	if err != nil {
		return nil, err
	}

	if maxSize < 1 {
		maxSize = 0
	}

	lk := &qlock{}
	b.setQueueLock(lk)
	if err := b.setMaxBatch(opts.maxBatch); err != nil {
		return nil, err
	}
	if err := b.setMaxSize(maxSize); err != nil {
		return nil, err
	}

	q := &Queue[T]{
		backing:  b,
		backup:   backup,
		lk:       lk,
		maxBatch: opts.maxBatch,
		name:     name,
	}
	// An empty name disables telemetry: no meter is built and instrument/recordDepth
	// become no-ops, so nothing is recorded.
	if name != "" {
		meter := context.MeterProvider(ctx).Meter(metrics.MeterName(2))
		q.met = newQueueMetrics(meter)
	}
	if backup != nil {
		if err := b.Hydrate(ctx, backup); err != nil {
			b.Close(ctx)
			return nil, err
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

// opOptions hold per-operation options.
type opOptions struct {
	// sideEffect runs while the queue's lock is held, once the operation is otherwise
	// guaranteed to succeed; a non-nil return rolls the operation back. nil (the
	// default) means no side effect. See WithSideEffect.
	sideEffect func() error
}

// OpOption is an optional argument for queue operations. Options are applied in order, so
// later options override earlier ones. All options are optional; zero values ask for defaults.
type OpOption func(opts opOptions) (opOptions, error)

// resolveOpOptions applies each option in order, propagating any error.
func resolveOpOptions(opts []OpOption) (opOptions, error) {
	o := opOptions{}
	for _, opt := range opts {
		var err error
		o, err = opt(o)
		if err != nil {
			return o, err
		}
	}
	return o, nil
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
// effect already run. The side effect must not call methods on the same queue — that self-deadlocks.
func WithSideEffect(f func() error) OpOption {
	return func(opts opOptions) (opOptions, error) {
		opts.sideEffect = f
		return opts, nil
	}
}

// runSideEffect invokes se if non-nil. Backings call this inside their critical sections, once the
// operation is otherwise guaranteed to succeed; a non-nil return aborts (rolls back) the operation.
func runSideEffect(se func() error) error {
	if se == nil {
		return nil
	}
	return se()
}

// Close closes the queue and releases any resources associated with it. After calling Close,
// the queue should not be used. Any Push/Pop/NotEmpty/NotFull blocked on a full/empty queue
// unblocks and returns ErrClosed, which takes precedence over a simultaneous context
// cancellation. If a side effect is configured it runs under the lock before the close takes
// effect; if it fails the close is rolled back (the queue stays open) and the error returned.
func (q *Queue[T]) Close(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "Close")
	defer func() { done(&err) }()

	err = q.backing.Close(ctx, options...)
	return err
}

// NotEmpty waits until the queue is not empty or the context is cancelled. If the queue
// is closed while blocked it returns ErrClosed (which takes precedence over context
// cancellation).
func (q *Queue[T]) NotEmpty(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "NotEmpty")
	defer func() { done(&err) }()

	err = q.backing.NotEmpty(ctx, options...)
	return err
}

// NotFull waits until the queue is not full or the context is cancelled. If the queue is
// closed while blocked it returns ErrClosed (which takes precedence over context
// cancellation).
func (q *Queue[T]) NotFull(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "NotFull")
	defer func() { done(&err) }()

	err = q.backing.NotFull(ctx, options...)
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
func (q *Queue[T]) Push(ctx context.Context, vs []T, options ...OpOption) (ok bool, err error) {
	ctx, done := q.instrument(ctx, "Push")
	defer func() { done(&err) }()

	if len(vs) == 0 {
		var opts opOptions
		opts, err = resolveOpOptions(options)
		if err != nil {
			return false, err
		}
		if opts.sideEffect != nil {
			q.lk.lock()
			err = opts.sideEffect()
			q.lk.unlock()
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
	defer func() { done(&err) }()

	items, err = q.backing.Pop(ctx, n, options...)
	if err != nil {
		return nil, err
	}
	q.recordDepth(ctx)
	return items, nil
}

// Peek returns the item at the front of the queue without removing it. If the queue is empty the
// second return value will be false. If the queue is not empty, the second return value will be true
// and the first return value will be the item at the front of the queue.
func (q *Queue[T]) Peek(ctx context.Context, options ...OpOption) (v T, ok bool, err error) {
	ctx, done := q.instrument(ctx, "Peek")
	defer func() { done(&err) }()

	v, ok, err = q.backing.Peek(ctx, options...)
	return v, ok, err
}

// Exists returns true if the item exists in the queue. This is useful for checking if an item is in the queue before
// pushing it onto the queue. If we have an index configured, this will use that. If its a btree, this will be
// O log(n). If standard array, this will be O(n), so if your list is large this can be problematic.
func (q *Queue[T]) Exists(ctx context.Context, v T, options ...OpOption) (exists bool, err error) {
	ctx, done := q.instrument(ctx, "Exists")
	defer func() { done(&err) }()

	exists, err = q.backing.Exists(ctx, v, options...)
	return exists, err
}

// Del removes every item from the queue that returns Item.Equal(e) == true for any element e of v
// (all matches, not just one). Duplicate elements in v are idempotent and an empty v is a no-op.
// If no items match, this returns a nil error. If a side effect is configured it runs under the
// lock; if it fails the deletion is rolled back (nothing is removed) and the error returned.
func (q *Queue[T]) Del(ctx context.Context, v []T, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "Del")
	defer func() { done(&err) }()

	if err = q.backing.Del(ctx, v, options...); err != nil {
		return err
	}
	q.recordDepth(ctx)
	return nil
}

// matchesAny reports whether stored Equals any element of vs.
func matchesAny[T Item[T]](stored T, vs []T) bool {
	for i := range vs {
		if stored.Equal(vs[i]) {
			return true
		}
	}
	return false
}

// Len returns the number of items in the queue.
func (q *Queue[T]) Len() int64 {
	return q.backing.Len()
}

// Clear removes all items from the queue. If a side effect is configured it runs under the
// lock; if it fails the clear is rolled back (nothing is removed) and the error returned.
func (q *Queue[T]) Clear(ctx context.Context, options ...OpOption) (err error) {
	ctx, done := q.instrument(ctx, "Clear")
	defer func() { done(&err) }()

	if err = q.backing.Clear(ctx, options...); err != nil {
		return err
	}
	q.recordDepth(ctx)
	return nil
}

// RangeAll returns an iter.Seq2 that ranges over the items in the queue. It holds the
// read lock for the entire iteration, so writers (Push/Pop/Del/Clear) block until the
// sequence is fully consumed or abandoned (early loop exit or context cancellation).
// Do not call a mutating Queue method from inside the loop on the same goroutine — that
// self-deadlocks; use RangeAllCOW if you need writers to make progress during iteration.
func (q *Queue[T]) RangeAll(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		ctx, done := q.instrument(ctx, "RangeAll")
		var rangeErr error
		defer func() { done(&rangeErr) }()
		for v, err := range q.backing.All(ctx) {
			if err != nil {
				rangeErr = err
			}
			if !yield(v, err) {
				return
			}
		}
	}
}

// RangeAllCOW is like RangeAll but does not block writers for the whole iteration. It
// holds the read lock only until a writer is waiting; at that point it copies the
// remaining items into a slice, releases the lock, and finishes iterating over the
// copy while the writer proceeds. The snapshot is taken at the point of contention, so
// items yielded after that reflect the queue state at that moment, not later mutations.
// For on-disk backings the remainder is also copied into memory, which can be large.
func (q *Queue[T]) RangeAllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		ctx, done := q.instrument(ctx, "RangeAllCOW")
		var rangeErr error
		defer func() { done(&rangeErr) }()
		for v, err := range q.backing.AllCOW(ctx) {
			if err != nil {
				rangeErr = err
			}
			if !yield(v, err) {
				return
			}
		}
	}
}

// Backing is the underlying data structure that implements the queue. It is sealed to this
// package; construct one with a backing constructor and pass it to New:
type Backing[T Item[T]] interface {
	// setQueueLock sets the shared lock for the queue and its backing. This is only called once, by New(),
	// and should not be called by implementations directly. The queue and backing use the same lock so that, for example, a RangeAll
	// can hold the read lock while iterating over the backing's items, blocking writers until the iteration is done.
	setQueueLock(lk *qlock)
	// setMaxBatch sets the maximum number of items a single Push may contain (default 1000).
	// A Push with more items than this returns ErrBatchTooLarge. For the on-disk backing this is also
	// the size of the write-staging buffer, so larger values trade memory for fewer, larger group commits.
	// n must be >= 1. This should only be called by the New() constructor.
	setMaxBatch(n int) error
	// setMaxSize sets the maximum number of items the queue can hold. If the queue is bounded and a batch
	// is pushed that exceeds the maximum size, Push returns ErrBatchTooLarge. For unbounded queues, this is
	// a no-op. This should only be called by the New() constructor.
	setMaxSize(n int) error
	// Push pushes a batch of items as a unit (all or none). On a bounded queue an error is
	// returned if the batch cannot ever fit; otherwise it blocks until the whole batch fits
	// or the context is canceled (context.Cause(ctx)). The caller guarantees len(vs) > 0.
	// If Close is called while Push is blocked, the call unblocks and returns ErrClosed;
	// ErrClosed takes precedence over context cancellation (a closed backing returns
	// ErrClosed even if ctx is also canceled). A WithSideEffect option runs under the
	// write lock once the push is otherwise guaranteed to succeed; a non-nil return rolls
	// the push back and is returned.
	Push(ctx context.Context, vs []T, options ...OpOption) error
	// Pop removes and returns up to n items from the front of the queue. It blocks until
	// at least one item is available or the context is canceled (context.Cause(ctx)),
	// then returns 1..n items. The caller guarantees n >= 1. If Close is called while Pop
	// is blocked, the call unblocks and returns ErrClosed; ErrClosed takes precedence over
	// context cancellation. A WithSideEffect option runs under the write lock once
	// the pop is otherwise guaranteed to succeed; a non-nil return rolls the pop back
	// (no items removed) and is returned.
	Pop(ctx context.Context, n int, options ...OpOption) ([]T, error)
	// Peek returns the item at the front of the queue without removing it. If the queue is empty the
	// second return value will be false. If the queue is not empty, the second return value will be true
	// and the first return value will be the item at the front of the queue. A WithSideEffect option
	// runs under the read lock before it is released; its error is returned.
	Peek(ctx context.Context, options ...OpOption) (T, bool, error)
	// Exists returns true if the item exists in the queue. This is useful for checking if an item is in the queue before
	// pushing it onto the queue. We use bloom filters if available and priority also helps. If neither, we have to
	// do a linear scan of the queue, which is O(n) and not ideal. Errors are only for disk issues.
	// A WithSideEffect option runs under the read lock before it is released; its error is returned.
	Exists(ctx context.Context, v T, options ...OpOption) (bool, error)
	// Del removes every item from the queue that returns Item.Equal(e) == true for any element e of v
	// (all matches, not just one). Duplicate elements in v are idempotent and an empty v is a no-op.
	// If no items match, this returns a nil error. Errors are only for disk issues. A WithSideEffect
	// option runs under the write lock once the deletion is otherwise guaranteed to succeed; a
	// non-nil return rolls the deletion back and is returned.
	Del(ctx context.Context, v []T, options ...OpOption) error
	// NotEmpty waits until the queue is not empty or the context is cancelled. If Close
	// is called while blocked it returns ErrClosed; ErrClosed takes precedence over
	// context cancellation. A WithSideEffect option runs under the read lock before
	// it is released; its error is returned.
	NotEmpty(ctx context.Context, options ...OpOption) error
	// NotFull waits until the queue is not full or the context is cancelled. If Close is
	// called while blocked it returns ErrClosed; ErrClosed takes precedence over context
	// cancellation. A WithSideEffect option runs under the read lock before it is
	// released; its error is returned.
	NotFull(ctx context.Context, options ...OpOption) error
	// Len returns the number of items in the queue.
	Len() int64
	// Close closes the queue and releases any resources associated with it. After calling
	// Close, the queue should not be used. Close unblocks any in-flight Push/Pop/NotEmpty/
	// NotFull blocked on a full/empty queue; those calls return ErrClosed (which takes
	// precedence over a simultaneous context cancellation). A WithSideEffect option
	// runs under the write lock before the close takes effect; a non-nil return aborts the
	// close (the backing stays open) and is returned.
	Close(ctx context.Context, options ...OpOption) error
	// Clear removes all items from the queue. Errors are only for disk issues. A WithSideEffect
	// option runs under the write lock once the clear is otherwise guaranteed to succeed; a
	// non-nil return rolls the clear back and is returned.
	Clear(ctx context.Context, options ...OpOption) error
	// All ranges over the items holding the read lock for the whole iteration.
	// Errors are only for disk issues or if the context is canceled.
	All(ctx context.Context) iter.Seq2[T, error]
	// AllCOW ranges over the items but, when a writer is waiting, copies the
	// remainder, releases the lock, and finishes from the copy.
	AllCOW(ctx context.Context) iter.Seq2[T, error]
	// Hydrate loads items from the backup into the backing, calling Backup.OnLoad for each loaded item,
	// then attaches the backup so future mutations mirror to it. Must only be called once,
	// before any other mutation. Items loaded during hydrate do not mirror back to the backup.
	Hydrate(ctx context.Context, b Backup[T]) error

	private() // seal the interface to this package
}
