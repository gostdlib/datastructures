package core

import (
	"iter"

	"github.com/gostdlib/base/context"
)

// Unlimited is a constant that can be used to indicate that the queue should be unbounded.
// This is the default if maxSize is < 1.
const Unlimited = 0

// Seal carries the unexported method that seals Backing. A backing implementation embeds it,
// which promotes private() into that type's method set; because this package is internal, no
// code outside this module can embed it, so Backing stays unimplementable from outside.
type Seal struct{}

func (Seal) private() {}

// Item is type constraint for items that can be stored in the queue.
// There are built in backing implementations that support Item for numeric types, string/[]byte types
// and for generic value types.
type Item[T any] interface {
	// Less returns true if the item is less than the other item.
	Less(T) bool
	// Equal returns true if the item is equal to the other item.
	Equal(T) bool
	// Priority returns the priority sort key. A higher value is more desirable and is
	// dequeued sooner. It must be order-consistent with Less: if a.Less(b) then
	// a.Priority() > b.Priority(); items with equal Priority are ordered by insert
	// sequence.
	//
	// That ordering clause binds only items destined for a priority queue. A FIFO item
	// must return 0, which makes the clause unsatisfiable for any Less that orders two
	// items strictly -- and harmless, because no FIFO backing consults Less at all: they
	// order by insert sequence alone. A FIFO item is free to implement Less however it
	// likes, including as a constant false.
	//
	// Items pushed onto a priority queue must return a value > 0; items pushed onto a
	// FIFO queue must return 0 (the queue rejects a Push that violates this). Zero is
	// reserved for that queue-kind gate, so it is not usable as a "lowest priority"
	// value: the lowest priority a priority queue accepts is 1.
	//
	// Only consulted by the on-disk priority backing (NewBboltPriority); other backings
	// sort via Less directly.
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

// Backing is the underlying data structure that implements the queue. It is sealed to this
// package; construct one with a backing constructor and pass it to New:
type Backing[T Item[T]] interface {
	// SetQueueLock sets the shared lock for the queue and its backing. This is only called once, by New(),
	// and should not be called by implementations directly. The queue and backing use the same lock so that, for example, a RangeAll
	// can hold the read lock while iterating over the backing's items, blocking writers until the iteration is done.
	SetQueueLock(lk *QLock)
	// SetMaxBatch sets the maximum number of items a single Push may contain (default 1000).
	// A Push with more items than this returns ErrBatchTooLarge. For the on-disk backing this is also
	// the size of the write-staging buffer, so larger values trade memory for fewer, larger group commits.
	// n must be >= 1. This should only be called by the New() constructor.
	SetMaxBatch(n int) error
	// SetMaxSize sets the maximum number of items the queue can hold. If the queue is bounded and a batch
	// is pushed that exceeds the maximum size, Push returns ErrBatchTooLarge. For unbounded queues, this is
	// a no-op. This should only be called by the New() constructor.
	SetMaxSize(n int) error
	// Push pushes a batch of items as a unit (all or none). On a bounded queue an error is
	// returned if the batch cannot ever fit; otherwise it blocks until the whole batch fits
	// or the context is canceled (context.Cause(ctx)). The caller guarantees len(vs) > 0.
	// If Close is called while Push is blocked, the call unblocks and returns ErrClosed;
	// ErrClosed takes precedence over context cancellation (a closed backing returns
	// ErrClosed even if ctx is also canceled). A WithSideEffect option runs under the
	// write lock once the push is otherwise guaranteed to succeed; a non-nil return rolls
	// the push back and is returned.
	//
	// A WithOnAdmit option obliges the implementation to either honor it — reserve capacity
	// for the batch, release the lock, run the hook, retake the lock and insert, counting
	// the reservation in both the maxSize gate and NotFull for as long as the hook runs —
	// or return ErrOnAdmitUnsupported. Ignoring it is not an option: the hook carries the
	// caller's durable write. An implementation that honors it also owes a wakeup to any
	// producer waiting on NotFull whenever it gives a reservation back without inserting.
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
	// non-nil return rolls the deletion back and is returned. The returned int is the number of items deleted.
	Del(ctx context.Context, v []T, options ...OpOption) (int, error)
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
	//
	// The attach is the last thing a successful Hydrate does, and a failing one never does it:
	// that is what makes "items loaded during hydrate do not mirror back" structural rather than
	// an argument about which lines happen to touch the field. It also means a backing whose
	// Hydrate failed has no Backup to close, so closing it is New's job, not the backing's.
	//
	// Whether the loaded items count against maxSize is the one place the backings deliberately
	// differ. The in-memory backings exempt them: a restore is not an admission, so a backup
	// larger than the bound still loads and still leaves the whole bound free for new pushes,
	// with the exemption decaying as the restored entries leave. The on-disk backing does not —
	// it admits through a staging buffer and counts restored items normally, so restoring past
	// the bound blocks new pushes until the store drains.
	Hydrate(ctx context.Context, b Backup[T]) error

	// private is satisfied only by embedding Seal, which lives in this internal package.
	// That is what keeps Backing unimplementable from outside the module.
	private()
}

// Signals exposes a backing's two wait signals. Every backing implements it. It exists so that a
// test can wait for a parked producer or consumer without naming the concrete backing type: the
// alternative was a type switch listing every backing, which silently stopped covering whichever
// one was added last.
type Signals interface {
	// NotFullSignal is the signal a blocked Push parks on and a removal wakes.
	NotFullSignal() *Signal
	// NotEmptySignal is the signal a blocked Pop parks on and an insert wakes.
	NotEmptySignal() *Signal
}

// ClosedOrCauser is the helper every backing uses in the ctx.Done() arm of a blocked wait: it
// reports ErrClosed if the backing has been closed and the context's cause otherwise, so a Close
// deterministically wins a race with a cancellation. Exported so it can be tested directly rather
// than only through the timing of a real blocked operation.
type ClosedOrCauser interface {
	ClosedOrCause(ctx context.Context) error
}
