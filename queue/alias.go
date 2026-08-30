package queue

import (
	"os"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/datastructures/queue/internal/backings/bbolt"
	"github.com/gostdlib/datastructures/queue/internal/backings/btree"
	"github.com/gostdlib/datastructures/queue/internal/backings/btype"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
	"github.com/gostdlib/datastructures/queue/internal/backings/fifo"
	"github.com/gostdlib/datastructures/queue/internal/backings/heap"
)

// This file is the public face of the package. The vocabulary every backing shares —
// the Item/Backup/Backing contract, the lock, the option types and the error taxonomy —
// lives in internal/backings/core so that the backing implementations, which live in
// sibling packages under internal/backings, can reach it without importing this package.
// They cannot: this package imports them, to forward the constructors below.
//
// The names are re-exported here by type alias, not redeclared, so queue.Backing and
// core.Backing are the same type and a backing built in one of those packages satisfies
// the interface a caller names here.

// Unlimited is a constant that can be used to indicate that the queue should be unbounded.
// This is the default if maxSize is < 1.
const Unlimited = core.Unlimited

// Item is the type constraint for items that can be stored in the queue. Implementations
// supply four methods:
//
//   - Less(T) bool: reports whether the receiver sorts before the argument. Must be
//     order-consistent with Priority.
//   - Equal(T) bool: value identity, used by Exists and Del. Must be consistent with Hash.
//   - Priority() uint64: the priority sort key. Items pushed onto a priority queue must
//     return > 0; items pushed onto a FIFO queue must return 0. Only the on-disk priority
//     backing orders by it directly; the others sort via Less.
//   - Hash() uint64: a value-derived bucket key for WithIndex. If a.Equal(b) then
//     a.Hash() == b.Hash(). Collisions are allowed; Equal still confirms a match.
//
// There are built-in implementations for numeric types (Number), string and []byte types
// (String, Bytes) and for arbitrary values (Value).
type Item[T any] = core.Item[T]

// Backup provides an implementation of a backup for the running queue. As items are written
// and removed from the queue, the backup is updated to reflect the current state, which allows
// recovery after a crash. This is useful even with an on-disk queue, since it allows recovery
// from an off-disk backup. Implementations supply:
//
//   - Push(ctx, vs): mirrors a queue Push. Non-blocking; the backup has no maximum size.
//   - Del(ctx, vs): removes exactly one matching (Item.Equal) occurrence per element of vs.
//     It is called with the precise items the queue removed, so the backup stays a true
//     mirror regardless of the backing's ordering. An element with no match is a no-op.
//   - Restore(ctx, vs): re-inserts vs at the front in vs order, undoing a Del whose queue
//     mutation then failed. The compensating counterpart of Del.
//   - Clear(ctx), Close(ctx): as on the queue itself.
//   - RangeAll(ctx): ranges the backup's items, in the same order as the queue's RangeAll.
//   - OnLoad(ctx, v): called once per item during hydration, in backing order, so a caller
//     can rebuild external state. It runs during New, before the queue exists, so it must
//     not operate on this queue or its backing store.
//
// Backup calls are made before the main operation completes. If the backup call fails the main
// operation is not attempted and the error is returned; if the backup succeeds and the main
// operation then fails, the queue attempts a rollback through Restore. Give the implementation
// typed errors so a caller can tell what to do when that rollback is itself impossible.
type Backup[T Item[T]] = core.Backup[T]

// Backing is the underlying data structure that implements the queue. It is sealed: construct
// one with a backing constructor (NewFIFO, NewBTreeFIFO, NewPriority, NewBTreePriority,
// NewBboltFIFO, NewBboltPriority) and pass it to New. Outside code cannot implement it.
type Backing[T Item[T]] = core.Backing[T]

// OpOption is an optional argument for queue operations. Options are applied in order, so
// later options override earlier ones. All options are optional; zero values ask for defaults.
// Some options are valid only for certain operations and return an error elsewhere; see each
// option. A nil func handed to an option is an error, not a no-op.
type OpOption = core.OpOption

// BackingOption is an optional argument to a backing constructor. A single option type is
// shared across constructors; each option validates against the constructor it is given and
// returns an error if it is not valid there.
type BackingOption = core.BackingOption

// The sentinel family. These are the same values the backings return, re-exported so callers
// match them with errors.Is against this package.
var (
	// ErrEmpty is returned when the queue is empty and there are no items to pop. Deliberately
	// not permanent: an empty queue is a state a retry can clear.
	ErrEmpty = core.ErrEmpty
	// ErrClosed is returned when an operation is attempted on a closed queue.
	ErrClosed = core.ErrClosed
	// ErrBadOption tags every error a misused construction or per-operation option produces.
	// The named option sentinels below are built on top of it, so errors.Is(err, ErrBadOption)
	// is the single question "did I configure this wrong?" and each of those is the finer answer.
	ErrBadOption = core.ErrBadOption
	// ErrBatchTooLarge is returned by Push when the batch exceeds the configured max batch size
	// (WithMaxBatch) or a bounded queue's maximum size.
	ErrBatchTooLarge = core.ErrBatchTooLarge
	// ErrPriorityRequired is returned when an item with Priority() == 0 is pushed onto a priority queue.
	ErrPriorityRequired = core.ErrPriorityRequired
	// ErrPriorityNotAllowed is returned when an item with Priority() > 0 is pushed onto a FIFO queue.
	ErrPriorityNotAllowed = core.ErrPriorityNotAllowed
	// ErrCodecRequired is returned when a Value without both Encoder and Decoder set is used
	// with an on-disk (bbolt) backing.
	ErrCodecRequired = core.ErrCodecRequired
	// ErrOnAdmitUnsupported is returned when WithOnAdmit is used on a backing that cannot run a
	// hook between admission and insertion. Only the on-disk backing is in this position.
	ErrOnAdmitUnsupported = core.ErrOnAdmitUnsupported
	// ErrOnAdmitNotPush is returned when WithOnAdmit is given to an operation other than Push.
	ErrOnAdmitNotPush = core.ErrOnAdmitNotPush
	// ErrNilOnAdmit is returned by WithOnAdmit when handed a nil func.
	ErrNilOnAdmit = core.ErrNilOnAdmit
	// ErrNilSideEffect is returned by WithSideEffect when handed a nil func.
	ErrNilSideEffect = core.ErrNilSideEffect
	// ErrOnAdmitFailed wraps whatever a WithOnAdmit hook returned, so a caller can tell its own
	// hook's failure from one the queue produced.
	ErrOnAdmitFailed = core.ErrOnAdmitFailed
	// ErrBackupFailed reports that the caller's Backup failed. See the package docs for the
	// order in which to triage the five caller-code sentinels.
	ErrBackupFailed = core.ErrBackupFailed
	// ErrShutdownIncomplete is returned by Close when the backing could not be released.
	// Deliberately not permanent: two of its three causes are exactly what a retry is for.
	ErrShutdownIncomplete = core.ErrShutdownIncomplete
	// ErrShutdownInProgress is returned to a Close that gave up waiting for another Close that
	// had already committed to releasing. Unlike ErrShutdownIncomplete it does not mean
	// "nothing happened, retry".
	ErrShutdownInProgress = core.ErrShutdownInProgress
	// ErrCodecFailed reports that the item codec failed — the WithCodec funcs, or the default
	// JSON encoding when none was supplied.
	ErrCodecFailed = core.ErrCodecFailed
	// ErrSideEffectFailed wraps whatever a WithSideEffect func returned.
	ErrSideEffectFailed = core.ErrSideEffectFailed
	// ErrItemFailed reports that one of the caller's Item methods ended an operation without
	// returning on the on-disk backing's flusher, which is not the caller's goroutine to lose.
	ErrItemFailed = core.ErrItemFailed
)

// The backing constructors. Each is a forwarder: the implementations live in their own packages
// under internal/backings, and this package imports them, which is why the vocabulary above had
// to move down rather than the other way round. Go has no function aliases and these are generic,
// so a forwarding body is the only mechanism available.

// NewFIFO returns an in-memory FIFO Backing backed by a slice. Use this when queue size is
// going to be < 10K items.
func NewFIFO[T Item[T]]() (Backing[T], error) { return fifo.New[T]() }

// NewBTreeFIFO returns an in-memory FIFO Backing. Without WithIndex it uses the positional
// btype tree (cheapest push/pop; Exists/Del are O(n) scans like NewFIFO). With WithIndex it
// uses a tidwall/btree keyed by insert sequence plus a hash index, giving O(log n) Del and
// O(1) Exists — required for delete-heavy workloads that scan RangeAll and Del/Exists each
// matching entry. WithBTreeWidth applies only to the indexed (keyed btree) variant, but its range
// is validated either way: a width below 2 is an error here as it is everywhere else, rather than
// a silent no-op decided by whether WithIndex happens to be present.
func NewBTreeFIFO[T Item[T]](options ...BackingOption) (Backing[T], error) {
	o, err := core.ApplyBackingOptions(core.CallBTreeFIFO, options)
	if err != nil {
		return nil, err
	}
	if !o.Index {
		return btype.New[T]()
	}
	return btree.NewFIFO[T](o)
}

// NewBTreePriority returns an in-memory priority Backing backed by github.com/tidwall/btree,
// keyed by Item.Less with insert sequence as the tiebreak. It accepts WithBTreeWidth and
// WithIndex. Pass the result to New.
func NewBTreePriority[T Item[T]](options ...BackingOption) (Backing[T], error) {
	o, err := core.ApplyBackingOptions(core.CallBTreePriority, options)
	if err != nil {
		return nil, err
	}
	return btree.NewPriority[T](o)
}

// NewPriority returns an in-memory priority Backing backed by container/heap. Items pop in
// Item.Less order, ties broken by insert order. Pass the result to New.
func NewPriority[T Item[T]]() (Backing[T], error) { return heap.New[T]() }

// diskCodecRequirer is implemented by item types whose default JSON encoding cannot round-trip
// (Value, via its function fields). The check lives here rather than in the bbolt package so that
// the marker, and the built-in item types that carry it, stay in this package: moving them down
// would have made Number/String/Bytes/Value aliases into internal/ for no gain.
type diskCodecRequirer interface {
	requiresDiskCodec()
}

// needsCodec reports whether T cannot round-trip through the default JSON codec and no WithCodec
// was supplied.
func needsCodec[T Item[T]](o core.BackingOpts) bool {
	var zero T
	_, needs := any(zero).(diskCodecRequirer)
	return needs && o.CodecEncode == nil
}

// NewBboltFIFO returns an on-disk FIFO Backing backed by go.etcd.io/bbolt, keyed by insert
// sequence. The database lives in "queue.db" under root and is the source of truth on reopen.
// It accepts WithIndex(). As a disk based queue, it is imporant to optimize with batch pushes and pulls.
// If doing Del(), Exists(), this can be extremely slow without WithIndex(). If using a Backup, it can be better
// to choose a new location for root on restart and using WithNoSync(), WithBoltFreelistMap() and WithNoFreelistSync()
//
// Opening takes bbolt's process-wide lock on the file. By default that wait is unbounded and ctx
// does not bound it either — bolt.Open has no context — so a second handle on the same root blocks
// here until the first is closed. Pass WithBoltTimeout to fail instead of waiting. The default is
// left as an indefinite wait deliberately: a caller handing off a store between processes wants the
// wait, and a finite default would turn that handoff into an error.
func NewBboltFIFO[T Item[T]](ctx context.Context, root *os.Root, options ...BackingOption) (Backing[T], error) {
	o, err := core.ApplyBackingOptions(core.CallBboltFIFO, options)
	if err != nil {
		return nil, err
	}
	if needsCodec[T](o) {
		return nil, ErrCodecRequired
	}
	return bbolt.NewFIFO[T](ctx, root, o)
}

// NewBboltPriority returns an on-disk priority Backing backed by go.etcd.io/bbolt, keyed by
// Item.Priority with insert sequence as the tiebreak. The database lives in "queue.db" under
// root and is the source of truth on reopen. It accepts WithIndex. If doing Del(), Exists(), this can be
// extremely slow without WithIndex(). If using a Backup, it can be better to choose a new location for root on
// restart and using WithNoSync(), WithBoltFreelistMap() and WithNoFreelistSync().
//
// Opening takes bbolt's process-wide lock on the file. By default that wait is unbounded and ctx
// does not bound it either — bolt.Open has no context — so a second handle on the same root blocks
// here until the first is closed. Pass WithBoltTimeout to fail instead of waiting. The default is
// left as an indefinite wait deliberately: a caller handing off a store between processes wants the
// wait, and a finite default would turn that handoff into an error.
func NewBboltPriority[T Item[T]](ctx context.Context, root *os.Root, options ...BackingOption) (Backing[T], error) {
	o, err := core.ApplyBackingOptions(core.CallBboltPriority, options)
	if err != nil {
		return nil, err
	}
	if needsCodec[T](o) {
		return nil, ErrCodecRequired
	}
	return bbolt.NewPriority[T](ctx, root, o)
}
