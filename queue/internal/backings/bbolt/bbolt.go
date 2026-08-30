package bbolt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
	"iter"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/go-json-experiment/json"
	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	bolt "go.etcd.io/bbolt"
)

const (
	// hydrateBatch is the number of items the on-disk backing accumulates before flushing
	// them to storage as one batch (one bbolt txn) during hydrate.
	hydrateBatch = 100

	// flushInterval is the upper bound on how long a buffered item waits before being
	// committed; group commit normally flushes much sooner.
	flushInterval = 100 * time.Millisecond

	// drainDefault bounds the final flush at shutdown. It only has to be long enough for a slow
	// durable Backup to finish the batch already in hand, and short enough that a Backup which never
	// returns cannot hang Close forever.
	drainDefault = 30 * time.Second

	// joinSlackDefault is what the join gets on top of the drain it is waiting for. The join
	// budget was once equal to the drain budget, which meant a drain finishing comfortably inside its
	// own limit still blew the join: Close reported core.ErrShutdownIncomplete, released nothing, and left
	// the file lock held, while the flusher finished moments later and an immediate retry succeeded.
	joinSlackDefault = 30 * time.Second
)

var (
	itemsBucket = []byte("items")
	// errStopIter terminates a bbolt ForEach/cursor walk early. It never escapes a method.
	errStopIter = errors.New("stop iteration")
)

// Hooks is a per-instance set of test seams. Tests set fields on a specific
// *Backing[T] before any flusher work runs; every field is nil in production.
// Per-instance (rather than package globals) so concurrent tests on different queues
// do not step on each other.
type Hooks struct {
	// FaultAfterBackup, when non-nil, is invoked inside Pop and Del immediately after the
	// backup has been mirrored but before the on-disk delete is applied; a non-nil return
	// aborts the transaction, exercising the Restore compensation path.
	FaultAfterBackup func() error
	// CommitStart, when non-nil, is invoked at the very start of commit(), before any
	// backup mirror or db.Update, so a test can pin the flusher mid-commit and race a
	// concurrent Clear/Close against it. commit() runs only in the single flusher
	// goroutine, so a plain func is race-free here.
	CommitStart func()
	// DBClose, when non-nil, stands in for the shutdown's call to db.Close. It is handed the real
	// close so a test can still release the bolt handle, and whatever it returns is what the
	// shutdown records as cErr. It exists because that is the one failure no other seam can
	// produce: cErr is set by a defer that runs after the performer's return expression has
	// already been evaluated, which is the exact ordering the "every caller gets the same answer"
	// contract turns on, and every other injectable failure is set before that point.
	DBClose func(close func() error) error
}

// flushResult is shared by every Push whose items joined one buffered batch. The flusher
// sets err then closes done; waiters read err after done is closed.
type flushResult struct {
	done chan struct{}
	err  error
}

// clearCmd is a synchronous Clear request routed to the flusher goroutine. Routing
// through the flusher serializes Clear with all in-flight commits: any item the
// flusher is about to commit (or is mid-commit on) is drained first, then the bucket
// is deleted. Without this routing a Clear concurrent with a mid-commit Push would
// delete the bucket before the commit's db.Update landed, letting the item reappear.
type clearCmd struct {
	ctx context.Context
	// sideEffect is the Clear caller's WithSideEffect func (nil when unset); doClear runs
	// it under the lock before the clear is applied.
	sideEffect func() error
	done       chan error
}

func jsonDecode[T any](data []byte) (T, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// diskCodecRequirer is implemented by item types whose default JSON encoding cannot
// round-trip (Value, via its function fields). NewBboltFIFO/NewBboltPriority reject such
// a type with core.ErrCodecRequired unless WithCodec supplies a codec.
type diskCodecRequirer interface {
	requiresDiskCodec()
}

// bboltBacking is an on-disk queue backed by go.etcd.io/bbolt, used for both FIFO and
// priority. Each item is stored in the "items" bucket under a key built by keyOf:
//   - FIFO:     8-byte big-endian per-bucket sequence number
//   - priority: 8-byte big-endian complement of Item.Priority() followed by the 8-byte
//     sequence number
//
// bbolt iterates keys in byte-lexicographic order, so the head of the queue is the
// bucket's first key: insert order for FIFO, highest Item.Priority value (insert order
// breaking ties) for priority. The complement is what makes the second of those true --
// a higher priority is more desirable, and complementing turns "highest priority" into
// "smallest key" without disturbing the sequence half. The priority key is a fixed 16
// bytes, so it is inherently prefix-free.
//
// Items are encoded with github.com/go-json-experiment/json. Recovery is automatic: on
// Open the existing database is the source of truth.
//
// Semantics (blocking, hydration, backup mirror) mirror fifo, with one on-disk Hydrate
// exception: items are only loaded from the backup when the database is empty (a non-empty
// database is the source of truth and would otherwise be duplicated). On a non-empty
// (restart) database no items are loaded from the backup, but Backup.OnLoad is still
// driven once per persisted item, in stored order, so callers rebuilding external state
// get the same callback they would for an in-memory backing. lk, maxSize and maxBatch are
// injected by New via SetQueueLock, SetMaxSize and SetMaxBatch before any other use; the
// flush goroutine is started by SetQueueLock so it never races the lock injection.
type Backing[T core.Item[T]] struct {
	core.Seal
	lk    *core.QLock
	db    *bolt.DB
	keyOf func(v T, seq uint64) []byte
	idx   *index
	count int64
	// inflight is the number of items admitted into the staging buffer but not yet
	// committed (buffered or snapshot-in-flight). Guarded by lk like count. The bounded
	// maxSize admission gate tests count+inflight so concurrent Pushes whose items are
	// still buffered cannot collectively overshoot maxSize. (Named distinctly from
	// core.QLock.pending, which counts writers blocked on the lock.)
	inflight int64
	maxSize  int
	maxBatch int
	priority bool
	notFull  *core.Signal
	notEmpty *core.Signal
	closed   bool
	backup   core.Backup[T]

	// Write-staging buffer. push appends here and blocks until the flusher commits the
	// batch. buf, cur and flushReq are guarded by lk; cur.done/err follow the
	// flushResult protocol. The flusher runs in flushGroup and stops when flushCancel
	// is called (close()).
	buf         []T
	cur         *flushResult
	flushReq    chan struct{}
	clearReq    chan *clearCmd
	flushCtx    context.Context
	flushCancel context.CancelFunc
	flushGroup  sync.Group

	// On-disk codec from WithCodec; nil falls back to the default JSON encoding.
	encode func(dst *bytes.Buffer, v T) error
	decode func(src []byte, dst *T) error

	// hooks holds optional test seams. Both fields are nil in production.
	Hooks Hooks
	// shutOnce runs the whole shutdown — stop the flusher, wait for it, release the Backup and
	// the bolt handle — exactly once, and makes every other caller wait for it rather than skip
	// it. A plain "already done" flag is not enough: a second Close would sail past it while the
	// first was still inside flushGroup.Wait and report success with the bolt file lock still
	// held, and it would tear the db down under a live flusher, closing the store out from under
	// an accepted batch and calling the caller's Backup.Close inside its own in-flight
	// Backup.Push. It is also why Wait is in here: Group.Wait is not safe for two callers.
	// The shutdown is deliberately not a sync.Once. A Once runs its function while holding a
	// mutex every later caller blocks on, which turns a single caller's problem into everyone's:
	// caller code the flusher runs can call Close, and joining the flusher from the flusher
	// deadlocked not just that caller but every unrelated Close behind it. A Once is also marked
	// done when its function panics, so a panicking Backup.Close released late callers with a nil
	// error and a Backup that was never closed. This shape runs no caller code under a lock, lets
	// a failed attempt be retried, and bounds every wait.
	shutMu       sync.Mutex
	shutBusy     bool
	shutComplete bool
	// shutCommitted says the flusher has been joined and the release is under way. Past that
	// point nothing can be retried, so a waiter that gives up needs a different answer than the
	// one that means "nothing was released".
	shutCommitted bool
	shutDone      chan struct{}
	shutGErr      error
	shutBErr      error
	shutCErr      error
	// flusherStarted says whether SetQueueLock ever launched the flusher. A backing constructed
	// but never handed to New has none, and waiting for a goroutine that was never started would
	// burn the whole join deadline before releasing anything. Atomic rather than lock-guarded
	// because the shutdown reads it while holding the shutdown token, and p.lk can be held by any
	// caller code at that moment — an unbounded acquisition there stalls every other Close.
	flusherStarted atomic.Bool
	// flusherDone is closed when flushLoop returns, however it returns — the deferred close runs
	// on a panic and on runtime.Goexit too. It exists because sync.Group.Wait cannot be bounded:
	// its own doc says the context it takes cannot cancel it, so joining through it means a
	// Backup that ignores ctx hangs Close with no way out.
	flusherDone chan struct{}
	// inClear is true while runClear is on the stack. It is read and written only from the
	// flusher goroutine, which is single-threaded, so it needs no lock. doFlush's unwind guard
	// reads it to know whether an outer handler is going to deliver a result and cancel the
	// flush context, or whether it has to do that itself.
	inClear bool
	// drain and slack are this backing's shutdown budgets, seeded from the package defaults.
	// Fields rather than package globals, for the same reason hooks is per-instance: tests
	// shorten them so a bound can be observed inside a test's own deadline, and a global written
	// by one test's cleanup races the flusher of every queue another test left running — the
	// flusher reads the drain on its way out, and t.Context() is cancelled just before cleanups
	// run. They are written once by the constructor, before SetQueueLock starts the flusher; a
	// test overriding them does so in that same window, which is ordered ahead of every read by
	// the flush-context cancellation that wakes the reader.
	drain time.Duration
	slack time.Duration
}

// drainBudget bounds the final flush at shutdown. See drainDefault.
func (p *Backing[T]) drainBudget() time.Duration { return p.drain }

// joinBudget bounds how long Close waits for the flusher to stop, and how long a second Close
// waits for the first. Past it Close reports core.ErrShutdownIncomplete having released nothing, which
// is the safe answer: releasing under a live flusher destroys an accepted batch. Derived rather
// than fixed so that shortening the drain shortens this too and keeps the containment relationship.
func (p *Backing[T]) joinBudget() time.Duration { return p.drain + p.slack }

// encodeItem serializes v for storage using the WithCodec encoder if set, else the
// default JSON encoding.
func (p *Backing[T]) encodeItem(v T) ([]byte, error) {
	// Tagged here rather than at the fourteen call sites, so core.ErrCodecFailed covers every returned
	// codec error the way core.ErrBackupFailed covers every returned Backup error. Without it a caller
	// could not tell "my decoder rejected this row" from "the store is corrupt".
	if p.encode != nil {
		var buf bytes.Buffer
		if err := p.encode(&buf, v); err != nil {
			return nil, core.WrapCodec(err)
		}
		return buf.Bytes(), nil
	}
	data, err := json.Marshal(v)
	return data, core.WrapCodec(err)
}

// decodeItem is the inverse of encodeItem.
func (p *Backing[T]) decodeItem(data []byte) (T, error) {
	if p.decode != nil {
		var v T
		if err := p.decode(data, &v); err != nil {
			var zero T
			return zero, core.WrapCodec(err)
		}
		return v, nil
	}
	v, err := jsonDecode[T](data)
	return v, core.WrapCodec(err)
}

// index maps Item.Hash() to the bbolt storage keys in that bucket, so Exists/Del
// only Get+decode the bucket members instead of scanning the whole table. nil when WithIndex
// is not set. Stored keys are the exact bbolt keys, so bucket.Delete is O(log n).
type index struct {
	m map[uint64][][]byte
}

func newIndex() *index {
	return &index{m: map[uint64][][]byte{}}
}

func (x *index) add(hash uint64, storageKey []byte) {
	x.m[hash] = append(x.m[hash], storageKey)
}

func (x *index) remove(hash uint64, storageKey []byte) {
	s := x.m[hash]
	for i := range s {
		if bytes.Equal(s[i], storageKey) {
			s[i] = s[len(s)-1]
			x.m[hash] = s[:len(s)-1]
			break
		}
	}
	if len(x.m[hash]) == 0 {
		delete(x.m, hash)
	}
}

func (x *index) bucket(hash uint64) [][]byte {
	return x.m[hash]
}

// fifoKey orders by insert sequence only (FIFO).
func fifoKey[T core.Item[T]](_ T, seq uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, seq)
	return out
}

// priorityKey orders by Item.Priority() with insert sequence as a tiebreak. A higher
// Priority is more desirable, and bbolt walks keys in ascending byte order, so the
// priority half is stored complemented: the largest Priority becomes the smallest eight
// bytes and sorts to the head. The sequence half is stored as-is, so items sharing a
// priority still come out in insert order. The fixed 16-byte width (8-byte complemented
// priority || 8-byte seq) is inherently prefix-free.
func priorityKey[T core.Item[T]](v T, seq uint64) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[:8], ^v.Priority())
	binary.BigEndian.PutUint64(out[8:], seq)
	return out
}

// NewFIFO returns an on-disk FIFO Backing keyed by insert sequence. Options are already
// resolved by the caller, which also performs the item-type codec check.
func NewFIFO[T core.Item[T]](ctx context.Context, root *os.Root, o core.BackingOpts) (core.Backing[T], error) {
	return newBacking(ctx, root, o, fifoKey[T], false)
}

// NewPriority returns an on-disk priority Backing keyed by Item.Priority with the insert
// sequence as the tiebreak. Options are already resolved by the caller.
func NewPriority[T core.Item[T]](ctx context.Context, root *os.Root, o core.BackingOpts) (core.Backing[T], error) {
	return newBacking(ctx, root, o, priorityKey[T], true)
}

func newBacking[T core.Item[T]](ctx context.Context, root *os.Root, o core.BackingOpts, keyOf func(v T, seq uint64) []byte, priority bool) (core.Backing[T], error) {
	var encode func(*bytes.Buffer, T) error
	var decode func([]byte, *T) error
	if o.CodecEncode != nil {
		e, okE := o.CodecEncode.(func(*bytes.Buffer, T) error)
		d, okD := o.CodecDecode.(func([]byte, *T) error)
		if !okE || !okD {
			return nil, fmt.Errorf("%w: WithCodec encoder/decoder do not match the queue item type", core.ErrBadOption)
		}
		encode, decode = e, d
	}
	var zero T
	if _, needs := any(zero).(diskCodecRequirer); needs && encode == nil {
		return nil, core.ErrCodecRequired
	}

	bopts := &bolt.Options{
		Timeout:         o.BoltTimeout,
		NoSync:          o.BoltNoSync,
		NoFreelistSync:  o.BoltNoFreelistSync,
		NoGrowSync:      o.BoltNoGrowSync,
		PreLoadFreelist: o.BoltPreLoadFreelist,
		Mlock:           o.BoltMlock,
		MmapFlags:       o.BoltMmapFlags,
		InitialMmapSize: o.BoltInitialMmapSize,
		PageSize:        o.BoltPageSize,
		OpenFile:        o.BoltOpenFile,
	}
	if o.BoltFreelistMap {
		bopts.FreelistType = bolt.FreelistMapType
	}
	path := filepath.Join(root.Name(), "queue.db")
	db, err := bolt.Open(path, 0o600, bopts)
	if err != nil {
		// The path is the whole message when this is a lock contention: bbolt's own error for a
		// file another handle already holds is the bare string "timeout", which tells an operator
		// nothing about which store or why. bolt.ErrTimeout stays reachable through errors.Is, so
		// no new sentinel is needed here — what was missing was the context around it.
		return nil, fmt.Errorf("queue: opening the bbolt store at %q: %w", path, err)
	}
	var count int64
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(itemsBucket)
		if err != nil {
			return err
		}
		count = int64(b.Stats().KeyN)
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	p := &Backing[T]{
		lk:       &core.QLock{},
		db:       db,
		keyOf:    keyOf,
		count:    count,
		priority: priority,
		notFull:  core.NewSignal(),
		notEmpty: core.NewSignal(),
		cur:      &flushResult{done: make(chan struct{})},
		flushReq: make(chan struct{}, 1),
		clearReq: make(chan *clearCmd, 1),
		encode:   encode,
		decode:   decode,
		drain:    drainDefault,
		slack:    joinSlackDefault,
	}
	if o.Index {
		p.idx = newIndex()
		// rebuildIndex runs the caller's code on this goroutine — Item.Hash for every record, and a
		// WithCodec decoder if one was given — so it can end this frame without returning. Handling
		// only the returned error left a panic or a runtime.Goexit skipping db.Close, and the bolt
		// file lock is process-wide and held until the handle is closed: the next NewBboltFIFO on
		// that path blocks (or, with WithBoltTimeout, fails) for the life of the process, with no
		// queue in existence for anyone to Close. runLocked decides on a completion flag rather
		// than on recover() because recover() reports nothing during a Goexit; the panic still
		// keeps unwinding with its own stack, this only makes sure the handle does not go with it.
		if err := core.RunLocked(p.rebuildIndex, func() { db.Close() }); err != nil {
			db.Close()
			return nil, err
		}
	}
	fctx, cancel := context.WithCancel(ctx)
	p.flushCtx = fctx
	p.flushCancel = cancel
	p.flusherDone = make(chan struct{})
	p.shutDone = make(chan struct{})
	// The flusher runs for the whole life of the queue, so it must not be submitted to a Limited
	// pool: it would hold one of that pool's slots until Close, and a caller creating more queues
	// than the limit allows would find New blocking forever with nothing able to break the tie —
	// the submit context has its cancellation stripped (see SetQueueLock), so not even the caller's
	// own ctx frees it. Pool.Default() is what the worker package provides for exactly this: a
	// long-lived goroutine that has to run outside the caller's bound. An unlimited pool is left
	// alone so a caller's custom pool still runs and still reports the flusher.
	pool := context.Pool(ctx)
	if pool.Limit() > 0 {
		pool = pool.Default()
	}
	p.flushGroup = pool.Group()
	// The flush goroutine is started by SetQueueLock so it never observes the placeholder
	// lock created above being swapped for the shared one injected by New.
	return p, nil
}

// SetQueueLock injects the shared lock and starts the flush goroutine. New calls this once,
// before any other use, so the goroutine never races the lock assignment.
func (p *Backing[T]) SetQueueLock(lk *core.QLock) {
	p.lk = lk
	// Set before the submit, not from inside flushLoop, and it has to stay that way. Pool.Submit
	// enqueues and returns; the job runs later on one of the pool's runners, so a New immediately
	// followed by a Close can reach the join before flushLoop has executed a single statement.
	// Setting it there would have that Close read false, skip both the flusherDone join and
	// flushGroup.Wait, and release under a flusher that had not started — trading a deterministic
	// join for a scheduling race, and dropping whatever error the flusher went on to report.
	//
	// The cost of setting it here is that a submit the pool declined would leave flusherDone never
	// closed, so every Close would burn p.joinBudget() and report core.ErrShutdownIncomplete without ever
	// releasing the bolt file lock. That is unreachable: the flusher only ever lands on an
	// unlimited pool (see below), and submit declines only on an already-cancelled context, which
	// the WithoutCancel below rules out.
	p.flusherStarted.Store(true)
	// Launch the flusher with a non-cancelable accounting context: the worker pool
	// skips a job whose launch context is already canceled, which would happen if the
	// queue is created then closed before the pooled worker starts (the flusher would
	// never run, nor do its final flush). WithoutCancel strips only the
	// cancellation/deadline while preserving context values (the gostdlib pool, logger
	// and OTEL tracer ride on ctx). flushLoop's own shutdown is driven by p.flushCtx,
	// which it selects on.
	p.flushGroup.Go(context.WithoutCancel(p.flushCtx), func(context.Context) error { return p.flushLoop(p.flushCtx) })
}

func (p *Backing[T]) SetMaxBatch(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: max batch must be at least 1, got %d", core.ErrBadOption, n)
	}
	p.maxBatch = n
	return nil
}

func (p *Backing[T]) SetMaxSize(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: max size must be at least 0, got %d", core.ErrBadOption, n)
	}
	p.maxSize = n
	return nil
}

// flushLoop is the group-commit loop: it commits the write buffer as soon as it is
// signaled (every append signals), so the first buffered item flushes immediately and
// items arriving during an in-flight commit coalesce into the next one. The ticker is an
// upper bound. On ctx cancel (close) it does a final flush and returns.
func (p *Backing[T]) flushLoop(ctx context.Context) error {
	// However this goroutine ends — return, panic, or runtime.Goexit — say so, so a join can be
	// bounded on something that is actually closed rather than on a Wait that never returns.
	defer close(p.flusherDone)
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.doFlush(ctx, &codeSubject{})
			// The flusher is the only thing that can ever commit for this queue, so its exit has to
			// close the backing whether it was Close that cancelled the context or the caller. Close
			// sets closed itself before cancelling, so for it this changes nothing. For a caller who
			// cancelled the context handed to NewBboltFIFO/NewBboltPriority it changes everything:
			// the queue used to go on reporting itself open with nobody left to commit, so the next
			// Push buffered, blocked on a flush result that could never arrive — a wait that is
			// documented as not context-cancelable — and Close returned nil without freeing it.
			// cancel is false because this context is already done and nobody is still on the way
			// with a result for Clear to prefer.
			p.closeFromFlusher(false, ctx.Err())
			return nil
		case <-p.flushReq:
			p.doFlush(ctx, &codeSubject{})
		case cmd := <-p.clearReq:
			p.runClear(cmd)
		case <-t.C:
			p.doFlush(ctx, &codeSubject{})
		}
	}
}

// doFlush snapshots the buffer under the lock, rotates the flushResult so new pushes join
// the next batch, commits the snapshot, then signals the waiters of this batch.
func (p *Backing[T]) doFlush(ctx context.Context, blamed *codeSubject) {
	// A flush that runs after the flush context is cancelled still has to reach the caller's
	// Backup with a live context: this drain exists to unblock pushers still waiting on their
	// batch, and handing it a dead context fails every one of them. Substituting here rather than
	// in the shutdown arm is deliberate — which arm of the select notices the cancellation first
	// is a coin flip, and the flushReq, ticker and pre-clear drain arms were all handing the dead
	// one straight through. Bounded, because stripping cancellation with nothing in its place
	// lets a Backup that waits on ctx.Done() hang the drain forever.
	if ctx.Err() != nil {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.drainBudget())
		defer cancel()
		ctx = dctx
	}
	p.lk.Lock()
	if len(p.buf) == 0 {
		p.lk.Unlock()
		return
	}
	snap := p.buf
	p.buf = nil
	r := p.cur
	p.cur = &flushResult{done: make(chan struct{})}
	p.lk.Unlock()

	// commit calls into the Backup, the item codec and the caller's Item methods, all of them
	// caller-supplied, on this goroutine. If any of them ends the frame without returning, the
	// pushers waiting on r are owed a result and the capacity their batch reserved is owed back —
	// and the flusher itself must not be the thing that dies, because nothing restarts it. A panic
	// is converted into the batch's error and the flusher carries on; a Goexit cannot be stopped,
	// so the backing is closed instead of left silently unable to commit anything ever again.
	finished := false
	released := false
	// commit runs three different bodies of caller code — the Backup, the item codec, then the
	// Item's own methods — and an unwind can come out of any of them. Blaming whichever one the
	// guard happened to name told a queue configured with no Backup at all that its Backup had
	// failed; naming none of them told a queue whose Item panicked that the queue itself had. The
	// subject belongs to the caller, so a drain run on someone else's behalf reports through to
	// them.
	defer func() {
		if finished {
			return
		}
		if !released {
			p.releaseInflight(int64(len(snap)))
		}
		blameFlush := func(how string) error {
			if blamed.what == "" {
				return fmt.Errorf("the queue %s while committing", how)
			}
			return fmt.Errorf("%w: %s %s", blamed.sentinel, blamed.what, how)
		}
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); ok {
				// Keep an error panic value reachable through errors.Is/As.
				r.err = fmt.Errorf("%w: %w", blameFlush("panicked"), e)
			} else {
				r.err = fmt.Errorf("%w: %v", blameFlush("panicked"), rec)
			}
			close(r.done)
			return
		}
		r.err = blameFlush("ended the flusher goroutine")
		close(r.done)
		// If runClear is on the stack above, it still owes its caller a result and will cancel
		// once it has sent one; cancelling here would beat that send.
		p.closeFromFlusher(!p.inClear, blameFlush("ended the flusher goroutine"))
	}()
	r.err = p.commit(ctx, snap, &released, blamed)
	close(r.done)
	finished = true
}

// commit mirrors the batch to the backup (if any), writes it to bbolt in one transaction,
// then updates count/index/notEmpty under the lock. A backup or write failure fails the
// whole batch (returned to every waiter); the items are not made visible.
func (p *Backing[T]) commit(ctx context.Context, snap []T, released *bool, blamed *codeSubject) error {
	if p.Hooks.CommitStart != nil {
		p.Hooks.CommitStart()
	}
	blamed.setBackup(p.backup, "the Backup")
	if p.backup != nil {
		if err := p.backup.Push(ctx, snap); err != nil {
			p.releaseInflight(int64(len(snap)))
			*released = true
			return core.WrapBackup(err)
		}
	}
	type idxEnt struct {
		hash uint64
		sk   []byte
	}
	var ents []idxEnt
	// The Backup is done with. The codec subject is set around the encode call itself below and
	// nowhere wider: this transaction also runs bbolt's own code and the caller's Item methods via
	// keyOf, and blaming the codec for those told a queue with no WithCodec at all that its codec
	// had failed. Narrowing it left the Item methods with no subject of their own, which is not
	// the same as them being the queue's code — they get one below.
	blamed.clear()
	err := p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(itemsBucket)
		for _, v := range snap {
			blamed.setCodec()
			data, err := p.encodeItem(v)
			blamed.clear()
			if err != nil {
				return err
			}
			seq, err := b.NextSequence()
			if err != nil {
				return err
			}
			// keyOf calls Item.Priority on the priority backing and the index needs Item.Hash:
			// caller code, set and cleared around each call the way the codec's is, so bbolt's
			// own Put in between is not billed to it.
			blamed.setItem()
			sk := p.keyOf(v, seq)
			blamed.clear()
			if err := b.Put(sk, data); err != nil {
				return err
			}
			if p.idx != nil {
				blamed.setItem()
				hash := v.Hash()
				blamed.clear()
				ents = append(ents, idxEnt{hash: hash, sk: sk})
			}
		}
		return nil
	})
	if err != nil {
		p.releaseInflight(int64(len(snap)))
		*released = true
		return err
	}
	// Deferred, not explicit: doFlush's unwind guard calls releaseInflight, which takes this same
	// lock. An unwind anywhere in this section — p.idx.add is the only thing that could — would
	// otherwise leave the lock held and deadlock the guard against it, which is precisely the
	// failure the guard exists to prevent. *released tells the guard the capacity is already back
	// so it cannot subtract twice.
	wasEmpty := func() bool {
		p.lk.Lock()
		defer p.lk.Unlock()
		was := p.count == 0
		p.count += int64(len(snap))
		p.inflight -= int64(len(snap))
		*released = true
		if p.idx != nil {
			for _, e := range ents {
				p.idx.add(e.hash, e.sk)
			}
		}
		return was
	}()
	if wasEmpty && p.notEmpty.HasWaiters() {
		p.notEmpty.Signal()
	}
	return nil
}

// rebuildIndex scans the existing database once and populates the in-memory index. Called
// at construction so a reopened on-disk queue (the source of truth) has a consistent index.
func (p *Backing[T]) rebuildIndex() error {
	return p.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(itemsBucket).ForEach(func(k, data []byte) error {
			item, err := p.decodeItem(data)
			if err != nil {
				return err
			}
			sk := append([]byte(nil), k...)
			p.idx.add(item.Hash(), sk)
			return nil
		})
	})
}

// Hydrate implements Backing.Hydrate().
func (p *Backing[T]) Hydrate(ctx context.Context, b core.Backup[T]) error {
	p.lk.Lock()
	defer p.lk.Unlock()
	if p.count > 0 {
		// The on-disk store already holds items: this is a restart restoring from
		// bbolt's own durable storage rather than from the backup. Still drive OnLoad
		// for each persisted item (in stored order) so callers rebuilding external
		// structures get the same callback they would for an in-memory backing.
		if err := p.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(itemsBucket).Cursor()
			for k, data := c.First(); k != nil; k, data = c.Next() {
				v, err := p.decodeItem(data)
				if err != nil {
					return err
				}
				if err := b.OnLoad(ctx, v); err != nil {
					return core.WrapBackup(err)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		p.backup = b
		return nil
	}
	type pending struct {
		v    T
		data []byte
	}
	var buf []pending
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		sks := make([][]byte, 0, len(buf))
		err := p.db.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(itemsBucket)
			for i := range buf {
				seq, err := bkt.NextSequence()
				if err != nil {
					return err
				}
				sk := p.keyOf(buf[i].v, seq)
				if err := bkt.Put(sk, buf[i].data); err != nil {
					return err
				}
				sks = append(sks, sk)
			}
			return nil
		})
		if err != nil {
			return err
		}
		p.count += int64(len(buf))
		if p.idx != nil {
			for i := range buf {
				p.idx.add(buf[i].v.Hash(), sks[i])
			}
		}
		buf = buf[:0]
		return nil
	}
	for v, err := range b.RangeAll(ctx) {
		if err != nil {
			return core.WrapBackup(err)
		}
		if err := core.ValidateKindOne(p.priority, v); err != nil {
			return err
		}
		if err := b.OnLoad(ctx, v); err != nil {
			return core.WrapBackup(err)
		}
		data, err := p.encodeItem(v)
		if err != nil {
			return err
		}
		buf = append(buf, pending{v: v, data: data})
		if len(buf) == hydrateBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	p.backup = b
	return nil
}

// Push implements Backing.Push(). It stages the batch in the write buffer (capacity
// maxBatch) and blocks until the flusher commits it. Over-maxBatch batches are rejected by
// Queue.Push before reaching here, so len(vs) <= maxBatch always holds; on a bounded queue
// a batch larger than maxSize returns core.ErrBatchTooLarge. When concurrent pushes have
// momentarily filled the shared maxBatch buffer this Push flushes and retries (it always
// fits once drained); that pre-buffer wait is context-cancelable, but once the items are
// buffered the wait for the flush to commit is not.
//
// The side effect runs under the lock at buffer-admission time, before the items are
// staged: a side-effect failure aborts with nothing staged. A commit failure after
// admission fails the Push, but the side effect has already run and cannot be un-run.
func (p *Backing[T]) Push(ctx context.Context, vs []T, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallPush, options)
	if err != nil {
		return err
	}
	if err := core.ValidateKind(p.priority, vs); err != nil {
		return err
	}
	for {
		p.lk.Lock()
		if p.closed {
			p.lk.Unlock()
			return core.ErrClosed
		}
		if p.maxSize > 0 && int64(len(vs)) > int64(p.maxSize) {
			p.lk.Unlock()
			return core.ErrBatchTooLarge
		}
		if opts.OnAdmit != nil {
			// This backing admits into a staging buffer and commits in groups, so it has no
			// moment that means "capacity is reserved and nothing is written yet" — the point
			// WithOnAdmit hands to the caller. Refuse rather than run the hook at a moment that
			// does not match what it promises. Ordered after the kind, closed and batch-size
			// checks so this backing reports the same error for those as every other one.
			p.lk.Unlock()
			return core.ErrOnAdmitUnsupported
		}
		if p.maxSize > 0 && p.count+p.inflight+int64(len(vs)) > int64(p.maxSize) {
			if err := p.notFull.Wait(ctx, p.lk.Unlock); err != nil {
				return p.ClosedOrCause(ctx)
			}
			continue
		}
		if len(p.buf)+len(vs) <= p.maxBatch {
			if err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock); err != nil {
				p.lk.Unlock()
				return err
			}
			p.buf = append(p.buf, vs...)
			p.inflight += int64(len(vs))
			r := p.cur
			p.lk.Unlock()
			// Group commit: signal on every append, not just when full. The flusher
			// commits immediately when idle; pushes that arrive while a commit is in
			// flight coalesce into the next batch. The interval timer is only an upper
			// bound, and the maxBatch cap only forces backpressure.
			select {
			case p.flushReq <- struct{}{}:
			default:
			}
			<-r.done // not ctx-cancelable once buffered
			return r.err
		}
		// Buffer lacks room: trigger a flush and wait for it to drain. This wait is
		// context-cancelable because the items are not yet buffered.
		r := p.cur
		p.lk.Unlock()
		select {
		case p.flushReq <- struct{}{}:
		default:
		}
		select {
		case <-r.done:
		case <-ctx.Done():
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
		if p.count > 0 {
			k := n
			if int64(k) > p.count {
				k = int(p.count)
			}
			out := make([]T, 0, k)
			var sks [][]byte
			if p.idx != nil {
				sks = make([][]byte, 0, k)
			}
			// Peek + mirror + delete must be one transaction: the flush goroutine's
			// commit() runs db.Update without holding p.lk, so a separate read txn
			// could see a higher-priority item inserted between the peek and the delete.
			backupDeleted := false
			err := p.db.Update(func(tx *bolt.Tx) error {
				c := tx.Bucket(itemsBucket).Cursor()
				// Pass 1: read the first k items in order without removing them.
				key, data := c.First()
				for i := 0; i < k; i++ {
					if key == nil {
						return core.ErrEmpty
					}
					// Guarded like the side effect and the Backup calls below: the caller's
					// codec runs here holding the write lock, and this path unlocks explicitly,
					// so an unwind out of it would carry the lock away and wedge the queue.
					var dv T
					if err := core.RunLocked(func() error {
						var e error
						dv, e = p.decodeItem(data)
						return e
					}, p.lk.Unlock); err != nil {
						return err
					}
					out = append(out, dv)
					if p.idx != nil {
						sks = append(sks, append([]byte(nil), key...))
					}
					key, data = c.Next()
				}
				// The side effect runs before the backup mirror and the deletes: an
				// error rolls back the txn with nothing deleted and the backup untouched.
				if err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock); err != nil {
					return err
				}
				// Mirror the exact popped items to the backup before removing them.
				// An error here rolls back the txn so nothing is deleted.
				if p.backup != nil {
					del := func() error { return p.backup.Del(ctx, out) }
					if err := core.RunBackup(del, p.lk.Unlock); err != nil {
						return err
					}
					backupDeleted = true
				}
				if p.Hooks.FaultAfterBackup != nil {
					if e := p.Hooks.FaultAfterBackup(); e != nil {
						return e
					}
				}
				// Pass 2: delete the first k items.
				for i := 0; i < k; i++ {
					dk, _ := c.First()
					if dk == nil {
						return core.ErrEmpty
					}
					if err := c.Delete(); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				// The txn rolled back (or the commit did not durably land) but the
				// backup was already mirrored, so the items are still on disk.
				// Restore them to the front of the backup (these were the head
				// items) to keep it a true mirror, then report the failure; if the
				// restore also fails, both errors are returned and the backup is
				// genuinely out of sync.
				if backupDeleted {
					restore := func() error { return p.backup.Restore(ctx, out) }
					if rerr := core.RunBackup(restore, p.lk.Unlock); rerr != nil {
						p.lk.Unlock()
						return nil, errors.Join(err, rerr)
					}
				}
				p.lk.Unlock()
				return nil, err
			}
			if p.idx != nil {
				// idx.remove calls the caller's Item.Hash, and this path unlocks explicitly, so an
				// unwind out of Hash would carry the lock away and wedge the queue. The items are
				// already gone from bbolt by here, so the release only unlocks.
				core.RunLockedNoErr(func() {
					for i, v := range out {
						p.idx.remove(v.Hash(), sks[i])
					}
				}, p.lk.Unlock)
			}
			p.count -= int64(k)
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
	if p.count == 0 {
		return zero, false, core.RunSideEffect(opts.SideEffect)
	}
	var v T
	err = p.db.View(func(tx *bolt.Tx) error {
		_, data := tx.Bucket(itemsBucket).Cursor().First()
		if data == nil {
			return core.ErrEmpty
		}
		dv, err := p.decodeItem(data)
		if err != nil {
			return err
		}
		v = dv
		return nil
	})
	if err != nil {
		return zero, false, err
	}
	return v, true, core.RunSideEffect(opts.SideEffect)
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
	found := false
	err = p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(itemsBucket)
		if p.idx != nil {
			for _, sk := range p.idx.bucket(v.Hash()) {
				data := b.Get(sk)
				if data == nil {
					continue
				}
				item, err := p.decodeItem(data)
				if err != nil {
					return err
				}
				if item.Equal(v) {
					found = true
					return errStopIter
				}
			}
			return nil
		}
		return b.ForEach(func(_, data []byte) error {
			item, err := p.decodeItem(data)
			if err != nil {
				return err
			}
			if item.Equal(v) {
				found = true
				return errStopIter
			}
			return nil
		})
	})
	if err != nil && !errors.Is(err, errStopIter) {
		return false, err
	}
	return found, core.RunSideEffect(opts.SideEffect)
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
	if p.count == 0 {
		return 0, core.RunSideEffect(opts.SideEffect)
	}
	// Collect the keys and items matching v without deleting them. The lock is held
	// for the whole Del, so the database cannot change before the delete below.
	var keys [][]byte
	var items []T
	// khash[i] is the index-bucket hash for keys[i] (only filled on the indexed path,
	// which is the only path that later calls idx.remove).
	var khash []uint64
	if err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(itemsBucket)
		if p.idx != nil {
			// Scan only the buckets for the distinct hashes in v; dedup collected
			// keys by string(sk) so a slot matched by duplicate/same-hash elements
			// of v is removed once.
			seenHash := make(map[uint64]struct{}, len(v))
			seenKey := make(map[string]struct{})
			for e := range v {
				h := v[e].Hash()
				if _, ok := seenHash[h]; ok {
					continue
				}
				seenHash[h] = struct{}{}
				for _, sk := range p.idx.bucket(h) {
					if _, ok := seenKey[string(sk)]; ok {
						continue
					}
					data := b.Get(sk)
					if data == nil {
						continue
					}
					item, err := p.decodeItem(data)
					if err != nil {
						return err
					}
					if core.MatchesAny(item, v) {
						seenKey[string(sk)] = struct{}{}
						keys = append(keys, append([]byte(nil), sk...))
						items = append(items, item)
						khash = append(khash, h)
					}
				}
			}
			return nil
		}
		return b.ForEach(func(k, data []byte) error {
			item, err := p.decodeItem(data)
			if err != nil {
				return err
			}
			if core.MatchesAny(item, v) {
				keys = append(keys, append([]byte(nil), k...))
				items = append(items, item)
			}
			return nil
		})
	}); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, core.RunSideEffect(opts.SideEffect)
	}
	// The side effect runs before the backup mirror and the deletes: a failure aborts
	// with nothing removed and the backup untouched.
	if err := core.RunSideEffect(opts.SideEffect); err != nil {
		return 0, err
	}
	// Mirror the exact removed items to the backup before deleting them.
	if p.backup != nil {
		if err := p.backup.Del(ctx, items); err != nil {
			return 0, core.WrapBackup(err)
		}
	}
	err = p.db.Update(func(tx *bolt.Tx) error {
		if p.Hooks.FaultAfterBackup != nil {
			if e := p.Hooks.FaultAfterBackup(); e != nil {
				return e
			}
		}
		b := tx.Bucket(itemsBucket)
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// The backup was already mirrored but the database delete failed (or did not
		// durably commit), so the items are still on disk. Restore them to the backup
		// to keep it a true mirror, then report the failure.
		if p.backup != nil {
			if rerr := p.backup.Restore(ctx, items); rerr != nil {
				// The delete did not land, so nothing was removed however the restore went.
				return 0, errors.Join(err, core.WrapBackup(rerr))
			}
		}
		return 0, err
	}
	removed := len(keys)
	deleted := keys
	if p.idx != nil {
		for i, sk := range deleted {
			p.idx.remove(khash[i], sk)
		}
	}
	p.count -= int64(removed)
	// Freed capacity: gated on HasWaiters; see Pop.
	if p.notFull.HasWaiters() {
		p.notFull.Signal()
	}
	return removed, nil
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
		if p.count > 0 {
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
		// Count the staging buffer the same way Push does. Items that have been admitted but not
		// yet committed still occupy the bound, so testing p.count alone reports room that the
		// very next Push refuses — NotFull would return and the Push behind it would block.
		if p.maxSize == 0 || p.count+p.inflight < int64(p.maxSize) {
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
	return p.count
}

// ClosedOrCause implements core.ClosedOrCauser. It returns core.ErrClosed if the backing has been closed, else the ctx cause.
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
	// The side effect runs before the close takes effect; a failure aborts the close and
	// the backing stays open. An already-closed backing still runs it, and still goes on to
	// shutdown below: closed only means "no more operations", and the flusher sets it too when
	// caller code ends its goroutine, from where it can release nothing. That call is then the
	// one that owes the shutdown.
	if err := core.RunSideEffectLocked(opts.SideEffect, p.lk.Unlock); err != nil {
		p.lk.Unlock()
		return err
	}
	p.closed = true
	p.lk.Unlock()

	// Wake anyone parked immediately, not after the shutdown: they only need to see closed, and
	// the shutdown below can legitimately take up to the join deadline. Repeated in a defer as
	// well, so a shutdown that unwinds still leaves nobody parked on a queue that has closed.
	p.notEmpty.Signal()
	p.notFull.Signal()
	defer func() {
		p.notEmpty.Signal()
		p.notFull.Signal()
	}()
	return p.shutdown(ctx)
}

// shutdown stops the flusher and releases what the backing owns: the caller's Backup and the bolt
// handle. One caller performs it; the rest wait for that caller and receive the same answer, because
// a caller told Close succeeded has to be able to rely on the file lock being gone.
//
// Three constraints shape this, each of them learned from a version that violated it:
//
// No caller-supplied code runs while a lock is held. Backup.Close can call back into this queue,
// and when the release ran inside a sync.Once every other Close was parked on that Once's mutex —
// one caller's mistake became a queue-wide deadlock. Here a re-entrant Close finds shutBusy, waits
// on a channel with a deadline, and gets an error instead of hanging forever.
//
// Every wait that can hurt a bystander is bounded. sync.Group.Wait cannot be cancelled — its own
// doc says the context it takes has no effect — so joining the flusher through it means a Backup
// that ignores ctx hangs Close with nothing able to break the tie. The join watches flusherDone,
// which the flusher closes however it ends, against a deadline, and every caller waiting on
// another caller's shutdown waits against one too. The performer's own call into Backup.Close is
// the exception: it is the caller's code on the caller's goroutine, so it is theirs to bound, and
// moving it off that goroutine to enforce a deadline here left the queue calling their Backup
// after Close had already returned.
//
// A failed attempt leaves nothing released and can be retried. If the flusher will not stop, the
// db is deliberately NOT closed: closing it under a live flusher destroys an accepted batch, which
// is worse than reporting that shutdown could not finish. The caller gets that as an error rather
// than as silence, and a later Close tries again.
func (p *Backing[T]) shutdown(ctx context.Context) (err error) {
	p.shutMu.Lock()
	if p.shutComplete {
		// Copied under the lock. Reading them after unlocking is safe only by an argument about
		// terminal state living somewhere else, and that is one retry path away from being wrong.
		g, b, c := p.shutGErr, p.shutBErr, p.shutCErr
		p.shutMu.Unlock()
		return errors.Join(g, b, c)
	}
	if p.shutBusy {
		// Someone else is releasing. Wait for them holding nothing — this is also the path a
		// re-entrant Close from inside the caller's own Backup.Close lands on, and it must come
		// back with an error rather than park forever.
		done := p.shutDone
		p.shutMu.Unlock()
		timer := time.NewTimer(p.joinBudget())
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			// Distinguish the two reasons for giving up. Once the flusher has been joined the
			// release is committed and no retry will ever re-run it, so handing back the retry
			// sentinel would tell the caller to do something that cannot happen.
			p.shutMu.Lock()
			committed := p.shutCommitted
			p.shutMu.Unlock()
			if committed {
				return core.ErrShutdownInProgress
			}
			return core.ErrShutdownIncomplete
		}
		p.shutMu.Lock()
		g, b, c, complete := p.shutGErr, p.shutBErr, p.shutCErr, p.shutComplete
		p.shutMu.Unlock()
		if !complete {
			return core.ErrShutdownIncomplete
		}
		return errors.Join(g, b, c)
	}
	p.shutBusy = true
	done := p.shutDone
	p.shutMu.Unlock()

	var gErr, bErr, cErr error
	complete := false
	// All the bookkeeping lives in a defer, because the caller's Backup.Close can end this frame
	// without returning. Doing it inline left shutBusy set and done never closed when it did, so
	// every later Close on that queue waited out the deadline for a release that was never coming
	// — the same abnormal-exit hole this rewrite exists to close, reintroduced one level up.
	defer func() {
		p.shutMu.Lock()
		p.shutGErr, p.shutBErr, p.shutCErr = gErr, bErr, cErr
		p.shutComplete = complete
		p.shutBusy = false
		if !complete {
			// Nothing was released; let a later Close try again on a fresh channel.
			p.shutDone = make(chan struct{})
		}
		p.shutMu.Unlock()
		close(done)
		// Assigned here, not at the return statement: cErr is set by the db.Close defer that runs
		// after the return expression is evaluated, so returning the join directly handed the
		// performer a nil while every waiter got the real error — the exact inverse of the
		// everyone-gets-the-same-answer contract this design exists for.
		if complete {
			err = errors.Join(gErr, bErr, cErr)
		}
	}()

	p.flushCancel()
	if p.flusherStarted.Load() {
		select {
		case <-p.flusherDone:
		case <-time.After(p.joinBudget()):
			// Refuse to release under a live flusher: it is still committing, and closing the db
			// beneath it destroys a batch the queue already accepted. complete stays false, so a
			// later Close retries rather than inheriting a shutdown that never happened.
			return core.ErrShutdownIncomplete
		}
		gErr = p.flushGroup.Wait(ctx)
	}

	// Past the join this attempt is committed: the handle is released by the defer below whatever
	// the caller's Backup does, so a retry must not run any of it again. Published so a waiter
	// that gives up can say "still releasing" rather than "nothing happened, try again".
	complete = true
	p.shutMu.Lock()
	p.shutCommitted = true
	p.shutMu.Unlock()
	defer func() {
		if e := p.closeDB(); cErr == nil {
			cErr = e
		}
	}()
	if p.backup != nil {
		// Deliberately on this goroutine and deliberately unbounded. Bounding it meant running the
		// caller's Backup.Close on a goroutine of ours, and when the bound expired that goroutine
		// outlived Close and went on touching the caller's Backup after Close had returned — a
		// worse contract than the wait it removed, and a data race against any caller that
		// reasonably assumes Close is done with their Backup.
		//
		// The queue-wide harm an unbounded wait would do is already handled elsewhere: every OTHER
		// caller waits on shutDone against a deadline, so a Backup.Close that never returns costs
		// its own caller and nobody else.
		//
		// Pre-set, then overwritten by the call's own result. A Backup.Close that ends this frame
		// without returning — a panic, or a runtime.Goexit — never reaches the assignment, and the
		// deferred bookkeeping above publishes whatever bErr holds to every later Close. Leaving it
		// zero told those callers the shutdown had succeeded with the Backup never closed, which is
		// the "told it succeeded" failure this whole design exists to end.
		bErr = fmt.Errorf("%w: Backup.Close did not return", core.ErrBackupFailed)
		bErr = core.WrapBackup(p.backup.Close(ctx))
	}
	return errors.Join(gErr, bErr, cErr)
}

// closeDB releases the bolt handle, through the hooks.dbClose seam when one is installed.
//
// A failure is tagged core.ErrShutdownIncomplete rather than handed back as the bare bolt error it is.
// This is the one release failure the caller can do nothing about — it reaches them joined with
// whatever else the shutdown reported, with complete and shutComplete already true, so no later
// Close retries it — and without the tag there is nothing in the error to tell "the file lock is
// gone" from "the file lock may still be held by this process". The underlying error stays
// reachable through errors.Is/As.
func (p *Backing[T]) closeDB() error {
	var err error
	if p.Hooks.DBClose != nil {
		err = p.Hooks.DBClose(p.db.Close)
	} else {
		err = p.db.Close()
	}
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: releasing the bolt handle failed, the store's file lock may still be held: %w", core.ErrShutdownIncomplete, err)
}

// Clear implements Backing.Clear(). It is routed through the single-threaded flusher
// so it serializes with all in-flight commits: any item whose Push has buffered or is
// mid-commit is drained first (its Push returns success and its items are briefly in
// the queue) and only then is the bucket deleted. No in-flight commit can land items
// after the delete.
//
// Context cancellation is honored only until the command is accepted: like a buffered
// Push, once the flusher has the command Clear waits for its result, so the caller
// never observes an error while the clear (and its side effect) happens after the
// call. doClear itself aborts if the caller's ctx is already canceled when it starts,
// before the side effect runs.
func (p *Backing[T]) Clear(ctx context.Context, options ...core.OpOption) error {
	opts, err := core.ResolveOpOptions(core.CallClear, options)
	if err != nil {
		return err
	}
	cmd := &clearCmd{ctx: ctx, sideEffect: opts.SideEffect, done: make(chan error, 1)}
	select {
	case p.clearReq <- cmd:
	case <-p.flushCtx.Done():
		return core.ErrClosed
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	select {
	case err := <-cmd.done:
		return err
	case <-p.flushCtx.Done():
		// The flusher exited (Close). It may have completed cmd concurrently; prefer
		// its result over core.ErrClosed so a clear that actually happened is reported.
		select {
		case err := <-cmd.done:
			return err
		default:
		}
		return core.ErrClosed
	}
}

// closeFromFlusher shuts the backing down from inside the flusher goroutine, for the case where
// caller code has ended that goroutine and it is not coming back. Without this the failure is
// silent and total: Clear waits on a result nobody will send, and every later Push buffers into a
// staging area nothing will ever commit and then blocks on a flush result that never arrives.
// Marking the backing closed makes each of those stop at its own closed check and return core.ErrClosed
// instead. A Push already waiting on an in-flight batch at that moment still blocks, which is the
// one case this cannot reach.
// cancel says whether to cancel the flush context here. That context is Clear's "the flusher is
// gone, stop waiting" signal, so cancelling it while an unwind is still on its way to deliver a
// result races that delivery and Clear reports core.ErrClosed instead of what actually went wrong.
// Whoever will deliver the result cancels; everyone else passes false.
// cause is what to report to the batch buffered against the current flushResult. It did not fail
// on its own merits — the flusher died under it — so naming a sentinel it cannot justify told
// every collateral pusher the Backup had failed, on queues that had no Backup at all.
func (p *Backing[T]) closeFromFlusher(cancel bool, cause error) {
	p.lk.Lock()
	p.closed = true
	// Anything buffered against the current flushResult will never be committed — this
	// goroutine was the only thing that could have committed it, and a Push that buffered
	// while the doomed batch was in flight is already waiting on it. Settle that batch here
	// or those pushers block forever on a done channel nobody is left to close.
	pending := int64(len(p.buf))
	p.buf = nil
	p.inflight -= pending
	r := p.cur
	p.cur = &flushResult{done: make(chan struct{})}
	p.lk.Unlock()

	r.err = fmt.Errorf("the flusher ended before this batch was committed: %w", cause)
	close(r.done)
	p.notFull.Signal()
	p.notEmpty.Signal()
	if cancel {
		p.flushCancel()
	}
}

// releaseInflight gives back capacity reserved by a batch that will never be committed and wakes a
// producer parked on it.
func (p *Backing[T]) releaseInflight(n int64) {
	p.lk.Lock()
	p.inflight -= n
	p.lk.Unlock()
	if p.notFull.HasWaiters() {
		p.notFull.Signal()
	}
}

// codeSubject names the body of caller-supplied code an operation is currently running, so an
// unwind out of it is reported against the right one and with the right sentinel. The zero value
// means no caller code is running, which is its own answer: an unwind then came from the queue.
type codeSubject struct {
	what     string
	where    string
	sentinel error
}

// setSideEffect points the blame at the side effect, or clears it when there is none — a caller
// who passed no hook must never be told one failed.
func (c *codeSubject) setSideEffect(sideEffect func() error) {
	if sideEffect == nil {
		*c = codeSubject{}
		return
	}
	*c = codeSubject{what: "the side effect", sentinel: core.ErrSideEffectFailed}
}

// setBackup points the blame at a Backup method, or clears it when there is no Backup. Callers
// clear it again once that method returns, so work the queue does afterwards is not billed to it.
func (c *codeSubject) setBackup(backup any, what string) {
	where := c.where
	if backup == nil {
		*c = codeSubject{where: where}
		return
	}
	*c = codeSubject{what: what, where: where, sentinel: core.ErrBackupFailed}
}

// setCodec points the blame at the item codec. where is preserved so a subject set during a
// particular phase — the pre-clear drain, say — keeps saying so.
func (c *codeSubject) setCodec() {
	*c = codeSubject{what: "the item codec", where: c.where, sentinel: core.ErrCodecFailed}
}

// setItem points the blame at the caller's Item methods. They are caller code nobody opts into: the
// Item constraint requires them, so they are the one body of it a queue with no Backup, no codec and
// no side effect still runs. The two this goroutine reaches are Hash (for WithIndex) and Priority
// (through the priority key); Less and Equal never run here. where is preserved for the same reason
// setCodec preserves it.
func (c *codeSubject) setItem() {
	*c = codeSubject{what: "the Item's method", where: c.where, sentinel: core.ErrItemFailed}
}

// clear says no caller-supplied code is running: an unwind now came from the queue itself.
func (c *codeSubject) clear() {
	*c = codeSubject{where: c.where}
}

// runClear runs doClear and reports its result, and is what keeps a Clear caller's WithSideEffect
// func from taking more than its own operation down with it. doClear invokes that caller code on
// the flusher goroutine, which is not the caller's to lose: an escaping panic leaves flushLoop,
// enters the shared worker pool that ran it, and ends the process; a runtime.Goexit ends the
// flusher outright, after which Clear waits on a result nobody will send and no later Push ever
// commits — silently, with no error anywhere.
//
// A panic is recovered and returned to the Clear caller, leaving the flusher running. A Goexit
// cannot be stopped, so the flusher is shut down deliberately instead: the caller gets an error and
// every later operation fails fast with core.ErrClosed rather than blocking forever on a flusher that is
// not coming back. A Push already waiting on an in-flight batch at that moment still blocks, which
// is the one case this cannot reach.
func (p *Backing[T]) runClear(cmd *clearCmd) {
	sent := false
	// doClear drains the buffered pushes before it does anything else, and that drain runs the
	// Backup and the item codec. So an unwind reaching here did not necessarily come from the
	// Clear side effect — it may have come from caller code that ran before the side effect was
	// even considered, and blaming the wrong one sends the caller looking in the wrong place.
	p.inClear = true
	defer func() { p.inClear = false }()
	// doClear runs several different bodies of caller-supplied code in sequence — the Backup, the
	// item codec and the Item's own methods during the pre-clear drain, then the Clear side
	// effect, then Backup.Clear — and an unwind can come out of any of them. A latch saying "the side effect has run by now" is
	// not the same question as "the side effect is what ended the frame", and answering the first
	// blamed a side effect that had already returned, or that the caller never passed at all.
	// doClear updates this immediately before it hands control to each one.
	// Seeded with the phase only. Which body of caller code runs during the drain is not knowable
	// here — commit sets it as it hands control over — and naming the Backup up front reported a
	// Backup failure for a codec that died on a queue with no Backup.
	blamed := codeSubject{where: " during the pre-clear drain"}
	blame := func(how string) error {
		if blamed.what == "" {
			// No caller code was running, so this came from the queue itself.
			return fmt.Errorf("the queue %s while clearing", how)
		}
		return fmt.Errorf("%w: %s %s%s", blamed.sentinel, blamed.what, how, blamed.where)
	}
	defer func() {
		if sent {
			return
		}
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				cmd.done <- fmt.Errorf("%w: %w", blame("panicked"), e)
			} else {
				cmd.done <- fmt.Errorf("%w: %v", blame("panicked"), r)
			}
			return
		}
		// runtime.Goexit: this goroutine is ending whatever we do, so make that visible.
		err := blame("ended the flusher goroutine")
		cmd.done <- err
		p.closeFromFlusher(true, err)
	}()
	cmd.done <- p.doClear(cmd.ctx, cmd.sideEffect, &blamed)
	sent = true
}

// doClear runs only in the flusher goroutine. It first drains any buffered items
// (so their Pushes return success and the items are reflected in p.count), then
// deletes the bucket under p.lk. Because it runs single-threaded with all commit()s,
// no concurrent commit can land items after the delete.
//
// The drain step uses the flusher lifetime ctx (p.flushCtx), not the Clear caller's
// ctx: the items being committed belong to other callers' Pushes, and a Clear
// caller's ctx cancellation must not fail them via backup.Push. The Clear-specific
// operations below (backup.Clear) do use the caller's ctx — that one the caller owns.
func (p *Backing[T]) doClear(ctx context.Context, sideEffect func() error, blamed *codeSubject) error {
	p.doFlush(p.flushCtx, blamed)

	p.lk.Lock()
	defer p.lk.Unlock()
	if p.closed {
		return core.ErrClosed
	}
	// The caller is still waiting on cmd.done, but if its ctx was canceled while the
	// command sat in the queue, abort before the side effect so nothing happens.
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if p.count == 0 {
		blamed.setSideEffect(sideEffect)
		return core.RunSideEffect(sideEffect)
	}
	// The side effect runs before the backup clear and the bucket delete, so a failure
	// aborts with nothing removed and the backup untouched.
	blamed.setSideEffect(sideEffect)
	err := core.RunSideEffect(sideEffect)
	blamed.setBackup(p.backup, "the Backup's Clear")
	if err != nil {
		return err
	}
	if p.backup != nil {
		if err := p.backup.Clear(ctx); err != nil {
			return core.WrapBackup(err)
		}
	}
	// The drain is over; the phase suffix goes with it.
	blamed.where = ""
	// Past every body of caller code: what follows is the queue's own work, and an unwind there
	// must not be billed to a Backup that has already returned or was never configured.
	*blamed = codeSubject{}
	err = p.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(itemsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(itemsBucket)
		return err
	})
	if err != nil {
		return err
	}
	p.count = 0
	if p.idx != nil {
		p.idx = newIndex()
	}
	// Freed capacity: gated on HasWaiters; see Pop.
	if p.notFull.HasWaiters() {
		p.notFull.Signal()
	}
	return nil
}

// All implements Backing.All().
func (p *Backing[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		p.lk.RLock()
		defer p.lk.RUnlock()
		var zero T
		if p.closed {
			yield(zero, core.ErrClosed)
			return
		}
		p.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(itemsBucket).Cursor()
			for k, data := c.First(); k != nil; k, data = c.Next() {
				select {
				case <-ctx.Done():
					yield(zero, context.Cause(ctx))
					return errStopIter
				default:
				}
				v, err := p.decodeItem(data)
				if err != nil {
					yield(zero, err)
					return errStopIter
				}
				if !yield(v, nil) {
					return errStopIter
				}
			}
			return nil
		})
	}
}

// AllCOW implements Backing.AllCOW().
func (p *Backing[T]) AllCOW(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		p.lk.CowEnter()
		defer p.lk.CowExit()
		p.lk.RLock()
		// The read lock is released on each exit below, which a panic out of the caller's iterator
		// body — or out of the caller's codec — skips entirely, leaking it for the life of the
		// process and blocking every later writer. The flag keeps the explicit releases (they
		// must happen before yielding unlocked) while the defer covers the unwind.
		held := true
		release := func() {
			if held {
				held = false
				p.lk.RUnlock()
			}
		}
		defer release()
		if p.closed {
			release()
			yield(zero, core.ErrClosed)
			return
		}
		// snap holds the raw encoded remainder, copied once a writer is waiting. We
		// store undecoded bytes so the read lock (which excludes that writer) is held
		// only for the scan, not the decode+consume phase that follows — honoring the
		// copy-on-write contract: a waiting writer proceeds during iteration.
		var snap [][]byte
		contended := false
		stop := false
		// rerr carries a decode failure or a cancellation out of the View callback instead of
		// yielding from inside it: the callback runs under the read lock, and an error yield must
		// reach the caller's loop body with no lock held, like every other error exit here.
		var rerr error
		p.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(itemsBucket).Cursor()
			for k, data := c.First(); k != nil; k, data = c.Next() {
				if contended || p.lk.WriteWanted() {
					contended = true
					snap = append(snap, append([]byte(nil), data...))
					continue
				}
				v, err := p.decodeItem(data)
				if err != nil {
					rerr = err
					return errStopIter
				}
				select {
				case <-ctx.Done():
					rerr = context.Cause(ctx)
					return errStopIter
				default:
				}
				if !yield(v, nil) {
					stop = true
					return errStopIter
				}
			}
			return nil
		})
		release()
		if rerr != nil {
			yield(zero, rerr)
			return
		}
		if stop {
			return
		}
		for _, data := range snap {
			v, err := p.decodeItem(data)
			if err != nil {
				yield(zero, err)
				return
			}
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
func (p *Backing[T]) NotFullSignal() *core.Signal { return p.notFull }

// NotEmptySignal implements core.Signals.
func (p *Backing[T]) NotEmptySignal() *core.Signal { return p.notEmpty }

// SetDrainBudget and SetJoinSlack override this backing's shutdown budgets. The production values
// are minutes-scale on purpose, which is longer than a test asserting a bound can wait.
//
// They are per-instance rather than package globals for the same reason Hooks is: the flusher reads
// the drain on its way out, and t.Context() is cancelled just before a test's cleanups run, so a
// global restored in a cleanup races the flusher of every queue any test left running. Call them
// before anything can cancel the flush context — right after construction — since that
// cancellation is what orders the write ahead of the flusher's read.
func (p *Backing[T]) SetDrainBudget(d time.Duration) { p.drain = d }

// SetJoinSlack sets what the join gets on top of the drain it is waiting for. See SetDrainBudget.
func (p *Backing[T]) SetJoinSlack(d time.Duration) { p.slack = d }

// DrainBudgetFor reports the drain budget, so a test can check its shortening took effect.
func (p *Backing[T]) DrainBudgetFor() time.Duration { return p.drainBudget() }

// JoinBudgetFor reports the join budget, so a test can check its shortening took effect and that
// it still contains the drain budget.
func (p *Backing[T]) JoinBudgetFor() time.Duration { return p.joinBudget() }

// FlushCtx is the flusher's lifetime context, which Close cancels. A test watches it to know the
// flusher has been told to stop.
func (p *Backing[T]) FlushCtx() context.Context { return p.flushCtx }

// FlusherDone is closed when the flush goroutine returns, however it returns. A test watches it to
// know the flusher is gone rather than merely asked to stop.
func (p *Backing[T]) FlusherDone() <-chan struct{} { return p.flusherDone }
