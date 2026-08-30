# `queue` — design and internals

Reference for the whole package: what each piece is, what it guarantees, and why it is built the
way it is. Written to survive the deletion of the in-code commentary, so anything load-bearing that
lived in a comment lives here.

`README.md` is the user-facing guide. This is the implementer's document.

---

## 1. Package map

The backings live in their own packages under `internal/backings`. `core` holds the vocabulary
they share; each backing package holds one implementation and nothing else.

| Package / file | Contents |
|---|---|
| `queue/doc.go` | Package doc: the six constructors and when to use each. |
| `queue/queue.go` | `Queue`, `New`, the operation methods, `RangeAll`/`RangeAllCOW`. |
| `queue/alias.go` | The public face: the type aliases, the sentinel re-exports, and the six constructor forwarders. |
| `queue/builtin.go` | `Number`, `String`, `Bytes`, `Value`, `JSONEncode`/`JSONDecode`, the `diskCodecRequirer` marker. |
| `queue/backing_options.go` | Every `With*` construction option. |
| `queue/metrics.go` | OTEL instruments, `instrument`, `errorKind`, `recordDepth`. |
| `internal/backings/core/item.go` | `Item`, `Backup`, `Backing`, `Seal`, `Signals`, `ClosedOrCauser`, `Unlimited`. |
| `internal/backings/core/errors.go` | The sentinel family, `permanent`, `core.WrapBackup`, `core.WrapCodec`. |
| `internal/backings/core/qlock.go` | `core.QLock`. |
| `internal/backings/core/signal.go` | `core.Signal`, the Mesa-style broadcast used for `notFull`/`notEmpty`. |
| `internal/backings/core/opoptions.go` | `core.OpCall`, `core.OpOptions`, `OpOption`, `core.ResolveOpOptions`. |
| `internal/backings/core/backingopts.go` | `core.BackingCall`, `core.BackingOpts`, `BackingOption`, `core.ApplyBackingOptions`. |
| `internal/backings/core/run.go` | The abnormal-control-flow helpers and `core.RunOnAdmit`. |
| `internal/backings/core/validate.go` | `core.ValidateKind`, `core.ValidateKindOne`, `core.MatchesAny`. |
| `internal/backings/core/seq.go` | `core.SeqItem`, `core.FifoSeqLess`, `core.PrioritySeqLess`. |
| `internal/backings/fifo/` | `fifo.Backing` — slice FIFO. |
| `internal/backings/btype/` | `btype.Backing` — positional COW B-tree FIFO, plus the vendored tree. |
| `internal/backings/heap/` | `heap.Backing` and `minHeap` (+ move journal). |
| `internal/backings/btree/` | `btree.Backing` (keyed tidwall/btree, FIFO and priority) and its hash index. |
| `internal/backings/bbolt/` | `bbolt.Backing` — on-disk, group-commit flusher, shutdown state machine. |
| `internal/parker/` | `gopark`/`goready` broadcast primitive underneath `core.Signal`. |

### Why the dependency runs this way

`queue` imports the backings, because `queue.NewBboltFIFO` has to reach `bbolt.NewFIFO`. Go has no
function aliases and these constructors are generic, so a forwarding body is the only mechanism —
which means **no backing package may import `queue`**. Everything that appears in a `Backing`
method signature therefore had to move down into `core` and come back out of `queue` as a
**generic type alias**: `Item`, `Backup`, `Backing`, `OpOption`, `BackingOption`. A structurally
identical copy would not do, because Go type identity is by declaration — a backing implementing
`Hydrate(ctx, core.Backup[T])` would not satisfy an interface asking for `queue.Backup[T]`.

What deliberately stayed in `queue`: the `With*` option constructors (they only *build* a
`core.BackingOpts`, so their godoc is unaffected), `builtin.go` (the `diskCodecRequirer` check runs
in the `NewBbolt*` forwarders, so `Number`/`String`/`Bytes`/`Value` keep intact godoc), and
`Queue`/`metrics.go`, which no backing references.

The cost is that `queue.Item` and `queue.Backup` are aliases into an internal package, so pkgsite
renders the alias but not the target's method set. Their full contracts are written as prose on the
alias declarations in `alias.go` to compensate.

### The seal

`Backing` still cannot be implemented from outside. `core` exports a `Seal` struct carrying an
unexported `private()` method; each backing embeds it, which promotes `private()` into that type's
method set. Because `core` is under `internal/`, no code outside this module can embed `Seal`, so
nothing outside can satisfy `Backing`. This is the same mechanism as gRPC's
`mustEmbedUnimplementedFooServer`. The three setters carry per-backing behavior and so are
ordinary exported methods: `SetQueueLock`, `SetMaxBatch`, `SetMaxSize`.

---

## 2. The three-layer shape

```
Queue[T]  ── telemetry, batch limit, empty-batch fast path, RangeAll wrappers
   │        holds *core.QLock, Backing[T], Backup[T]
   ▼
Backing[T] ── the data structure + all blocking, locking, mirroring and unwind policy
   │          (sealed interface; six constructors, five implementations)
   ▼
storage    ── slice / positional B-tree / container.heap / tidwall.BTreeG / bbolt
```

Each arrow is now also a package boundary: `queue` → `internal/backings/<impl>` → `core`. A backing
can reach only what `core` exports, which is what turns the layering below from a convention into
something the compiler checks.

`Queue` is thin on purpose. Every semantic decision — when to block, when to mirror to the
`Backup`, what the lock protects, how an unwind is contained — lives in the backing. That is why
the backings look repetitive: the repetition is the contract, and each copy differs in the one
place its data structure differs.

### Ordering convention

**A higher `Item.Priority()` is more desirable and is dequeued sooner.** That is the rule for the
whole package, and it is stated here because the code enforces it in two independent places that
are easy to change apart from one another:

- **`Item.Less`.** The in-memory backings (btree, heap) never consult `Priority()` at all — they
  order through `core.PrioritySeqLess`, which is `Item.Less` with insert sequence as a tiebreak. The
  built-in item types encode the convention here, as `u.P > other.P`.
- **`bbolt.priorityKey`.** The on-disk backing is the only one that reads `Priority()` directly. It
  stores the **complement** of the priority, because bbolt walks keys in ascending byte order, so
  complementing is what puts the highest priority at the head. The sequence half is stored as-is,
  keeping ties FIFO.

An implementation must keep the two order-consistent: if `a.Less(b)` then `a.Priority() > b.Priority()`.
A type that flips one without the other sorts one way in memory and the other way on disk.

That clause binds only items destined for a **priority** queue. A FIFO item must report
`Priority() == 0`, which makes the implication unsatisfiable for any `Less` that orders two items
strictly — and harmless, because no FIFO backing calls `Less`: `btree.NewFIFO` uses
`core.FifoSeqLess` (insert sequence only) and `bbolt`'s `fifoKey` discards the item entirely. A FIFO
item may implement `Less` however it likes.

`Priority() == 0` is reserved as the queue-kind gate — a priority queue rejects it
(`ErrPriorityRequired`), a FIFO queue requires it (`ErrPriorityNotAllowed`) — so it is not usable as
a "lowest priority" value. The lowest a priority queue accepts is `1`.

### Construction order in `New`

1. Reject a nil backing.
2. `queueOptions{}.defaults()` **then** apply options. Defaults first is deliberate — applied
   after, the code cannot tell "caller passed nothing" from "caller passed 0", which silently
   promoted `WithMaxBatch(0)` to 1000 while correctly rejecting `-1`.
3. `validateOptions` — `maxBatch >= 1`, and the `any` backup type-asserts to `Backup[T]`.
4. `maxSize < 1` normalises to `0` (unbounded).
5. `lk := &core.QLock{}` → `b.SetQueueLock(lk)` → `SetMaxBatch` → `SetMaxSize`.
   **`SetQueueLock` is what starts bbolt's flusher goroutine**, so it must come before anything
   the flusher could observe; it is also why the placeholder `core.QLock` each backing constructor
   creates for itself is discarded here rather than reused.
6. Build `Queue`; build metrics only when `name != ""`.
7. If a `Backup` was given, `Hydrate` (see §7).
8. `recordDepth` to seed the depth counter from hydrated items.

---

## 3. `core.QLock` — the shared lock

```go
type qlock struct {
    mu        sync.RWMutex
    pending   atomic.Int32   // writers currently blocked on the write lock
    cowActive atomic.Int32   // COW iterators currently running
}
```

One lock is shared by the `Queue` and its backing, injected by `New`. That is what lets
`RangeAll` hold the read lock across an iteration that walks the backing's own data.

`Lock()` bumps `pending` around the acquire **only when `cowActive > 0`**. In the steady state
(no COW iterator running) nobody reads `writeWanted`, so the two atomics are skipped entirely.

`WriteWanted()` is the copy-on-write trigger. Its correctness rests on one fact: while a COW
iterator holds the read lock, a blocked writer *cannot* acquire, so `pending` stays > 0 until the
reader releases. The signal cannot be missed. It is only meaningful inside a `cowEnter()` /
`cowExit()` bracket; outside one, `pending` is not maintained.

---

## 4. `core.Signal` — the wait primitive

A Mesa-style broadcast over `internal/parker` (`runtime.gopark`/`goready` via linkname). No
channel allocation per wait; `Signal` is O(parked waiters).

```go
func (s *signal) Wait(ctx context.Context, unlock func()) error
```

**The Mesa invariant is the whole point.** `Wait` registers the parker *under `s.mu`*, and only
then calls `Unlock()` to release the caller's queue lock. Registering after the release would open
the classic lost-wakeup window where a `Signal` between "released the queue lock" and "entered
Wait" sees `waiters == 0` and completes as a no-op. Every backing therefore passes its own
`unlock`/`runlock` into `Wait` rather than releasing first.

Two paths out:

- `ctx.Done() == nil` (e.g. `context.Background()`): skip ctx watching, park, recycle the parker.
- Otherwise `context.AfterFunc(ctx, p.WakeFunc())`. If `cancel()` returns true the callback never
  ran and the parker is recycled through the pool; otherwise it is `Detach`ed from the waiter list
  instead. Without that detach, every ctx-cancelled `Wait` would leave a stale parker behind and a
  later `Signal` would be O(stale).

`HasWaiters()` gates `Signal` at every mutation site, so the common case (nobody parked) stays
allocation-free. `Close` broadcasts **unconditionally** — that asymmetry is relied on in several
places and is called out where it matters.

---

## 5. Abnormal control flow

Every queue operation runs caller-supplied code while holding a lock. Five bodies of it:

| Body | Opted into? |
|---|---|
| `Backup` methods | via `WithBackup` |
| item codec (`WithCodec`, or default JSON) | bbolt only |
| `WithSideEffect` func | per-operation |
| `WithOnAdmit` hook | per-Push |
| `Item.Less` / `Equal` / `Priority` / `Hash` | **never** — the `Item` constraint requires them |

Any of these can end the frame without returning: a `panic`, or `runtime.Goexit`. Most backings
release their locks with explicit statements (not `defer`) on each return path, so an unwind
carries the lock away and wedges the queue permanently — no error, no race report, every later
operation blocked forever.

### The completion-flag idiom

```go
func runLocked(fn func() error, release func()) (err error) {
    returned := false
    defer func() { if !returned { release() } }()
    err = fn()
    returned = true
    return err
}
```

**Deciding on a flag rather than on `recover()` is mandatory**: `recover()` returns nil during a
`runtime.Goexit`, so a guard written around it covers only half the abnormal exits. The panic
keeps unwinding with its original stack — this only guarantees it does not take the lock with it.

Variants:

- `runLockedNoErr(fn func(), release func())` — same, for caller code that returns no error. The
  four `Item` methods are reached *indirectly* as often as directly (through a heap comparator, a
  sort's less func, an index's add/remove), so the guarded call frequently names no `Item` method
  at all.
- `runSideEffectLocked(se, release)` — `core.RunLocked` + `ErrSideEffectFailed` tagging.
- `runBackup(fn, release)` — `core.RunLocked` + `ErrBackupFailed` tagging.
- `runSideEffect(se)` / plain — for operations that unlock through a `defer`; passing a release
  there would unlock twice.

**`release` must undo everything the return paths would have undone**, not merely unlock. On Push
that includes handing back a `WithOnAdmit` reservation *and* signalling `notFull`. On the priority
heap's Push and Pop it includes rolling the heap back. Getting this wrong was the source of three
separate reproduced defects: the guard released the lock but left the structure half-mutated.

**`release` must not itself call caller code.** It runs during an unwind out of exactly that code;
failing again there takes the process down with the lock still held.

Two non-lock uses of the same mechanism, because nothing about the flag is lock-specific:

- `New` wraps `Hydrate`, so an unwind out of a `Backup` still closes the backing and the Backup.
- `newBboltBacking` wraps `rebuildIndex`, so an unwind out of `Item.Hash` still releases the bolt
  handle — the file lock is process-wide and would otherwise be held for the life of the process
  with no queue in existence to `Close`.

---

## 6. Error taxonomy

### `permanent()`

```go
func permanent(msg string) error { return fmt.Errorf("%s: %w", msg, exponential.ErrPermanent) }
```

A caller wrapping a queue operation in `base/retry/exponential` must stop on a programmer error
instead of burning the retry budget. It is a *wrapper*, not a joined second sentinel, because the
exported value has to be the permanent one — `errors.Is(err, ErrClosed)` matches exactly the value
the package exports, and marking it anywhere else would leave
`errors.Is(err, exponential.ErrPermanent)` false for every error actually returned.

**Permanent:** `ErrClosed`, `ErrBadOption` (and the five option sentinels built on it),
`ErrBatchTooLarge`, `ErrPriorityRequired`, `ErrPriorityNotAllowed`, `ErrShutdownInProgress`.

**Deliberately not permanent:** `ErrEmpty` (a retry clears it) and `ErrShutdownIncomplete` (two of
its three causes are exactly what a retry is for — see §12.4).

### The blame sentinels

Five tags say **which body of caller code failed**, so the queue never takes the blame for the
caller's code and vice versa:

`ErrBackupFailed` · `ErrCodecFailed` · `ErrSideEffectFailed` · `ErrOnAdmitFailed` · `ErrItemFailed`

Tags are *additive* — the caller's own error stays reachable through `errors.Is`/`As`. A Backup
that returns `ErrClosed` still matches `errors.Is(err, ErrClosed)`; what changes is that
`ErrBackupFailed` is there to be asked about first.

**Triage order** (the same order `errorKind` uses for the metric attribute): test the five
caller-code sentinels first; only when all five are false does a queue sentinel mean the queue.

**The one case that breaks that reading.** Pop and Del mirror a removal to the Backup *before*
applying it. If the on-disk delete then fails, `Backup.Restore` compensates. If that Restore also
fails, both errors are joined — so `ErrBackupFailed` is present for a failure the **queue** caused,
and the triage order would have the caller blame their Backup. Read the whole joined error on Pop
and Del, not just the tag.

`ErrItemFailed` is special: `Item` methods return no error, so it never tags a returned error. It
carries only the two ways an `Item` method can end an operation without returning **on bbolt's
flusher goroutine** — which is not the caller's goroutine to lose. Everywhere else an `Item` unwind
continues on the caller's own goroutine with its own stack and carries no sentinel, deliberately:
converting it would mean recovering a panic the caller owns and is entitled to catch.

### `ErrBadOption`

One sentinel for every misconfiguration, because there is nothing a caller can do differently for
each — they are all "this call site is wrong, fix the code". Before it, "max batch must be at least
1" existed as six unrelated `errors.New` values across six backings: six identities for one
semantic error that no `errors.Is` could unify. The five named option sentinels
(`ErrCodecRequired`, `ErrOnAdmitUnsupported`, `ErrOnAdmitNotPush`, `ErrNilOnAdmit`,
`ErrNilSideEffect`) are built on top of it, so `errors.Is(err, ErrBadOption)` is the single
question "did I configure this wrong?" and each remains the finer answer.

---

## 7. Options

Three option types, three different shapes, each with its own reason.

| Type | Signature | Applied to | Errors? |
|---|---|---|---|
| `Option` | `func(queueOptions) queueOptions` | `New` | no (validated afterwards by `validateOptions`) |
| `OpOption` | `func(opOptions) (opOptions, error)` | every operation | yes, from the closure |
| `BackingOption` | `func(backingOpts) (backingOpts, error)` | backing constructors | yes, from the closure |

`OpOption` and `BackingOption` stamp the call site on the struct **before the first option runs**
(`opOptions.call`, `backingOpts.call`), so an option valid only on some calls rejects the rest from
inside its own closure. `core.ResolveOpOptions` and `core.ApplyBackingOptions` both `panic` on the zero
call value — not reachable from a caller (every site passes its own constant), it catches the one
mistake ~46 hand-stamped call sites invite: a new operation added without one.

A **nil func handed to an option is an error, not a no-op** (`ErrNilSideEffect`, `ErrNilOnAdmit`).
Passing one means the caller believed a hook was configured when none was, and the operation would
otherwise silently run without it.

### `WithSideEffect`

Runs **under the lock**, once the operation is otherwise guaranteed to succeed. A non-nil return
rolls the operation back. Mutators run it under the write lock; readers (`Peek`/`Exists`/
`NotEmpty`/`NotFull`) under the read lock, so concurrent readers' side effects may run
concurrently with each other.

Reentrancy: under the write lock it self-deadlocks immediately. Under the read lock a nested read
usually succeeds and deadlocks only when a writer has queued between the two acquisitions (Go's
`RWMutex` is not reentrant) — worse, because it only appears under load.

One backing does not run it on the caller's goroutine: **bbolt's `Clear`** hands it to the flusher.
A panic there is reported as `ErrSideEffectFailed` rather than reaching the caller's `recover`, and
a `runtime.Goexit` ends the flusher, which closes the queue.

### `WithOnAdmit`

Push-only. Runs after capacity is reserved and **with the lock released**, for the durable write a
caller must do between "there is room for this" and "this is queued" — which cannot go in
`WithSideEffect`, where a slow call stalls every other operation.

```go
func runOnAdmit(lk *qlock, inflight *int, n int, notFull *signal, hook func() error) error
```

Contract: caller holds the write lock on entry; `core.RunOnAdmit` holds it again on **every** return,
including a panicking hook. `inflight += n` for the duration, so the `maxSize` gate and `NotFull`
both count the reservation and concurrent Pushes cannot overshoot while nothing is inserted.

The reservation is released on every exit. On any exit that does not go on to insert — hook error,
or panic — a parked producer is signalled, because the released reservation may be exactly the
capacity it is waiting for. **The caller owes that same signal on any later exit of its own that
abandons the push**; that is what `opOptions.releaseReservation` is for, and why every abandon path
in the four in-memory Pushes calls it (including the unwind `release` closures).

Consequences the doc calls out, because they are not obvious:

- Not atomic with the Push — other operations interleave between admission and insertion.
- Insertion order is decided after the hook returns, so concurrent Pushes may be ordered
  differently than they were admitted (on a priority queue that only affects the equal-priority
  tiebreak).
- `Len()` does **not** count a reservation, so during a hook `Len()` reports fewer items than
  `NotFull` and `Push` are treating the queue as holding.
- A reentrant hook does not self-deadlock like a side effect does. It acquires the lock fine, then
  blocks forever against its own outstanding reservation — much harder to read.

**bbolt rejects it** with `ErrOnAdmitUnsupported`: it admits into a staging buffer and commits in
groups, so it has no moment meaning "capacity reserved, nothing written". Refusing beats running
the hook at a moment that does not match what it promises. The check is ordered *after* the kind,
closed and batch-size checks so bbolt reports the same error as every other backing for those.

### Construction options

`WithIndex` (keyed btree + both bbolt), `WithBTreeWidth` (keyed btree only, `>= 2`, default 32),
`WithCodec` (bbolt only), and the bbolt passthroughs (`WithNoSync`, `WithNoFreelistSync`,
`WithNoGrowSync`, `WithBoltTimeout`, `WithBoltPreLoadFreelist`, `WithBoltFreelistMap`,
`WithBoltMlock`, `WithBoltMmapFlags`, `WithBoltInitialMmapSize`, `WithBoltPageSize`,
`WithBoltOpenFile`), all guarded by `backingOpts.bboltOnly`.

`WithBTreeWidth` validates in its own closure **and** `newBTreeBacking` re-checks. The closure
covers `NewBTreeFIFO` without `WithIndex`, which returns before ever reaching the constructor; the
constructor check is the struct's own invariant, so a future constructor building `core.BackingOpts` by
hand cannot reach the tree unchecked. `core.ApplyBackingOptions` seeds `width: 32` *before* the loop —
patching the default in afterwards is what once let `WithBTreeWidth(0)` through while rejecting 1
and −1.

`WithCodec` stores its funcs as `any` (because `BackingOption` is not generic) and
`newBboltBacking` type-asserts them back.

---

## 8. Capacity accounting

Three counters interact with `maxSize`. Only bbolt differs.

### `hydrated` — the restore exemption (in-memory only)

Items loaded by `Hydrate` do **not** count against `maxSize`. A restore is not an admission: the
items already existed, so refusing them — or blocking every later Push until they drain — would
make a bounded queue unusable after a restart. A backup larger than the bound still loads and still
leaves the whole bound free.

The exemption decays as restored entries leave, so the bound takes full effect once the surplus is
gone. **Only a restored entry may reduce it.** Charging an admitted entry's removal to the surplus
understates the exemption, and an understated exemption is permanent: the queue refuses work it has
room for, forever.

Two classification strategies, because two orderings:

| Backing | Classifier | Why |
|---|---|---|
| `fifo`, `btype.Backing` | **positional** — the first `hydrated` positions | FIFO only ever appends, and removal is order-preserving, so restored entries stay at the front |
| `heap.Backing`, `btree.Backing` | **by seq** — `seq < hydrateEnd` | priority order means restored entries are not necessarily the ones that leave first |

`hydrateEnd` is set in a `defer` on **every** exit of `Hydrate`, not just the happy one: a partial
load that errored would otherwise leave `hydrated > 0` with the boundary at 0, so `seq < hydrateEnd`
is never true and the exemption never decays. Not reachable through `New` (which aborts on a
`Hydrate` error), but `Hydrate` is on the exported `Backing` interface.

Invariants: `hydrated <= Len()` always, so `Len() - hydrated` cannot go negative. Every removal
path (`items[k:]`, `DeleteAt`, `PopFront`, `PopMin`, `tree.Delete`, `heap.Pop`) has a
`dropHydrated`, and all four `Clear`s reset it to 0.

The admission gate is therefore `Len() - hydrated + inflight + len(vs) <= maxSize`, and `NotFull`'s
predicate is `Len() - hydrated + inflight < maxSize`.

### `inflight`

Two different meanings under one name:

- **In-memory backings**: items a Push reserved capacity for but has not inserted, because a
  `WithOnAdmit` hook is running unlocked.
- **bbolt**: items admitted into the staging buffer but not yet committed (buffered, or in a
  snapshot mid-commit). Guarded by `lk` like `count`. Named distinctly from `qlock.pending`, which
  counts writers blocked on the lock.

bbolt's `NotFull` tests `count + inflight < maxSize` for exactly this reason: testing `count` alone
reports room the very next Push refuses.

### bbolt has no exemption

Its `Hydrate` admits through the staging path and counts restored items normally, so restoring past
the bound blocks new pushes until the store drains. **This is the one place the backings
deliberately disagree on bounded-queue semantics**, and it is documented on `Backing.Hydrate`.

---

## 9. Iteration: `All` vs `AllCOW`

`Queue.RangeAll` → `Backing.All`. Holds the read lock for the whole iteration; writers block until
the sequence is consumed or abandoned. A mutating call from inside the loop body self-deadlocks.

`Queue.RangeAllCOW` → `Backing.AllCOW`. Holds the read lock only until a writer is waiting
(`qlock.writeWanted()`), then copies the remainder, releases, and finishes from the copy.

**Which backings are safe to mutate from inside the loop body** (because they take their whole
snapshot up front and yield with no lock held):

| Backing | `AllCOW` shape | Mutate from loop body? |
|---|---|---|
| `NewPriority` (heap) | sorts a copy under the **read** lock, releases before the first yield | ✅ |
| `NewBTreeFIFO` without `WithIndex` (btype) | O(1) `CopyInto` under the **write** lock, releases before the first yield | ✅ |
| `NewFIFO` | holds read lock, snapshots the tail on contention | ❌ |
| `NewBTreeFIFO` with `WithIndex` | ditto | ❌ |
| `NewBTreePriority` | ditto | ❌ |
| `NewBboltFIFO` / `NewBboltPriority` | ditto (snapshots **raw encoded bytes**) | ❌ |

`priorityHeap.All` is the odd one: it takes the **write** lock, not the read lock. Yielding in
priority order requires sorting, and sorting the heap array ascending in place leaves a valid
min-heap (a node's children are always at higher indices), so `Push`/`Pop` keep working with no
snapshot copy.

`btypeFIFO.AllCOW` takes the **write** lock for its snapshot because `CopyInto` flips the source's
`copied` flag — a non-atomic field — so it must exclude readers as well as writers. It is held only
for that O(1) operation.

bbolt's `AllCOW` snapshots **undecoded bytes**, so the read lock (which excludes the waiting
writer) is held only for the cursor scan, not the decode-and-consume phase.

### The error-yield rule

**Every backing releases its lock before yielding an error**, whether the error is `ErrClosed`, a
context cancellation, or a codec failure. That loop body therefore runs with no lock held and may
mutate the queue freely. It is also the last yield: the sequence ends there.

This is why `btreeBacking.AllCOW` and `bboltBacking.AllCOW` carry a cancellation out of their
`Scan`/`View` callbacks in a variable (`cerr` / `rerr`) and yield it after `release()`, rather than
yielding from inside the callback where the lock is still held.

`AllCOW`'s unwind guard is a `held bool` + `release()` closure + `defer release()`. The explicit
releases must happen *before* yielding unlocked; the defer covers a panic out of the caller's loop
body (or codec), which would otherwise leak the read lock for the life of the process.

---

## 10. Telemetry

Instruments (built once in `New`, only when `name != ""`; construction errors `panic`, matching
`base/concurrency/background`):

| Metric | Kind |
|---|---|
| `queue.operations` | Int64Counter |
| `queue.operation.errors` | Int64Counter |
| `queue.operation.duration` | Float64Histogram, seconds |
| `queue.depth` | Int64UpDownCounter |

Attributes: `queue.name`, `queue.operation`, and on errors `queue.error_kind`.

`queue.error_kind` is a **closed set of compile-time constants** — `backup`, `codec`, `side_effect`,
`on_admit`, `item`, `queue`, `unwind` — so cardinality is bounded no matter what an error says.
Nothing derived from error text ever reaches an attribute. It names which body of code failed,
which is what an operator paging on the metric actually wants: "the caller's Backup is refusing
every Push" and "the bbolt store is corrupt" were previously the same data point.

`errKindUnwind` is separate from `errKindQueue` because on that path there is no error to
classify — the operation ended without returning, and the queue did not fail.

### The `completed` flag

```go
ctx, done := q.instrument(ctx, "Push")
completed := false
defer func() { done(&err, &completed) }()
... the whole operation ...
completed = true
```

Deciding on `*errp` alone sees only one of the two ways to fail. Caller code can end the operation
without returning; the deferred call still runs with `*errp == nil`, so an operation killed by a
panicking Backup was recorded as a **success** — zero errors counted, span ended OK. Same idiom and
same reason as `core.RunLocked`: `recover()` reports nothing during a `Goexit`.

One `switch` decides both the counter and the span status, because a metric saying an operation
failed alongside a trace saying it succeeded is worse than either alone.

`Queue.Push` delegates its body to an unexported `push` so there is exactly one place to set
`completed = true`; inline, the flag would need setting on five return paths and the one a future
edit forgot would silently record success.

`recordDepth` emits a **delta** via `lastDepth.Swap(cur)`, which makes the running total
self-correcting under concurrent mutation. It carries only `queue.name` — depth is a queue-level
quantity, not a per-operation one.

---

## 11. `Hydrate`

```go
Hydrate(ctx context.Context, b Backup[T]) error   // must be called once, before any mutation
```

Loads from the `Backup`, calling `Backup.OnLoad` for each item in backing order, then **attaches**
the backup so future mutations mirror to it.

**The attach is the last statement of a successful `Hydrate`, and a failing one never does it.**
That is what makes "items loaded during hydrate do not mirror back to the backup" structural rather
than an argument about which lines happen to touch the field. It also means a backing whose
`Hydrate` failed has no `Backup` to close — so closing it is `New`'s job, not the backing's, which
is why `New`'s error path does `errors.Join(err, b.Close(ctx), wrapBackup(backup.Close(ctx)))`.
`Backing` is sealed, so those are all the implementations and nothing can double-close.

`New` wraps the whole call in `core.RunLocked` with a release that closes both, so an unwind out of
`Backup.RangeAll` / `OnLoad` / the codec / `Item.Hash` cannot leak the backing (for bbolt, the bolt
file lock, held for the life of the process with no queue in existence to `Close`) or the caller's
`Backup`, which nothing else will ever close.

### bbolt's exception

If the store is **non-empty**, it is the source of truth and nothing is loaded from the backup —
loading would duplicate. But `OnLoad` is still driven once per persisted item, in stored order, so
a caller rebuilding external state gets the same callbacks it would from an in-memory backing.

If the store is **empty**, items are loaded from the backup in batches of `hydrateBatch` (100) —
one `db.Update` per batch rather than per item.

---

## 12. The backings

### 12.1 `fifo.Backing` — slice FIFO (`queue.NewFIFO` → `fifo.New`)

Simplest of the five, and the reference implementation everything else mirrors. Use under ~10K
items.

Storage is `buf []T` plus a `head int`. The live contents are `buf[head:]`, reached through
`live()`; everything below `head` is slots Pop has already handed back. Push appends at the tail,
Pop zeroes what is leaving and advances `head`.

**Why not `buf = buf[k:]`.** That is what it used to do, and it is why this section exists. A
reslice moves the base pointer, so the space Pop frees is abandoned for good and each Pop leaves
`append` a shorter tail. Once the tail runs out `append` allocates a fresh, larger array — and
since a Pop costs one slot of spare capacity while a reallocation only buys about `len(live)` of
it, a shallow busy queue reallocated on **very nearly every Push**. That is the shape a small
bounded FIFO has under load. `head` fixes it by leaving the space addressable.

**Why the slide in Push.** `head` alone is not enough: nothing reuses the dead prefix, so `append`
extends past it and both `len(buf)` and `head` climb forever — a queue holding one item would sit
on an array proportional to everything that had ever passed through it. So Push slides the live
contents down over the dead prefix, then clears the duplicates the copy leaves above the new
length (they would otherwise pin items past their own removal, which is what the explicit zeroing
everywhere else in this backing exists to prevent).

The slide runs **only when `append` would have grown the array anyway**, so it costs nothing while
there is room and one copy instead of a reallocation when there is not. The array therefore grows
only when the live count genuinely outgrows it.

The two are separate properties and are tested separately: `head` stops the per-Push reallocation,
the slide stops the unbounded growth. Removing the slide leaves the allocation count unchanged —
the reallocations are on a doubling schedule and amortize away — which is why §16 has a test for
each.

`Exists` and `Del` are O(n) scans. `Del` builds a `kept` slice and swaps it in as the new `buf`
with `head` reset to 0, counting how many removed indices were `< hydrated` for the exemption.
Because it iterates `live()`, `i` still means "the i-th live item", which is what the positional
hydration accounting requires.

`Len()` returns `len(live())` — it includes hydrated items and excludes `inflight`.

The `min(k, f.hydrated)` in `Pop` is redundant (`dropHydrated` clamps) but harmless.

### 12.2 `btype.Backing` — positional COW B-tree (`queue.NewBTreeFIFO` without `WithIndex` → `btype.New`)

Same semantics as `fifo` over `tree[omit, T]` (`btype_btree.go`): `PushBack` / `PopFront` /
`DeleteAt` / `All`, with no comparator descent and no `core.SeqItem` wrapper. Better for large or
unbounded FIFO queues.

`Del` collects matching indices in one `All` pass, then deletes **highest index first** so earlier
indices stay valid.

`Pop` has two branches: with a `Backup`, peek the first `k` via `All` and mirror before mutating;
without one, `PopFront` directly into `out`.

Its `AllCOW` is one of the two mutation-safe ones (§9).

#### `btype_btree.go` internals

Vendored from `github.com/tidwall/btype` (MIT). Positional copy-on-write B-tree, `fanout = 64`
(`maxItems = 63`, `minItems = 31`).

- `node` holds `keys`/`values` arrays (keys **must** be the first field), an `rc` refcount, `len`,
  and a `*branch` (nil for a leaf).
- `CopyInto` is O(1): sets `t.copied = true`, shallow-copies the tree header, and bumps the root's
  `rc`. Subsequent mutation of either tree lazily copies nodes via `cowRoot`/`cowChild` → `cow0`,
  which is gated on `t.copied && rc > 0`.
- `Release` walks and decrements; `releaseNode` frees values through `dataRelease` when the count
  goes negative.
- `spare` holds one empty, exclusively-owned leaf that survived a Pop-to-empty, so a FIFO workload
  that intermittently drains does not allocate a fresh ~1KB leaf on every 0→1 cycle.
  `collapseRootIfNeeded` only populates it when the dropped root is a leaf with `rc == 0` (no
  snapshot references it), and `nodePopFront` zeroes each emptied slot so the arrays are clean for
  reuse.

The queue uses only the positional API (`PushBack`, `PopFront`, `Front`, `DeleteAt`, `All`, `Len`,
`Clear`, `CopyInto`, `Release`); the keyed/ordered half of the file is unused but kept intact
against the upstream commit.

### 12.3 `heap.Backing` — `container/heap` (`queue.NewPriority` → `heap.New`)

`minHeap[T]` holds `[]seqItem[T]` ordered by `core.PrioritySeqLess` (`Item.Less`, ties by insert seq).

#### The move journal

```go
type minHeap[T Item[T]] struct {
    items   []seqItem[T]
    swaps   []int   // index pairs, appended by Swap while journal is set
    journal bool
}
```

`container/heap` mutates the array **only** through `Swap` and through this type's own
`Push`/`Pop`. Journaling every swap and replaying it backwards therefore undoes a batch
move-for-move.

Why not just re-heapify to roll back? Because `heap.Init` runs `Item.Less`, and both rollback sites
are places where `Less` has either just failed or would be asked to fail again:

- **The unwind release.** A `Less` that unwinds during the rollback takes the frame with it, and
  nothing deferred can hand back an error from a frame being destroyed — so the `ErrBackupFailed`
  the rollback exists to report reaches nobody. The only fix is for the rollback to be *incapable*
  of failing.
- **Ordering.** Dropping the new entries instead leaves the array holding the right items in the
  wrong order — silent priority inversion on a routine `Backup.Del` error.

`rollback(origLen, popped)` replays at `max(len(items), origLen)` because a Push's journal holds
indices past `origLen` while a Pop's holds values past the current length (Pop only reslices, so
the capacity is still there). It restores popped values at `origLen-1-i`, replays swaps backwards,
zeroes the tail past `origLen` (so a rolled-back Push does not leave the caller's items reachable),
reslices to `origLen`, and turns the journal off.

A single shared journal is enough because both call sites hold the write lock: only one mutation is
ever in flight.

#### Push

`journalOn()` → guarded `heap.Push` loop → `journalOff()`. The release does
`rollback(startLen, nil)`, restores `nextSeq`, releases the reservation and unlocks. All-or-none is
the documented contract, and a `Less` unwinding on the second of three items had been leaving the
first in the heap for a Push that never returned.

#### Pop

Side effect → `journalOn()` → guarded `heap.Pop` loop building `popped`/`out` → mirror to Backup →
`journalOff()` → `dropHydrated`.

One shared `restore` closure serves both the unwind release and the Backup-refused path — sharing
it is what keeps the two from drifting.

**`dropHydrated` runs only after the mirror**, per popped `seq`. Dropping it before would leave the
exemption understated after a rollback, and an understated exemption is permanent.

`Del` rebuilds `kept` and calls `heap.Init` — acceptable here because no unwind guard depends on
it and the operation is already O(n).

### 12.4 `btree.Backing` — keyed tidwall/btree (`btree.NewFIFO`, `btree.NewPriority`)

One implementation, two variants selected by the comparator:

- `core.FifoSeqLess` — insert sequence only.
- `core.PrioritySeqLess` — `Item.Less`, ties by insert sequence.

Tree is built `btree.NewBTreeGOptions(less, btree.Options{Degree: width, NoLocks: true})`.
**`NoLocks: true` is correct because `core.QLock` already serialises every access.**

`btree.index` maps `Item.Hash()` → the exact `core.SeqItem` locators, so `Exists` is a bucket lookup +
`Equal` scan and `Del` is O(log n) (the locator *is* the tree key). `remove` swaps with the last
element and truncates, deleting the map entry when the bucket empties.

#### Hash-before-mutate

`btreeIndex.add` and `remove` take the **hash as a parameter** rather than calling `si.item.Hash()`
themselves. That is the whole design:

- **Push** takes every `Item.Hash` for the batch *before the first `tree.Load`*. Taking each hash
  beside its own `Load` left a `Hash` that unwound with the entry already in the tree and never in
  the index: `Exists` reported false for it, `Del` removed nothing, and nothing but `Pop` or `Clear`
  could ever get it out. The index cannot be repaired afterwards either — putting the entry back
  means re-running `Hash`, the code that just failed.
- **`Hydrate`** hoists the hash the same way, for the same reason.
- **Pop** does **one ordered `tree.Scan` peek** — when there is anything to peek *for*, i.e. an
  index or a `Backup`; otherwise the pop loop fills `out` directly — that settles both the mirror
  batch and every index key before the first `PopMin`. `Scan` walks in pop order without mutating
  and without consulting
  the comparator. Taking a hash beside its own `PopMin` left an unwinding `Hash` with the entry out
  of the tree and still in the index — a permanent `Exists` false positive, and a `Del` counting a
  removal that did not happen. The pop cannot be undone either: reinserting runs the tree
  comparator, which on the priority variant is caller code too.

  Consequence: the pop loop reaches no caller code (`PopMin` removes the leftmost directly, no
  key search and no comparisons; `idx.remove` works from the pre-taken keys), so it needs **no
  unwind guard and leaves no half-mutated state** for one to describe.

`tree.Load` is used instead of `Set` on every insert: an O(1) append for ascending input, and `seq`
is strictly increasing (FIFO) / unique (priority tiebreak), so it is correct and much cheaper than
`Set`'s full descent.

`Del` on the indexed path scans only the buckets for the **distinct** hashes in `v`, deduping
collected items by `seq`, so a slot matched by duplicate or same-hash elements is removed once.
Unindexed it falls back to a full `Scan`.

**Known gap:** `btreeBacking.Push` is *not* all-or-none against a panicking `Item.Less` on the
priority variant. Making it so would need a transactional insert from tidwall/btree.

### 12.5 `bbolt.Backing` — on-disk (`bbolt.NewFIFO`, `bbolt.NewPriority`)

The database lives at `<root>/queue.db`, bucket `"items"`. **On reopen the database is the source
of truth.**

#### Key layout

| Variant | Key | Width |
|---|---|---|
| FIFO | big-endian per-bucket sequence | 8 bytes |
| priority | big-endian `Item.Priority()` ‖ big-endian sequence | 16 bytes |

bbolt iterates keys byte-lexicographically, so the head of the queue is the bucket's first key:
insert order for FIFO, highest priority (insert order breaking ties) for priority. The priority
half of the key is stored complemented, which is what turns "highest priority" into "smallest
key"; the sequence half is stored as-is so ties stay FIFO. The 16-byte priority key is
fixed-width and therefore inherently prefix-free.

`bolt.Open` takes bbolt's **process-wide file lock**. By default the wait is unbounded and `ctx`
does not bound it (`bolt.Open` has no context), so a second handle on the same root blocks until
the first is closed. `WithBoltTimeout` makes it fail instead. The indefinite default is deliberate:
a caller handing a store between processes wants the wait, and a finite default would turn that
handoff into an error. The open error wraps the **path** because bbolt's own contention error is
the bare string `"timeout"`; `bolt.ErrTimeout` stays reachable through `errors.Is`.

#### Codec

`encodeItem`/`decodeItem` use the `WithCodec` funcs if set, else `go-json-experiment/json`. Both
tag with `core.WrapCodec` at the single choke point rather than at ~14 call sites, so `ErrCodecFailed`
covers every returned codec error the way `ErrBackupFailed` covers every Backup error — otherwise
a caller cannot tell "my decoder rejected this row" from "the store is corrupt".

`diskCodecRequirer` is an unexported marker implemented by `Value` (its function fields cannot
round-trip through JSON). A codec-less `Value` queue is rejected with `ErrCodecRequired`.

#### Write path: staging buffer + group commit

Push does **not** write. It:

1. checks closed / kind / batch-size / `ErrOnAdmitUnsupported` / the `maxSize` gate
   (`count + inflight + len(vs) > maxSize` → wait on `notFull`);
2. if `len(buf) + len(vs) <= maxBatch`: run the side effect under the lock, append to `buf`,
   `inflight += len(vs)`, capture `r := p.cur`, unlock, non-blocking send on `flushReq`, then
   **`<-r.done`**;
3. else: capture `r`, unlock, poke `flushReq`, wait for `r.done` **or** `ctx.Done()`, and retry.

**The two waits differ deliberately.** The pre-buffer wait (step 3) is context-cancelable because
the items are not yet buffered. Once the items *are* buffered (step 2) the wait is **not**
cancelable: the batch has been accepted and someone must report its outcome.

`flushResult` is the per-batch rendezvous: the flusher sets `err` then closes `done`; every Push
whose items joined that batch reads `err` after `done` closes.

`maxBatch` therefore doubles as the staging-buffer size — larger values trade memory for fewer,
larger group commits.

#### The flusher

Started by `SetQueueLock` (not by `New` directly, and not from inside `flushLoop`):

```go
p.flushGroup.Go(context.WithoutCancel(p.flushCtx), func(context.Context) error {
    return p.flushLoop(p.flushCtx)
})
```

- **Pool choice.** `context.Pool(ctx)`, but if `pool.Limit() > 0` it switches to `pool.Default()`.
  The flusher lives for the whole life of the queue, so on a `Limited` pool it would hold a slot
  until `Close`, and a caller creating more queues than the limit would find `New` blocking forever
  with nothing able to break the tie (the submit context has its cancellation stripped). An
  unlimited pool is left alone so a caller's custom pool still runs and still reports the flusher.
- **`flusherStarted` is set here, before the submit, and must stay that way.** `Pool.Submit`
  enqueues and returns; the job runs later, so a `New` immediately followed by a `Close` can reach
  the join before `flushLoop` has executed a statement. Setting it inside `flushLoop` would have
  that `Close` read false, skip both the `flusherDone` join and `flushGroup.Wait`, and release
  under a flusher that had not started. The cost — a declined submit would leave `flusherDone`
  never closed — is unreachable: the flusher only ever lands on an unlimited pool, and submit
  declines only on an already-cancelled context, which the `WithoutCancel` rules out.

`flushLoop` selects on four arms:

| Arm | Action |
|---|---|
| `ctx.Done()` | final `doFlush`, then `closeFromFlusher(false, ctx.Err())`, return |
| `flushReq` | `doFlush` |
| `clearReq` | `runClear` |
| ticker (`flushInterval`, 100ms) | `doFlush` — an upper bound only |

Group commit: **every append signals**, so the first buffered item flushes immediately and items
arriving during an in-flight commit coalesce into the next batch.

The `ctx.Done()` arm closing the backing is load-bearing. The flusher is the only thing that can
ever commit for this queue, so its exit must close the backing whether `Close` cancelled the
context or the *caller* did (by cancelling the ctx handed to `NewBboltFIFO`). Without it, a queue
whose construction ctx was cancelled went on reporting itself open with nobody left to commit: the
next Push buffered, blocked on a flush result that could never arrive — a wait documented as not
cancelable — and `Close` returned nil without freeing it.

#### `doFlush`

1. If `ctx.Err() != nil`, substitute `context.WithTimeout(context.WithoutCancel(ctx), drainBudget())`.
   A shutdown drain still has to reach the caller's `Backup` with a **live** context — the drain
   exists to unblock pushers still waiting on their batch, and a dead context fails every one of
   them. Done here rather than in the shutdown arm because which arm notices the cancellation first
   is a coin flip, and `flushReq`/ticker/pre-clear-drain were all handing the dead one through.
   Bounded, because stripping cancellation with nothing in its place lets a `Backup` that waits on
   `ctx.Done()` hang the drain forever.
2. Under the lock: snapshot `buf`, clear it, **rotate `cur`** so new pushes join the next batch.
3. `r.err = p.commit(...)`; `close(r.done)`; `finished = true`.

The unwind guard (`finished` / `released` flags) is what keeps caller code from killing the queue:
a **panic** is converted into the batch's error and the flusher carries on; a **`Goexit`** cannot
be stopped, so `closeFromFlusher` is called instead of leaving the backing silently unable to
commit anything ever again. It releases `inflight` unless `commit` already did.

#### `commit`

Backup mirror → one `db.Update` (encode, `NextSequence`, `keyOf`, `Put`, collect index entries) →
then, in a **deferred** closure, take the lock to bump `count`, drop `inflight`, and add the index
entries. Deferred rather than explicit because `doFlush`'s guard calls `releaseInflight`, which
takes the same lock; an unwind inside that section (only `idx.add` could) would otherwise leave the
lock held and deadlock the guard against it. `*released` tells the guard the capacity is already
back so it cannot subtract twice.

A backup or write failure fails the **whole batch** — returned to every waiter — and the items are
never made visible.

#### `codeSubject` — the blame taxonomy on the flusher

The flusher runs three different bodies of caller code in sequence, and an unwind can come out of
any of them. Blaming whichever one a guard happened to name told a queue with **no Backup at all**
that its Backup had failed; naming none told a queue whose `Item` panicked that the *queue* had.

```go
type codeSubject struct { what, where string; sentinel error }
```

`setBackup` / `setCodec` / `setItem` / `setSideEffect` / `clear`. The zero value means no caller
code is running — which is its own answer: an unwind then came from the queue. `where` carries a
phase suffix (e.g. `" during the pre-clear drain"`) across `setCodec`/`setItem`.

`commit` sets the subject **around each individual call** and clears it immediately after, so
bbolt's own `Put` between an encode and a `keyOf` is not billed to the codec, and `keyOf`
(`Item.Priority`) and `Hash` get `ErrItemFailed` rather than nothing.

`setSideEffect(nil)` **clears** the subject — a caller who passed no hook must never be told one
failed. `setBackup(nil, …)` does the same.

#### `Clear` — routed through the flusher

`Clear` sends a `clearCmd` on `clearReq` and waits for `cmd.done`. Routing through the
single-threaded flusher serialises it with all in-flight commits: any item whose Push has buffered
or is mid-commit is drained first (its Push returns success and the items are briefly in the
queue), and only then is the bucket deleted. Without the routing, a `Clear` concurrent with a
mid-commit Push would delete the bucket before the commit's `db.Update` landed and the item would
reappear.

Cancellation is honoured only until the command is accepted — like a buffered Push. Once the
flusher has it, `Clear` waits for the result, so the caller never observes an error while the clear
(and its side effect) happens after the call. `doClear` itself aborts if the caller's ctx is
already cancelled when it starts, *before* the side effect runs.

`doClear` uses **`p.flushCtx` for the drain** and the **caller's ctx** for `backup.Clear`. The items
being drained belong to other callers' Pushes; a `Clear` caller's cancellation must not fail them
through `backup.Push`.

`runClear` is the unwind guard around it, and the reason it exists is that `doClear` runs caller
code on the flusher goroutine, which is not the caller's to lose: an escaping panic leaves
`flushLoop`, enters the shared worker pool, and ends the process. A panic is recovered and returned
to the `Clear` caller with the flusher still running; a `Goexit` cannot be stopped, so the flusher
is shut down deliberately (`closeFromFlusher(true, …)`) — the caller gets an error and every later
operation fails fast with `ErrClosed` instead of blocking on a flusher that is not coming back.

`inClear` (flusher-goroutine-local, no lock needed) tells `doFlush`'s guard whether an outer handler
is going to deliver a result and cancel the flush context, or whether it must do so itself. That is
the `cancel bool` on `closeFromFlusher`: **whoever will deliver the result cancels; everyone else
passes false**, because cancelling while an unwind is still on its way to deliver races that
delivery and `Clear` reports `ErrClosed` instead of what actually went wrong.

#### `closeFromFlusher(cancel bool, cause error)`

Marks the backing closed and settles the batch buffered against the current `flushResult` — this
goroutine was the only thing that could have committed it, so those pushers would block forever on
a `done` channel nobody is left to close. `cause` is passed in rather than invented because the
batch did not fail on its own merits: naming a sentinel it cannot justify told every collateral
pusher the Backup had failed, on queues with no Backup at all.

One case it cannot reach: a Push already waiting on an in-flight batch at that moment.

#### `Pop` — one transaction

Peek + mirror + delete must be **one `db.Update`**: `commit` runs `db.Update` without holding
`p.lk`, so a separate read transaction could see a higher-priority item inserted between the peek
and the delete.

Inside the transaction: pass 1 decodes the first `k` items in order (each decode guarded, since the
codec runs holding the write lock on a path that unlocks explicitly); side effect; Backup mirror;
`Hooks.FaultAfterBackup`; pass 2 deletes the first `k`.

On transaction failure with `backupDeleted == true`, `Backup.Restore` puts the head items back. If
the restore also fails, both errors are joined — see the `ErrBackupFailed` caveat in §6.

Index removal happens after the transaction, guarded (`idx.remove` calls `Item.Hash`); the items
are already gone from bbolt by then, so the release only unlocks.

#### `Del`

Collect (indexed: only the buckets for distinct hashes in `v`, deduped by `string(sk)`; unindexed:
full `ForEach`) → side effect → Backup mirror → one `db.Update` of deletes. On failure,
`Backup.Restore` compensates and the count is **0** — the delete did not land, so nothing was
removed however the restore went.

#### Shutdown state machine

The single most intricate part of the package. Guarded by `shutMu` with
`shutBusy` / `shutComplete` / `shutCommitted` / `shutDone` / `shutGErr` / `shutBErr` / `shutCErr`.

Three constraints, each learned from a version that violated it:

1. **No caller-supplied code runs while a lock is held.** `Backup.Close` can call back into the
   queue. When the release ran inside a `sync.Once`, every other `Close` was parked on that Once's
   mutex — one caller's mistake became a queue-wide deadlock. A `Once` is also marked done when its
   function panics, so a panicking `Backup.Close` released late callers with a nil error and a
   Backup that was never closed. Here a re-entrant `Close` finds `shutBusy`, waits on a channel
   with a deadline, and gets an error instead of hanging.
2. **Every wait that can hurt a bystander is bounded.** `sync.Group.Wait` cannot be cancelled — its
   own doc says the context it takes has no effect — so joining through it means a Backup that
   ignores ctx hangs `Close` with nothing able to break the tie. The join watches `flusherDone`
   (closed by a `defer` in `flushLoop`, so it fires on return, panic *and* `Goexit`) against
   `joinBudget()`. **The exception is the performer's own `Backup.Close`**: it is the caller's code
   on the caller's goroutine, so it is theirs to bound. Moving it off that goroutine to enforce a
   deadline left the queue touching their Backup *after* `Close` had returned — a worse contract
   than the wait it removed. Every *other* caller waits on `shutDone` against a deadline, so a
   `Backup.Close` that never returns costs only its own caller.
3. **A failed attempt leaves nothing released and can be retried.** If the flusher will not stop,
   the db is deliberately **not** closed: closing it under a live flusher destroys an accepted
   batch, which is worse than reporting that shutdown could not finish.

Paths:

- `shutComplete` → return the joined stored errors (copied under the lock; reading them after
  unlocking would be safe only by an argument about terminal state living elsewhere).
- `shutBusy` → wait on `shutDone` vs `joinBudget()`. On timeout, `shutCommitted` decides which
  error: committed → `ErrShutdownInProgress` ("still releasing, no retry will re-run it"); not
  committed → `ErrShutdownIncomplete` ("nothing happened, retry").
- Otherwise become the performer. All bookkeeping lives in a `defer`, because the caller's
  `Backup.Close` can end the frame without returning — doing it inline left `shutBusy` set and
  `shutDone` never closed, so every later `Close` waited out the deadline for a release that was
  never coming.

`err` is assigned **inside** that defer, not at the `return` statement: `cErr` is set by the
`closeDB` defer that runs *after* the return expression is evaluated, so returning the join
directly handed the performer a `nil` while every waiter got the real error — the exact inverse of
the everyone-gets-the-same-answer contract.

`bErr` is **pre-set** to `"%w: Backup.Close did not return"` and then overwritten by the call's own
result. A `Backup.Close` that unwinds never reaches the assignment, and the deferred bookkeeping
publishes whatever `bErr` holds to every later `Close`. Leaving it zero told those callers the
shutdown had succeeded with the Backup never closed.

`closeDB` tags a `db.Close` failure with `ErrShutdownIncomplete` rather than handing back the bare
bolt error. This is the one release failure the caller can do nothing about — it arrives with
`shutComplete` already true, so no later `Close` retries — and without the tag nothing in the error
distinguishes "the file lock is gone" from "the file lock may still be held by this process".

`Close` itself sets `closed = true` and **signals the parked waiters immediately**, before the
shutdown, and again in a `defer`. Waiters only need to see `closed`, and the shutdown can
legitimately take up to the join deadline; the deferred repeat means a shutdown that unwinds still
leaves nobody parked on a closed queue. An already-closed backing still runs the side effect and
still proceeds to `shutdown` — `closed` only means "no more operations", and the flusher sets it
too, from where it can release nothing, so that later `Close` is the one that owes the shutdown.

#### Shutdown budgets

```go
const bboltDrainDefault     = 30 * time.Second
const bboltJoinSlackDefault = 30 * time.Second

func (p *bboltBacking[T]) drainBudget() time.Duration { return p.drain }
func (p *bboltBacking[T]) joinBudget()  time.Duration { return p.drain + p.slack }
```

Per-instance fields seeded by the constructor, not package globals. `joinBudget` is **derived**
from the drain so shortening one shortens the other and containment is preserved: the join was once
equal to the drain, which meant a drain finishing comfortably inside its own limit still blew the
join — `Close` reported `ErrShutdownIncomplete`, released nothing, left the file lock held, and an
immediate retry succeeded milliseconds later.

They are fields rather than globals for the same reason `hooks` is per-instance. Tests shorten them
so a bound is observable inside a test deadline; a global written by a test's `Cleanup` races the
flusher of every queue any test left running, because `t.Context()` is cancelled *just before*
cleanups run and the flusher reads the drain on its way out. (Serial tests do not fix that — the
package has no `t.Parallel()` at all.) They are written once by the constructor before
`SetQueueLock` starts the flusher; a test overrides them in that same window, ordered ahead of
every read by the flush-context cancellation that wakes the reader.

---

## 13. Behaviour matrices

### Operation cost

| | `NewFIFO` | `NewBTreeFIFO` | `NewBTreeFIFO`+idx | `NewPriority` | `NewBTreePriority` | bbolt | bbolt+idx |
|---|---|---|---|---|---|---|---|
| Push | O(1) amort. | O(log n) | O(log n) | O(log n) | O(log n) | staged | staged |
| Pop | O(1) | O(log n) | O(log n) | O(log n) | O(log n) | O(k log n) | O(k log n) |
| Peek | O(1) | O(1) | O(1) | O(1) | O(1) | O(1) | O(1) |
| Exists | O(n) | O(n) | O(1) | O(n) | O(1) | O(n) full scan+decode | O(bucket) |
| Del | O(n) | O(n) | O(log n)/match | O(n)+Init | O(log n)/match | O(n) full scan+decode | O(bucket) |

### Semantics

| | in-memory ×4 | bbolt |
|---|---|---|
| `WithOnAdmit` | honoured (reservation) | `ErrOnAdmitUnsupported` |
| Hydrate exempt from `maxSize` | yes | **no** |
| Hydrate with existing data | n/a | store wins; `OnLoad` still driven |
| Side effect goroutine | caller's | caller's, **except `Clear`** (flusher) |
| Push wait cancelable | yes | pre-buffer yes, post-buffer **no** |
| `Item` unwind → sentinel | no (caller's goroutine) | yes on the flusher (`ErrItemFailed`) |

---

## 14. Non-obvious decisions, so they are not "fixed" by accident

- **`Queue.Push` with an empty batch never reaches a backing.** It returns `(true, nil)`, runs the
  side effect under `q.lk` (guarded, since it is the same lock the backings hold), and does not run
  `onAdmit` — so bbolt does not return `ErrOnAdmitUnsupported` for an empty batch. There is nothing
  to admit and nothing for the caller to make durable.
- **`Queue.Del` returns 0 on any error.** No partial deletion is ever reported; every backing rolls
  back rather than removing part of a batch. `Backing.Del`'s contract is the same.
- **`Del`'s count is entries removed, not query elements matched.** One query matching three queued
  entries reports 3; a duplicated query does not double-count.
- **`ErrClosed` beats context cancellation** in every blocked wait. That is what `ClosedOrCause`
  exists for: on wake it re-reads `closed` under the read lock and prefers `ErrClosed`.
- **`Pop(n)` panics on `n < 1`** — programmer error, not a returned error.
- **`Item.Priority` must be order-consistent with `Item.Less`.** Only bbolt's priority backing
  consults `Priority` for ordering; the others sort via `Less`. A queue whose `Priority` disagrees
  with `Less` will order differently on disk than in memory.
- **`Number.Hash` uses `reflect.Kind`** so it is correct for defined numeric types (`type ID int64`),
  which `NumberConstraint` admits via `~`. Floats are canonicalised with `+0.0` so `-0.0` and `+0.0`
  — which are `Equal` — hash equally.
- **`hashSeed` is per-process.** `Hash` only has to be self-consistent within one run: the
  `WithIndex` maps are in-memory and rebuilt at startup.
- **`Backup.Len()` was removed from the interface.** Nothing internal used it; `metrics.go` and
  `Queue.Len` both go through `Backing.Len`.
- **`heap.Backing.All` takes the write lock** (§9) — not a bug.
- **`btype.Backing.AllCOW` takes the write lock** for its O(1) snapshot (§9) — not a bug.

## 15. Test seams

The hooks compiled into production code. Production-nil, per-instance (not globals) so concurrent
tests on different queues do not collide. §16 covers the tests that use them.

| Seam | Where | Purpose |
|---|---|---|
| `bbolt.Hooks.FaultAfterBackup` | `Pop`, `Del` | fires after the mirror, before the on-disk delete — drives the `Restore` compensation path |
| `bbolt.Hooks.CommitStart` | top of `commit()` | pins the flusher mid-commit so a test can race `Clear`/`Close` against it |
| `bbolt.Hooks.DBClose` | `closeDB` | stands in for `db.Close`, handed the real close. The one failure no other seam can produce, because `cErr` is set by a defer running *after* the performer's return expression — the exact ordering the "everyone gets the same answer" contract turns on |
| `bbolt.Backing.SetDrainBudget` / `.SetJoinSlack` | shutdown | shorten the budgets so a bound is observable inside a test deadline (§12.5); `DrainBudgetFor`/`JoinBudgetFor` read them back |
| `bbolt.Backing.FlushCtx` / `.FlusherDone` | flusher | watch the flusher being told to stop, and having actually stopped |

Two more seams are interfaces in `core` rather than fields, and exist because the split made the
alternative — a test type-switching over every concrete backing — impossible to write from outside
those packages:

| Seam | Purpose |
|---|---|
| `core.Signals` | `NotFullSignal()` / `NotEmptySignal()`, so a test can wait for a parked producer or consumer without naming the backing. It replaced a five-arm type switch whose `default` a newly added backing would have fallen into. |
| `core.ClosedOrCauser` | `ClosedOrCause(ctx)`, so the Close-beats-cancellation precedence can be tested directly instead of through the timing of a real blocked operation. |

## 16. The test suite

Almost all of the suite is in `package queue` and drives the public API; three signal-primitive
files live in `core`, one bbolt white-box file in `bbolt`, and four storage tests in `fifo`, because
they reach internals those packages do not export. It
is mostly *regression* tests, and almost every one exists because a specific defect shipped. Each
entry below says what the test pins and, where it matters, which bug it was written against — that
is the part that cannot be recovered from reading the assertions.

Two properties recur and are worth stating once:

- **Liveness is probed with a writer, on a goroutine, against a timer.** A leaked *read* lock still
  admits readers, so a `Len`-based health check reports a wedged queue as healthy. And an operation
  parked inside `lk.Lock()` cannot observe a context deadline, so a ctx-bounded probe hangs instead
  of recording the fact.
- **Every error-bearing table carries a success row.** Not decoration: without it a queue that had
  stopped doing the operation entirely would satisfy every failure row.

### 16.1 Shared fixtures

| Fixture | File | What it is |
|---|---|---|
| `queueMakers()` | `unified_test.go` | The 11-config matrix every broad test runs: `fifo-slice`, `fifo-btree`, `fifo-btype`, `fifo-btree+index`, `priority-heap`, `priority-btree`, `priority-btree+index`, `fifo-bbolt`, `fifo-bbolt+index`, `priority-bbolt`, `priority-bbolt+index`. Parameterised by `maxSize` and `...Option`. |
| `memMakers()` | `unified_test.go` | `queueMakers()` minus bbolt, for tests bbolt cannot take. |
| `unwindMakers(t)` | `unwind_equivalence_test.go` | Exactly one maker per **source file** (`fifo-slice`, `fifo-btype`, `fifo-btree+index`, `priority-heap`, `fifo-bbolt`), selected **by name** and `t.Fatal`-ing if a name goes missing, so a rename is a failure rather than a silent hole. |
| `backingConfigs()` | `queue_test.go` | Older 12-config matrix (bounded/unbounded × kind × index) used by `TestBackingsConformance`. |
| `fakeBackup` | `backup_test.go` | In-memory `Backup[Number[int]]`. No internal locking — backings call it single-threaded under the queue lock. Carries `pushErr`/`delErr`/`restoreErr`/`onLoadErr` for returned failures and `pushHook`/`delHook`/`closeHook`/`clearHook`/`pushCtxHook` for arbitrary failure modes, so a harness can hold "how it fails" as data. `pushCtxHook` is the only way to observe the context the queue hands a `Backup`. |
| `fifoItem` / `prioItem` / `queryItem` / `itemFor` | `queue_test.go` | `Number[int]` builders. `prioItem(v)` sets `P = ^v` — a higher `P` pops sooner, so inverting keeps pop-by-priority equal to ascending value. The convention itself is pinned by `TestPriorityOrderIsDescending`, which bypasses this helper. |
| `diskRoot(t)` | `queue_test.go` | `os.OpenRoot(t.TempDir())`. |
| `sideEffectUnwinds()` | `sideeffect_unwind_test.go` | `normal` / `panic` / `Goexit`. |
| `failureModes()` | `unwind_equivalence_test.go` | `returns nil` (marked `succeeds`) / `returns an error` / `panics` / `calls runtime.Goexit`. |
| `onceHook(fail)` | `unwind_equivalence_test.go` | Fires the failure for the first caller only. Arming and then clearing a `fakeBackup` field would be a data race — the field is write-once instead. |
| `shortenShutdownBudgets(t, q)` | `sideeffect_unwind_test.go` | Calls `SetDrainBudget`/`SetJoinSlack` for 300ms each on a bbolt backing; no-op on other backings. See §12.5. |
| `hookInFlight(...)` | `onadmit_test.go` | Starts a Push whose `WithOnAdmit` hook blocks, runs a probe while the reservation is outstanding, then releases. Leaves the expected Push outcome to the caller, because the tests sharing it disagree about that. |
| `hydratedQ(...)` | `hydrated_test.go` | A bounded queue restored from a backup, plus the backup, so a caller can inject a mirror failure. |
| `withBboltFault(...)` | `backup_test.go` | Arms `Hooks.FaultAfterBackup` around a closure. |
| `restartBbolt(...)` | `backup_test.go` | Populates, closes, and reopens a bbolt store — the `count > 0` restart path. |

### 16.2 Conformance and scenarios

| Test | File | Purpose |
|---|---|---|
| `TestBackingsConformance` | `queue_test.go` | The broad API sweep over 12 configs: Push/Len/Peek/Exists, pop order, `Del` removing **all** `Equal` matches with the count being *entries removed* (one query removing two 7s reports 2; a duplicated query does not double-count), the three no-op `Del` shapes reporting 0 with a nil error, `RangeAll` order, `Clear`, `Close`. |
| `TestQueueSequential` | `unified_test.go` | Shuffled batch in, drain order out: insertion order for FIFO, ascending value otherwise (descending priority, under `prioItem`'s inverted encoding). |
| `TestQueueEmpty` | `unified_test.go` | Empty-queue behaviour: `Peek` not-found; blocked `Pop`/`NotEmpty` return on ctx cancel; a `Pop` blocked on empty unblocks on a concurrent Push. |
| `TestQueueFull` | `unified_test.go` | Bounded behaviour: Push blocks until space or cancel, `NotFull` errors on a cancelled ctx when full, an over-bound batch is `ErrBatchTooLarge`. |
| `TestQueueConcurrent` | `unified_test.go` | Many producers/consumers against an unbounded and a small bounded queue: every value delivered exactly once, no loss, duplication or deadlock. |
| `TestKindValidation` | `queue_test.go` | A priority backing rejects `Priority() == 0`, a FIFO backing rejects `> 0`. Each backing also gets an accepted item, so a backing refusing everything cannot pass on rejections alone. |
| `TestQueueCloseUnblocks` | `close_test.go` | `Close` unblocks an in-flight `Pop` (empty) and `Push` (full) with `ErrClosed`, on every backing. |
| `TestClosedOrCausePrecedence` | `close_test.go` | `ClosedOrCause` directly, no timing: a closed backing yields `ErrClosed` even with a cancelled ctx; an open one yields the cause. |
| `TestPopAfterCloseNonEmpty` | `pop_after_close_test.go` | Regression for a double-close panic. Every backing's `Pop` checked `closed` only *after* the non-empty branch, so a `Pop` on a closed-but-populated bounded queue took the items branch and `close()`d an already-closed channel. Closed `Pop` must return `ErrClosed`, bounded or not. |

### 16.3 Backup and hydration

| Test | File | Purpose |
|---|---|---|
| `TestBackupHydrate` | `backup_test.go` | `New` restores a pre-populated backup into every backing: items become the contents in backing order, `OnLoad` fires once per item, draining mirrors Pops back so the backup empties in lock-step. |
| `TestBackupHydrateOnLoad` | `backup_test.go` | The `OnLoad` contract: once per item; a failure on the first item aborts `New`; and the on-disk **restart** path (populated store, `count > 0`) still drives `OnLoad` per persisted item in stored order, matching the in-memory backings. |
| `TestBackupHydrateKind` | `backup_test.go` | `Hydrate` enforces the same kind rule as `Push`. The accepted rows show it is a rule and not a blanket refusal. |
| `TestBackupMirror` | `backup_test.go` | Every mutation mirrors the **exact** items: after Push/Pop/Del the backup is a true *multiset* mirror of the live contents, not merely the same length — including priority, where the popped item is not the backup's insertion front. `Clear` empties it, `Close` closes it. |
| `TestBackupPushError` | `backup_test.go` | A backup Push failure aborts the queue Push: error returned, item not added. |
| `TestBackupRestoreOrder` | `backup_test.go` | `Restore` re-inserts at the front in `vs` order, so a rolled-back head removal leaves the backup byte-for-byte as it was. |
| `TestBackupRestoreOnPopFailure` | `backup_test.go` | Forces the bbolt delete in `Pop` to fail after the mirror: `Restore` runs, the queue is unchanged, the backup is a true mirror again. |
| `TestBackupRestoreOnDelFailure` | `backup_test.go` | Same for `Del`. |
| `TestBackupDelCountOnFailure` | `backup_test.go` | Pins `Del`'s count on an on-disk failure: the write did not land, so the items are still on disk however the compensating restore went — anything but **0** tells an operator entries were removed at exactly the moment accurate numbers matter most. |
| `TestNewClosesTheBackupWhenHydrateFails` | `sideeffect_unwind_test.go` | The one path where no queue is handed back. Joining `b.Close` into the error was supposed to cover it and did not: every backing attaches the Backup as the *last* statement of a successful `Hydrate`, so on this path the backing's field is still nil and its `Close` skips the Backup entirely. `New` closes it instead. Success rows assert the inverse — a queue that *does* exist owns its Backup, so `New` must not close it and `Close` must. |
| `TestHydrationMaxSizeExemption` | `hydrated_test.go` | A restore can exceed `maxSize` and must not make the queue refuse everything until the backlog drains — a restart with a large backlog would stop accepting work exactly when it is most needed. The exemption must also decay by **whichever route** entries leave (Pop, Del, Clear), or one restore disables the limit for the life of the process. |
| `TestHydrationCreditFollowsRemovedEntries` | `hydrated_test.go` | The exemption tracks *which* entries left, not how many. Removing an admitted item must give its capacity back; charging it to the restored surplus understates the bound (the queue refuses work it has room for) and crediting both overstates it — checked from both directions. |

### 16.4 Bounded queues and lost wakeups

| Test | File | Purpose |
|---|---|---|
| `TestBoundedBatchPushWakesProducers` | `bounded_batch_test.go` | Lost-wakeup regression affecting **every** backing. A producer parks whenever `occupancy + len(vs) > maxSize`, which happens while occupancy is still *below* `maxSize` once `len(vs) > 1`. The backings only re-signalled when occupancy was at/over the bound, so a drain that freed capacity without occupancy ever reaching `maxSize` never woke it. Batch 3, `maxSize` 5: occupancy goes 0→3→park and no Pop ever sees 5. |
| `TestBboltBoundedDrainWakesProducers` | `bbolt_regression_test.go` | The bbolt-specific form: Push admits on `count + inflight`, but Pop only re-signalled when `count == maxSize` exactly. Buffered-uncommitted items live in `inflight`, so `count` can sit below the bound while the queue is logically full. |
| `TestBboltBoundedConcurrentPush` | `bounded_bbolt_test.go` | Concurrent over-admission: many Pushes race while their items are still uncommitted, so each passes an admission gate consulting only the committed count. The queue must never hold more than `maxSize`; the excess must block and return on cancel. |
| `TestBboltNotFullCountsStagedItems` | `sideeffect_unwind_test.go` | `NotFull` and `Push` must agree about "full". `NotFull` testing `count` alone reports room the very next Push refuses, so a producer wakes and immediately blocks. The success half shows `NotFull` still returns when there genuinely is room. |
| `TestSignalRaceMesaWait` | `signal_race_test.go` | The Mesa-wait lost wakeup. If `signal.Wait` ever stops invoking its unlock callback only **after** registering, a `Signal` landing entirely between "release the queue lock" and "enter Wait" sees `waiters == 0`, no-ops, and the producer parks forever. N producers × N consumers on `maxSize=1` across every in-memory backing, asserted to finish inside a hard 30s deadline — no waiter-count polling crutch. |

### 16.5 Abnormal control flow

The largest cluster, and the one the package's design is organised around. Five bodies of caller
code × three ways to leave (return an error, panic, `runtime.Goexit`) × every site that runs one
under a lock.

| Test | File | Purpose |
|---|---|---|
| `TestUnwindMatchesErrorReturn` | `unwind_equivalence_test.go` | **The structural guard.** Every unwind release is hand-written and nothing forces it to undo what the error return undoes; three separate defects came from exactly that omission (a reservation never handed back, a close never broadcast, a buffered batch never settled), each found by tripping over it rather than by a test. The rule needs no per-site knowledge, so it holds for sites nobody has written yet: **a panic or `Goexit` out of caller code must leave the queue in the same observable state as returning an error.** State is a comparable `queueState{Len, Responds, Admits, PushErr, Waiter}` so a failure prints a diff and names itself. 10 sites × 5 makers × 4 modes. |
| `TestSideEffectUnwindReleasesLock` | `sideeffect_unwind_test.go` | A side effect leaving abnormally cannot wedge the queue. Covers **every** operation, not just the five that were broken, so a future refactor dropping a `defer` from one of the safe four is caught too. |
| `TestBackupUnwindReleasesLock` | `sideeffect_unwind_test.go` | The same for the `Backup` under Push/Pop/Close. bbolt's Push is deliberately absent — it does not call the Backup on the caller's goroutine at all. |
| `TestItemMethodLockRelease` | `itemlock_test.go` | The fifth body of caller code, the one nobody opts into. Every other body is guarded at a named call; `Item` methods are required by the constraint and reached **indirectly** — heap and btree comparators, a sort's less func, the hash index — so a sweep for method names misses them, and the guards were missing at all 11 sites. Asserts a **later writer still makes progress**, not that the unwind propagated (it always did, and a test checking only that passes against the bug). `Goexit` is its own row because it defeats a `recover()`-based fix. |
| `TestPriorityHeapPopRestoreReleasesTheLock` | `itemlock_test.go` | The heap Pop's mirror-refused rollback, reachable only through a failing Backup. It *used* to be an `Item`-method site (re-heapify through `Less`); this pins that it stays not one — the fault armed inside `Backup.Del` is never reached, and the lock comes back. |
| `TestPriorityHeapPopMirrorRefusedKeepsTheError` | `invariant_test.go` | The rollback was throwing away the error it existed to report. Re-heapifying ran `Item.Less` on the frame that owed the caller `ErrBackupFailed`; a `Less` that ended that frame took the error with it and the caller never learned the mirror said no. Nothing deferred can return an error from a frame being destroyed, so the rollback must be *incapable* of failing. **Every row expects the same answer — that is the point: arming `Less` must make no difference.** |
| `TestPriorityHeapPushIsAllOrNone` | `invariant_test.go` | Push is documented all-or-none and was not: a `Less` ending the frame partway through a three-item batch left one behind for a Push that never returned, while the Pop side of the same backing rolled back — the two directions disagreed about what an unwind means. Contents are only half of it: the rollback must also leave a **valid heap**, so the check is that survivors come back out in priority order. |
| `TestBTreeIndexAgreesWithTheTree` | `invariant_test.go` | The two ways an unwinding `Item.Hash` left the index and the tree describing different queues. On Push the insert had landed before `idx.add` ran `Hash` — the entry was in the tree and never in the index (`Exists` false for a held item, `Del` a no-op, nothing but Pop or Clear could remove it). On Pop the mirror image — `PopMin` had removed it before `idx.remove` ran `Hash`, so the index kept a key for a gone item (permanent `Exists` false positive, `Del` counting phantom removals). Neither surfaces as an error and neither is repairable, so the assertion is the **agreement itself**: `RangeAll`, `Len`, `Exists`, `Del` and `Pop` must all describe one queue. |
| `TestCallerCodeUnderLockNeverLeaksIt` | `sideeffect_unwind_test.go` | The two sites no other test reaches: an `AllCOW` iterator body, and the item codec inside bbolt's `Pop`. Its own comment records that an earlier version *claimed* to be a mechanical sweep of every such site and was not — which made the `Item`-method gap read as covered for several review rounds, "which is worse than having had no test". |
| `TestWithOnAdmitUnwind` | `onadmit_test.go` | Both ways a hook frame leaves without returning (the hook is where a caller does durable I/O, the most panic-prone thing it does; `t.Fatal` reaches `Goexit`). Both must give back the reservation **and** the lock. Merged from two tests that each checked only one half — `recover()` reports a panic and says nothing about a `Goexit`. |
| `TestNewHydrateUnwindClosesEverything` | `contract_test.go` | `New`'s hydrate runs four bodies of caller code and only the error return closed the backing and the Backup. Both assertions matter and neither implies the other: an unclosed Backup is the caller's resource leak, an unclosed bolt handle is the whole store, unusable until the process exits. |
| `TestBboltIndexRebuildUnwindReleasesTheFileLock` | `contract_test.go` | The constructor's own caller code: `WithIndex` scans the store calling `Item.Hash` per record on the `New` caller's goroutine, before any queue exists. An unwind skipped `db.Close` and left the process-wide file lock held with nothing in existence to close it. **The assertion is that the store opens again**, not that the constructor reported something — an error return was already there and told the caller nothing about the handle. |

### 16.6 bbolt: flusher, clear, shutdown

| Test | File | Purpose |
|---|---|---|
| `TestBboltBufferedFlush` | `bbolt_buffer_test.go` | The group-commit buffer: over-cap batch rejected, at-cap batch commits and is poppable (proving Push blocked until the flusher committed), small batch flushed by the timer. |
| `TestBboltClearDrainsInFlightPush` | `bbolt_clear_test.go` | Regression: `Clear` used to take `p.lk` (which the flusher releases at the top of `commit`) and delete the bucket; the flusher then completed its `db.Update` against the recreated bucket and **the item reappeared**. The fix routes `Clear` through the flusher. |
| `TestBboltDoClearDrainUsesFlusherCtx` | `bbolt/doclear_ctx_test.go` | Regression: `doClear`'s drain commits items pushed by *other* callers, so it must use the flusher's lifetime ctx. With the Clear caller's ctx, a cancelled `Clear` failed an unrelated in-flight Push through `backup.Push` and that Push returned the cancellation. |
| `TestBboltDoClearCanceledCtx` | `bbolt/doclear_ctx_test.go` | A `Clear` caller cancelled while its command waits in the flusher queue must not have its side effect (or the clear) run behind its back. |
| `TestBboltClearSideEffectUnwind` | `sideeffect_unwind_test.go` | The one side-effect site not on the caller's goroutine. An escaping panic leaves `flushLoop`, enters the shared worker pool and **ends the process**; a `Goexit` ends the flusher, after which `Clear` waits forever and no later Push commits. Panic → error with the flusher still running; `Goexit` → the queue closes. |
| `TestBboltBackupUnwindOnFlusher` | `sideeffect_unwind_test.go` | The same for the `Backup` call the flusher makes during commit. |
| `TestBboltFlusherDeathSettlesBufferedBatch` | `sideeffect_unwind_test.go` | The batch that was never in flight. `doFlush` rotates the result before committing, so a Push arriving mid-commit buffers against a *fresh* one. Closing only the batch being committed leaves that second Push on a channel nobody will close — and its wait is deliberately not ctx-cancelable, so it never returns at all. |
| `TestBboltClearDrainBlamesTheBackup` | `sideeffect_unwind_test.go` | `Clear` drains before it looks at its own side effect, and the drain runs the `Backup` — so an unwind there is not the side effect's fault, and saying so (especially when no side effect was supplied) sends the caller looking in the wrong place. Which arm the flusher's select takes is roughly a coin flip, so the scenario is run repeatedly and is **required to land on the drain at least once** so it cannot quietly stop covering it. |
| `TestBboltClearBlamesTheCodeThatFailed` | `sideeffect_unwind_test.go` | Attribution across all three bodies a `Clear` runs in sequence. Reporting whichever ran most recently is not the same as reporting the one that ended the frame. |
| `TestFlusherBlamesTheCodecNotTheBackup` | `sideeffect_unwind_test.go` | Both paths run on a queue with **no Backup configured**, so any mention of one is plainly wrong: the batch whose own codec unwound, and the batch buffered behind it, which did not fail on its own merits and was told the Backup had failed. Getting the second under test needs a batch actually waiting when the flusher dies — an earlier version let the first finish first and the mutation survived. |
| `TestItemMethodUnwindOnFlusherBlamesTheItem` | `sideeffect_unwind_test.go` | The codec's blame window was correctly narrowed to the encode call, but nothing took over the vacated ground, so a panicking `Item` came back as "the queue panicked while committing" with no sentinel — the queue wearing the blame for caller code. Nothing else is configured on this queue, which is what makes the assertion sharp. |
| `TestBboltCloseTearsDownAfterFlusherDeath` | `sideeffect_unwind_test.go` | Closing is not the same fact as releasing, and the flusher cannot release anything from where it dies. A `Close` treating "already closed" as "nothing to do" returns nil having closed neither the Backup nor the handle, and the file lock survives for the process's life — reopening the directory blocks forever, because bolt's default timeout is zero. |
| `TestBboltCloseReleasesDBOnBackupUnwind` | `sideeffect_unwind_test.go` | The same resource by the other route: an unwind out of `Backup.Close` must not take the bolt handle with it. The backing is already marked closed by then, so a retried `Close` cannot recover it. |
| `TestBboltCloseBackupUnwindWakesWaiters` | `sideeffect_unwind_test.go` | The broadcasts live *after* the Backup call and an unwind skips them. Releasing the lock is not enough on its own. |
| `TestBboltCloseIsOnceNotSkip` | `sideeffect_unwind_test.go` | "Once" means later callers **wait for it and inherit its result**, not that they sail past a flag. Waving them through let a `Close` return nil while the first was still inside `flushGroup.Wait` — a caller told the store was released with the file lock still held — and let the second tear the db down under a live flusher: `Backup.Close` running inside its own in-flight `Backup.Push`, and an accepted batch dying with "database not open". |
| `TestBboltCloseSurvivesReentrantBackup` | `sideeffect_unwind_test.go` | Forbidden reentrancy should cost the caller its own operation, not the queue. Under a `sync.Once` it was a queue-wide deadlock. "It came back at all" is not enough — that is compatible with the shutdown being abandoned — so the outer `Close` must report success, the Backup must be closed, the handle released, and the re-entrant call told it failed rather than handed a nil it did nothing to earn. |
| `TestBboltCloseReportsPanickingBackupClose` | `sideeffect_unwind_test.go` | Marking the shutdown done and handing everyone a zero error made `Close` report success with the Backup never closed. |
| `TestBboltShutdownReportsTheSameAnswer` | `sideeffect_unwind_test.go` | The contract the whole design exists for. The performer used to return before the deferred `db.Close` recorded its error, so it alone saw nil. **Only `db.Close`'s error can show that**, because it is set after the performer's return expression is evaluated; an earlier version injected a `Backup.Close` failure, set *before* that point, and deleting the re-join line left it green. Both failures are injected, and the "arrives after it is already complete" ordering is covered too — every caller in the earlier version was concurrent, so nobody took that path. |
| `TestBboltShutdownDrainIsBounded` | `sideeffect_unwind_test.go` | The drain must hand the `Backup` a **live** context (or it fails every pusher it exists to unblock) and must be **bounded** (or a Backup waiting on `ctx.Done()` hangs `Close` forever — `sync.Group.Wait` cannot be cancelled). The first version asserted neither and both halves survived their mutants: it selected on the test's own ctx, and let the batch commit before `Close` so the drain found an empty buffer and never called the Backup. |
| `TestBboltShutdownBudgetsContain` | `sideeffect_unwind_test.go` | A join budget that does not contain the drain budget fails a shutdown that was going to succeed. The end-to-end half pins the flusher inside the first batch's commit while a second buffers behind it, so the **drain** is what commits the second batch and the flusher cannot stop until two slow Backups have run. |
| `TestCloseDBFailureIsReportedAsIncompleteShutdown` | `contract_test.go` | The one shutdown failure a caller can do nothing about: it arrives with `shutComplete` already true so nothing retries it, and as a bare bolt error nothing in it distinguished "the store is released" from "the file lock may still be held". |
| `TestBboltConstructorContextCancel` | `invariant_test.go` | Cancelling the ctx handed to the constructor killed the flusher while the queue went on reporting itself open: the next Push staged and blocked on a flush result nobody was left to produce — a wait documented as not ctx-cancelable, so the caller's own ctx could not free it — and `Close` returned without releasing. `Close` is unaffected because it marks closed *before* cancelling, and the success row says so. |

### 16.7 Iteration

| Test | File | Purpose |
|---|---|---|
| `TestRangeAllCOWParity` | `cow_test.go` | `RangeAllCOW` yields the same items as `RangeAll` on a quiescent queue. |
| `TestRangeAllCOWContention` | `cow_test.go` | A concurrent writer does not deadlock the iteration, and the snapshot is consistent. Correct regardless of scheduling: contended → snapshot and release; uncontended → it simply ranged the originals. |
| `TestRangeAllCOWReleasesLock` | `cow_test.go` | The eight explicit `release()` calls in `fifo.go`/`btree.go`/`heap.go`'s `AllCOW` **were pinned by nothing** — each is shadowed by the deferred release on the same closure, so the suite stayed green with all eight deleted. Deleting them does not leak the lock; it holds it across every yield, which is exactly the contract `AllCOW` exists to break. The instrument is a loop body that costs real time, and the assertion is *how far the scan had got* when the writer landed — the same number on an idle machine and a thrashing one, which is why it is asserted instead of elapsed time. |
| `TestRangeAllCOWReleasesLockWhenClosed` | `cow_test.go` | The `ErrClosed` error exit, which the above cannot reach (it never gets past the closed check). That yield runs the caller's loop body like any other, and the deferred release does not fire until the body has already returned. |
| `TestRangeAllCOWReleasesLockOnCancel` | `cow_test.go` | The third error exit. It was the odd one out: `fifo`/`heap`/`btype` released first while `btree` and `bbolt` yielded from *inside* the `Scan`/`View` callback under the lock — but only until a writer contended, after which both switch to a snapshot and yield unlocked. Which behaviour a caller saw came down to whether a writer had turned up. Now unconditional, which is what makes the test writable. Both branches per backing, since they are the two that used to disagree. |
| `TestBboltRangeAllCOWReleasesLock` | `bbolt_regression_test.go` | bbolt held the read lock across the entire decode-heavy scan, blocking the writer for the whole iteration. |

### 16.8 Options, errors, telemetry

| Test | File | Purpose |
|---|---|---|
| `TestBadOption` | `contract_test.go` | The single question "did I configure this wrong?". Option-misuse errors were anonymous strings, and "max batch must be at least 1" existed as **six separate `errors.New` values across six backings** — six identities for one semantic error that no `errors.Is` could unify. |
| `TestSentinelsArePermanent` | `contract_test.go` | Which sentinels stop a retry. None were marked, so a caller wrapping in `retry/exponential` retried a programmer error — an over-size batch, a closed queue, a nil hook — until the budget ran out. **The retryable rows are not filler**: marking `ErrEmpty` permanent would break every consumer that retries a `Pop`, which is the ordinary way to use this package. |
| `TestOperationErrorMetrics` | `contract_test.go` | Two defects, one fix. The counter decided on the deferred `*errp` alone, so an operation killed by a panicking side effect recorded **zero errors and an OK span** — the queue reporting perfect health while failing every call (`Goexit` is its own row because it defeats a `recover()`-based fix). And it had no blame attribute, so "the caller's Backup is failing every Push" and "the queue failed" were one data point — the exact distinction the five sentinels exist to draw, discarded where an operator would look at it. |
| `TestOpOptionValidation` | `onadmit_test.go` | The two ways an operation option is wrong at the call site: a nil func, and `WithOnAdmit` on an operation with no admission step. Both used to be accepted and silently do nothing — which for `WithOnAdmit` means dropping the caller's durable write. Every operation appears in a success row so a regression rejecting options wholesale cannot hide. |
| `TestBTreeWidth` | `contract_test.go` | The full `WithBTreeWidth` range matrix. The default was patched in *after* the options ran, which cannot tell "never asked" from "asked for 0", so `WithBTreeWidth(0)` was promoted to 32 while 1 and −1 were rejected. The third maker is the same hole by a different route: `NewBTreeFIFO` without `WithIndex` returns before the constructor's check, so every out-of-range width was accepted there while the identical call *with* `WithIndex` was refused. |
| `TestWithBTreeWidth` | `extras_test.go` | The round trip that shows a legal width still builds a working queue (the range matrix lives in `contract_test.go`). |
| `TestBackingOptionRejection` | `extras_test.go` | The shared `BackingOption` type rejects a constructor it does not support. |
| `TestBackingOptsApplied` | `extras_test.go` | Every bbolt passthrough option lands on `core.BackingOpts` — the plumbing check for twelve options at once. |
| `TestWithSideEffect` | `extras_test.go` | Runs under the lock as part of a successful op; a failure rolls the op back (nothing pushed/popped/closed). |
| `TestBackupErrorIsTaggedAsTheBackups` | `sideeffect_unwind_test.go` | A Backup's own error cannot pass itself off as the queue's — a Backup returning `ErrClosed` made an open, healthy queue report it had closed. The tag is additive, so the original stays reachable. All three tagged methods are covered (an earlier version drove only Push, so two thirds of the sites it claimed were untested), each with a succeeding Backup as well. |
| `TestCodecErrorIsTaggedAsTheCodecs` | `sideeffect_unwind_test.go` | The third body. Untagged, a caller could not tell "my decoder rejected this row" from "the store is corrupt". Both directions — a returned error and an unwind — are covered. |
| `TestEmptyNameDisablesTelemetry` | `codec_test.go` | An empty name means `met == nil` and no telemetry, with operations still working. |

### 16.9 `WithOnAdmit`

| Test | File | Purpose |
|---|---|---|
| `TestWithOnAdmit` | `onadmit_test.go` | Both outcomes: exactly once per admitted batch, and an error leaves the queue untouched while releasing the reserved capacity. Each case sizes the queue so **the batch is the entire bound**, which is what makes the follow-up Push a real test of the release — a leaked reservation leaves no room and it blocks. |
| `TestWithOnAdmitInFlight` | `onadmit_test.go` | What must hold while the hook runs unlocked: a reader still gets through (the whole reason it is not a `WithSideEffect`), the bound is still held by the reservation (dropping the lock must not let a concurrent Push overshoot), and a `Close` landing mid-hook is reported rather than the batch being inserted into a closed queue. |
| `TestWithOnAdmitReleasesOnLaterFailure` | `onadmit_test.go` | Missed-wakeup regression. Hook succeeds, side effect then fails: the reservation goes back with nothing inserted, so a producer that parked *because of that reservation* has to be woken — nothing else in that path will. The error return and the two unwinds all owe it; only the error return paid until the unwind callback was given the same duty. The success row is what stops the three failure rows from agreeing about nothing. |
| `TestWithOnAdmitOnDisk` | `onadmit_test.go` | bbolt rejects rather than ignores: silently ignoring would run the caller's durable write at a moment that does not mean what the option promises. The success case shows the rejection is about the option, not the queue refusing pushes generally. |

### 16.10 The `core.Signal` primitive

`internal/backings/core/signal_test.go` covers it in isolation, independent of any queue. It moved
there with the primitive: it reads the unexported waiter counter, which no other package can see.

| Test | Purpose |
|---|---|
| `TestSignalWaitThenSignal` | Basic flow: blocked `Wait` returns nil after `Signal`, counter back to zero. |
| `TestSignalMultipleWaiters` | The core broadcast guarantee — one `Signal` releases every registered waiter. |
| `TestSignalContextCancel` | A blocked `Wait` returns `context.Cause(ctx)` on cancel, and the counter decrements via the deferred `Add(-1)`. |
| `TestSignalRepeatedCycles` | Reuse across many cycles: after `Broadcast` the `parker.Waiter` is rearmed, so the next `Register` blocks again. |
| `TestSignalNoWaiters` | `Signal` with nobody registered is safe and leaves the Waiter armed — confirmed by a no-op `Signal`, then a `Wait` that still blocks until the second. |
| `TestSignalWaitBlocksUntilSignal` | `Wait` actually blocks — asserted as non-return over a window, then released. |
| `TestSignalMixedCancelAndSuccess` | Among N waiters, half cancelled before `Signal` and half signalled: each gets the right answer and the counter lands at zero. |
| `TestSignalAlreadyCancelledCtx` | An already-cancelled ctx must still register, return the cause, and decrement. |
| `TestSignalConcurrentSignals` | Races between `Wait`'s `Register`+`Add(1)` under `signal.mu` and `Signal`'s `Broadcast` under the parker's own mutex, over many overlapping batches. |

### 16.11 The `fifo` backing's storage

`internal/backings/fifo/fifo_test.go`. These are the only tests in the package, and they exist
because no correctness test can see what they pin: the queue returns exactly the right items
whether or not its storage is reused, so the assertions have to be an allocation count and a
capacity.

| Test | Purpose |
|---|---|
| `TestPushReusesPoppedSpace` | Pins `head`. Restoring the old `buf = buf[k:]` takes a Push/Pop pair from 1 alloc to 2, because the abandoned prefix leaves `append` a shorter tail every time. |
| `TestStorageDoesNotGrowWithThroughput` | Pins the slide, which the allocation count cannot see — without it the reallocations are on a doubling schedule and amortize away, while `cap(buf)` grows with total throughput instead of with depth. |
| `TestSlideDoesNotPinRemovedItems` | Pins the `clear` after the copy. **The first version was vacuous**: a push/pop loop at depth one settles at `len == cap == 1`, so the tail it checked was empty and the test stayed green with the clear deleted. It now builds the shape deliberately and fails loudly if either the setup or the result leaves nothing to check. |
| `TestCompactionPreservesContents` | The slide is the one place this backing moves contents rather than appending or handing them out; a wrong length or source there reorders or drops items. |

### 16.12 Item types and codecs

| Test | File | Purpose |
|---|---|---|
| `TestStringItem` | `extras_test.go` | `String`'s `Item` methods plus an end-to-end FIFO queue. |
| `TestBytesItem` | `extras_test.go` | `Bytes`'s methods plus an end-to-end **priority** queue. |
| `TestValueItem` | `extras_test.go` | `Value[T]`'s caller-supplied `Equaler`/`Hasher`, the nil-guard panics, and an end-to-end FIFO queue. |
| `TestNumberHashDefinedTypes` | `number_hash_test.go` | Regression: `Number.Hash` must key off the **underlying** numeric kind so defined types work. A concrete-type switch matches the exact dynamic type (`hashDefinedInt != int64`) and fell through to a panic; the `reflect.Kind` implementation is correct. |
| `TestValueCodecBbolt` | `codec_test.go` | A `Value` round-trips through an on-disk FIFO with `WithCodec` — payload and priority survive Push → store → Pop. |
| `TestValueCodecRejected` | `codec_test.go` | A `Value` queue on an on-disk backing without `WithCodec` is rejected at construction with `ErrCodecRequired`. |
| `TestBboltMlock` / `TestBboltNoSync` / `TestBboltOpenFile` | `extras_test.go` | The bolt passthrough options actually work end-to-end: mlock+mmap flags; the whole no-sync/freelist/mmap/pagesize set over 50 items in order; and `WithBoltOpenFile` routing the open through a caller-supplied function. |
| `TestNewWithLimitedPool` | `itemlock_test.go` | A construction that never returned. The flusher runs for the life of the queue and was submitted to whatever pool the ctx carried — on a `worker.Pool.Limited` each live queue permanently held a slot, so `New` for the (limit+1)th blocked forever, and the submit ctx has its cancellation stripped so the caller's own ctx could not break the tie. *(Lives in this file for want of a collision-free name of its own.)* |

### 16.13 Fuzz

`fuzz_test.go` decodes an operation script from the fuzz input and checks every observable result
against a trivial reference model. The queue is unbounded so Push never blocks, and removing ops are
only issued when the model is non-empty so Pop never waits.

| Fuzz target | Model |
|---|---|
| `FuzzFIFO` | `fifoModel` — values in insertion order. |
| `FuzzPriority` | `prioModel` — `prioItem`'s encoding (`P = ^v`), so pop order is ascending value with insertion order breaking ties. |

### 16.14 Examples

`example_test.go` carries eleven runnable `Example` functions — the package's user-facing
documentation, verified by their `// Output:` blocks: `Example` (FIFO), `_priority`, `_peek`,
`_existsDelete`, `_rangeAll`, `_bounded`, `_bbolt`, `_value`, `_waitNotEmpty`, `_backup`,
`_sideEffect`.

### 16.15 Benchmarks

| Benchmark | File | Measures |
|---|---|---|
| `BenchmarkSignal` | `bench_signal_test.go` | The per-backing waiter-counter gating, three scenarios per backing: `PopNoWaiter` (the headline — every Pop's `notFull` signal gated to a no-op), `PopWithWaiter` (no-regression sanity), `PushWasEmpty`. bbolt runs only `PopNoWaiter`, since disk wall time would drown the allocation signal. The file header carries the before/after `benchstat` recipe. |
| `BenchmarkSignalPrim*` | `bench_signal_primitive_test.go` | The `core.Signal` primitive in isolation: `NoWaiter` (alloc+close cost), `HasWaitersGated` (the queue's actual hot path), `HasWaitersOnly` (just the predicate), `PingPong` (end-to-end wake latency). |
| `BenchmarkDiag*` | `bench_alloc_diag_test.go` | Allocation attribution for the above: pool round-trip, raw `context.AfterFunc`+cancel, `AfterFunc` with a bound method value (does the method-value escape cost an alloc on top?), and `PingPong` with `context.Background` so `Wait` takes its no-watcher branch — which subtracts the `AfterFunc`/method-value overhead from the headline number. |
| `BenchmarkBoundedPopAllocs` | `bench_test.go` | Allocs per `Pop` on a bounded queue with no parked producer. Before the gating, every bounded Pop did `close()`+`make()` on a channel — one alloc per op. **Expectation: 0 allocs/op.** |
| `BenchmarkExists` / `BenchmarkDel` | `bench_test.go` | `WithIndex` vs the scan-based path. `Del` refills each iteration so every call actually removes something. |
| `BenchmarkPushPop` | `bench_test.go` | Index-maintenance overhead on the hot path. |
| `BenchmarkNumberHash` | `builtin_bench_test.go` | `Number.Hash`'s `reflect.ValueOf` kind dispatch across the three encoding paths (signed, unsigned, float). |
| `BenchmarkMemory*` / `BenchmarkDisk*` / `BenchmarkOursBatchFillDrain` | `compare_bench_test.go` | Fill/drain and concurrent-push against `nsqio/go-diskqueue`, `beeker1121/goque` and `tidwall/btype`. Disk sizes are deliberately small (serial fill/drain does durable I/O per item, so cost is disk latency × N); the concurrent and batched runs are the meaningful throughput comparisons. `OursBatchFillDrain` covers the batched Push path — one txn/fsync per batch — for the `*Queue` backings only, since the others have no batch API. |

## 17. Known gaps

- `btree.Backing.Push` is not all-or-none against a panicking `Item.Less` on the priority variant
  (§12.4).
- Nothing pins bbolt's `ErrClosed` exit path in a test.
- A number of test files still import stdlib `context`; `signal_race_test.go` and
  `compare_bench_test.go` still import stdlib `sync` (the former uses `sync.WaitGroup` and needs a
  rewrite to `sync.Group`, not an import swap).
- `btype`, `heap` and `btree` have no tests of their own. Everything that exercises them runs from
  `package queue` through the public API, which is where it belongs — but it does mean
  `go test ./internal/backings/btype/` and its siblings report "no test files". `fifo` and `bbolt`
  have package tests only because they pin things the public API cannot observe.
