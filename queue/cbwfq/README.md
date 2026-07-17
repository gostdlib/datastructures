# cbwfq

`package cbwfq` is a Class-Based Weighted Fair Queuing scheduler over a set of
`queue.Queue` instances — one queue per traffic class. It classifies items into
classes on `Push` and schedules dequeues across classes on `Pull` so dequeue
bandwidth is shared in proportion to each class's weight. It is the software
equivalent of a router's **LLQ + CBWFQ**.

```go
import "github.com/gostdlib/datastructures/queue/cbwfq"
```

The runnable snippets below live in `example_test.go` and render on the package's
[pkg.go.dev](https://pkg.go.dev) page (`go test`).

## Model

- **Classify on Push.** A caller-supplied `Classifier[T]` maps each item to a class
  name; the item is routed into that class's queue. An item that names no known
  class goes to the mandatory **Default** class.
- **Schedule on Pull.** The optional strict-priority **Priority** class is drained
  first whenever it is non-empty (router network-control traffic); otherwise a
  **Deficit Weighted Round Robin** grants each class a per-round quantum equal to
  its `Weight` and serves it that many items before advancing. Every item is unit
  cost, so a weight-3 class is served three items for every one from a weight-1
  class. Scheduling is work-conserving: an empty class is skipped, never idled.

Weight is a property of the **class**, not the item. A class set must contain
exactly one `Default` class and at most one `Priority` class.

## Classes

| `Class[T]` field | Meaning |
|---|---|
| `Name` | Unique, non-empty class name; returned by `Pull`. |
| `Weight` | Relative dequeue share among weighted classes (`>= 1`). Ignored when `Priority`. |
| `Queue` | The class's `*queue.Queue[T]`. The scheduler owns it after `New`. |
| `Default` | Exactly one class must set this; classifier misses route here. |
| `Priority` | At most one class may set this; drained ahead of all weighted classes. |

Once a queue is handed to the scheduler, mutate it only through the scheduler.

## Quick start (weighted fair dequeue)

```go
ctx := context.Background()

s, err := cbwfq.New(ctx, "example", classByPrefix, []cbwfq.Class[queue.String]{
	{Name: "std", Weight: 1, Queue: mkQ(), Default: true},
	{Name: "bulk", Weight: 3, Queue: mkQ()},
})
if err != nil {
	panic(err)
}
defer s.Close(ctx)

for i := 0; i < 2; i++ { s.Push(ctx, queue.String{V: "std:x"}) }
for i := 0; i < 6; i++ { s.Push(ctx, queue.String{V: "bulk:y"}) }

for i := 0; i < 8; i++ {
	_, class, _ := s.Pull(ctx, 1)
	fmt.Print(class, " ") // std bulk bulk bulk std bulk bulk bulk
}
```

## Strict-priority (control) class

A `Priority` class is always served first when it has traffic, ahead of every
weighted class — like router network-control:

```go
s, _ := cbwfq.New(ctx, "example", classByPrefix, []cbwfq.Class[queue.String]{
	{Name: "std", Weight: 1, Queue: mkQ(), Default: true},
	{Name: "ctrl", Queue: mkQ(), Priority: true},
})

s.Push(ctx, queue.String{V: "std:1"})
s.Push(ctx, queue.String{V: "std:2"})
s.Push(ctx, queue.String{V: "ctrl:1"}) // arrives last, served first

// Pull order: ctrl:1, std:1, std:2
```

A high-rate priority class can starve the weighted classes; assume control traffic
is low-rate (an LLQ-style rate cap is a possible future addition).

## API

| Method | Behavior |
|---|---|
| `New(ctx, name, classifier, classes)` | Validate the class set and build the scheduler. `name` labels OTEL spans/metrics (`""` disables scheduler telemetry). |
| `Push(ctx, item, opts...)` | Classify one item and route it to its class's queue. Blocks only if the target class is a bounded, full queue. |
| `Pull(ctx, n, opts...)` | Return up to `n` items from a single class (priority if non-empty, else the DWRR pick), plus the class name. Blocks until some class is non-empty. |
| `Len()` | Total items across all class queues. |
| `ClassLen(name)` | Depth of one class's queue; `ErrUnknownClass` if the name is unknown. |
| `Close(ctx)` | Close every class queue; unblocks any waiting `Pull` with `queue.ErrClosed`. |

`Push`/`Pull` options are `queue.OpOption` (e.g. `queue.WithSideEffect`) and forward
to the underlying `queue.Push`/`queue.Pop`. `Pull` returns `queue.ErrClosed` after
`Close` (precedence over context cancellation) and `context.Cause(ctx)` on
cancellation; it panics if `n < 1`.

## Notes

- Batch `Pull` returns items from a **single** class per call, capped at the
  class's remaining round quantum for weighted classes, so a large `n` cannot
  exceed a class's weighted share.
- `Pull` blocks only when **every** class is empty and is woken by the next `Push`;
  there is no per-queue polling.
