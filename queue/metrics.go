package queue

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/telemetry/otel/trace/span"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

// instrumentedOps is every operation that calls instrument. The table below is built from it at
// construction, so an operation added without a row here panics on its first call rather than
// silently recording nothing.
var instrumentedOps = [...]string{
	"Push", "Pop", "Peek", "Exists", "Del", "Clear", "Close",
	"NotEmpty", "NotFull", "RangeAll", "RangeAllCOW",
}

// errKinds is the closed queue.error_kind set, crossed with instrumentedOps to build errAttrs.
var errKinds = [...]string{
	errKindBackup, errKindCodec, errKindSideEffect, errKindOnAdmit,
	errKindItem, errKindQueue, errKindUnwind,
}

// opInstruments is everything about one operation that does not vary between calls. It exists
// because all of it used to be rebuilt on every single operation: the span name by string
// concatenation, the attribute sets by metric.WithAttributes — which copies its argument slice and
// then builds an attribute.Set — and the span option by closing over the name. A queue with a name
// paid for all of it even with no tracer and no exporter configured, because the arguments are
// built at the call site before span.New gets to short-circuit on a non-recording context.
//
// Everything here is derived from (queue name, operation), so it is computed once per queue.
type opInstruments struct {
	// Every field is a one-element slice rather than the bare option it holds. That is not
	// incidental: NewSpan, Counter.Add and Histogram.Record all take their options variadically,
	// so passing a single option builds a fresh one-element slice at the call site on every
	// operation. Storing the slice and expanding it with ... hands the callee the same backing
	// array each time and allocates nothing. None of them append to it — span.New only ranges
	// over its options, and the metric SDK only reads them to fold into a config — so sharing is
	// safe.

	// spanOpts holds a precomputed span.WithName for this operation.
	spanOpts []span.Option
	// addOpts and recordOpts both carry queue.name + queue.operation: the first for the operation
	// counter, the second for the latency histogram. They are separate fields only because Add and
	// Record take different option interfaces; the underlying attribute set is one value.
	addOpts    []metric.AddOption
	recordOpts []metric.RecordOption
	// errAddOpts is the above plus queue.error_kind, one entry per value in the closed kind set.
	errAddOpts map[string][]metric.AddOption
}

// queueMetrics holds the OTEL instruments shared by every operation on a Queue. It is
// built once in New and panics on instrument-construction error, matching
// github.com/gostdlib/base/concurrency/background.
type queueMetrics struct {
	// ops is the numbers of operations that have occurred, such as Push, Pop, ...
	ops metric.Int64Counter
	// errs are the numbers of errors encountered by ops.
	errs metric.Int64Counter
	// latency is the latency for various ops.
	latency metric.Float64Histogram
	// depth is the current queue depth.
	depth metric.Int64UpDownCounter
	// lastDepth was the last depth recorded.
	lastDepth atomic.Int64
	// byOp holds the per-operation constants. Keyed by the same string the call sites pass.
	byOp map[string]opInstruments
	// depthAddOpts is the queue.depth attribute set, pre-wrapped as Add options for the same reason
	// the opInstruments slices are. depth is a queue-level quantity, so unlike the others it carries
	// only queue.name and there is one of it rather than one per operation.
	depthAddOpts []metric.AddOption
}

func newQueueMetrics(m metric.Meter, name string) *queueMetrics {
	ops, err := m.Int64Counter(
		"queue.operations",
		metric.WithDescription("Total number of queue operations invoked."),
	)
	if err != nil {
		panic(err)
	}
	errs, err := m.Int64Counter(
		"queue.operation.errors",
		metric.WithDescription("Total number of queue operations that returned an error."),
	)
	if err != nil {
		panic(err)
	}
	latency, err := m.Float64Histogram(
		"queue.operation.duration",
		metric.WithDescription("Duration of queue operations."),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(err)
	}
	depth, err := m.Int64UpDownCounter(
		"queue.depth",
		metric.WithDescription("Current number of items in the queue."),
	)
	if err != nil {
		panic(err)
	}
	byOp := make(map[string]opInstruments, len(instrumentedOps))
	for _, op := range instrumentedOps {
		errAddOpts := make(map[string][]metric.AddOption, len(errKinds))
		for _, kind := range errKinds {
			errAddOpts[kind] = []metric.AddOption{metric.WithAttributes(
				attribute.String("queue.name", name),
				attribute.String("queue.operation", op),
				attribute.String("queue.error_kind", kind),
			)}
		}
		attrs := metric.WithAttributes(
			attribute.String("queue.name", name),
			attribute.String("queue.operation", op),
		)
		byOp[op] = opInstruments{
			spanOpts:   []span.Option{span.WithName("github.com/gostdlib/datastructures/queue.Queue." + op)},
			addOpts:    []metric.AddOption{attrs},
			recordOpts: []metric.RecordOption{attrs},
			errAddOpts: errAddOpts,
		}
	}

	return &queueMetrics{
		ops:          ops,
		errs:         errs,
		latency:      latency,
		depth:        depth,
		byOp:         byOp,
		depthAddOpts: []metric.AddOption{metric.WithAttributes(attribute.String("queue.name", name))},
	}
}

// The values of the queue.error_kind attribute on queue.operation.errors. The set is closed and
// every value is a compile-time constant, so the metric's cardinality is bounded no matter what an
// error says: nothing derived from error text ever reaches an attribute. They name which body of
// code failed, which is the question an operator paging on this metric is actually asking — "the
// caller's Backup is refusing every Push" and "the bbolt store is corrupt" were previously the same
// data point.
const (
	errKindBackup     = "backup"
	errKindCodec      = "codec"
	errKindSideEffect = "side_effect"
	errKindOnAdmit    = "on_admit"
	errKindItem       = "item"
	// errKindQueue is the queue's own failure: none of the five caller-code sentinels matched.
	errKindQueue = "queue"
	// errKindUnwind is an operation that ended without returning — a panic or a runtime.Goexit out
	// of caller-supplied code. There is no error to classify on that path, which is exactly why it
	// needs a value of its own rather than being folded into errKindQueue: the queue did not fail.
	errKindUnwind = "unwind"
)

// errorKind classifies err for the queue.error_kind attribute, in the order the ErrBackupFailed doc
// tells callers to triage: the five caller-code sentinels first, and only when none of them matches
// does the queue take the blame. A joined error carrying more than one tag reports the first match
// in that order, so the caller's code is named ahead of the queue.
func errorKind(err error) string {
	switch {
	case errors.Is(err, ErrBackupFailed):
		return errKindBackup
	case errors.Is(err, ErrCodecFailed):
		return errKindCodec
	case errors.Is(err, ErrSideEffectFailed):
		return errKindSideEffect
	case errors.Is(err, ErrOnAdmitFailed):
		return errKindOnAdmit
	case errors.Is(err, ErrItemFailed):
		return errKindItem
	}
	return errKindQueue
}

// opTimer finishes what instrument started: it records latency, and on a failure increments the
// error counter and marks the span.
//
// It is a value with a method rather than the closure it used to be, and that is the whole reason
// it exists as a named type. A closure returned from instrument captures ctx, the span, the start
// time and the instruments, so it has to be heap-allocated; every call site then wrapped it in a
// second closure to defer it, allocating again. Two allocations per operation, on every operation,
// for a call that could be deferred directly. As a value the defer is open-coded and neither
// allocation happens.
//
// The zero value is the telemetry-disabled case: end is then a no-op.
type opTimer struct {
	met   *queueMetrics
	oi    opInstruments
	ctx   context.Context
	sp    span.Span
	start time.Time
}

// end records the operation's outcome. errp must be non-nil (every caller passes the address of a
// named return), and so must completed.
//
// There are two ways to fail, and deciding on *errp alone only sees one of them. Every queue
// operation runs caller-supplied code — a Backup, a codec, a side effect, a WithOnAdmit hook, an
// Item method — and any of those can end the operation without returning. The deferred call still
// runs on that path, with *errp still nil, so an operation killed by a panicking Backup used to be
// recorded as a success: zero errors counted and a span ended with OK status. completed is the same
// flag idiom core.RunLocked uses, and for the same reason: recover() reports nothing during a
// runtime.Goexit, so a guard written around it covers only half of the abnormal exits.
//
// The counter and the span agree by construction — one switch decides both — because a metric that
// says an operation failed and a trace that says it succeeded is worse than either alone.
func (t opTimer) end(errp *error, completed *bool) {
	if t.met == nil {
		return
	}
	t.met.latency.Record(t.ctx, time.Since(t.start).Seconds(), t.oi.recordOpts...)
	err := *errp
	switch {
	case !*completed:
		// The operation never reached its return: caller-supplied code panicked or called
		// runtime.Goexit. There is no error value to record on the span, so the status message
		// says what happened instead of naming an error that does not exist.
		t.met.errs.Add(t.ctx, 1, t.oi.errAddOpts[errKindUnwind]...)
		if t.sp.IsRecording() {
			t.sp.Status(codes.Error, "the operation ended without returning: caller code panicked or called runtime.Goexit")
		}
	case err != nil:
		t.met.errs.Add(t.ctx, 1, t.oi.errAddOpts[errorKind(err)]...)
		if t.sp.IsRecording() {
			t.sp.Span.RecordError(err)
			t.sp.Status(codes.Error, err.Error())
		}
	}
	// End unconditionally: always end what you started (OTEL convention).
	// span.Span.End is itself a no-op on a non-recording span.
	t.sp.End()
}

// instrument starts (or no-ops) a span for op and records the operation count. Use as:
//
//	ctx, done := q.instrument(ctx, "Push")
//	completed := false
//	defer done.end(&err, &completed)
//	... the whole operation ...
//	completed = true
//
// There are two ways to fail, and deciding on *errp alone only sees one of them. Every queue
// operation runs caller-supplied code — a Backup, a codec, a side effect, a WithOnAdmit hook, an
// Item method — and any of those can end the operation without returning. The deferred call still
// runs on that path, with *errp still nil, so an operation killed by a panicking Backup used to be
// recorded as a success: zero errors counted and a span ended with OK status. completed is the same
// flag idiom runLocked uses, and for the same reason: recover() reports nothing during a
// runtime.Goexit, so a guard written around it covers only half of the abnormal exits.
//
// The counter and the span agree by construction — one switch decides both — because a metric that
// says an operation failed and a trace that says it succeeded is worse than either alone.
//
// When the queue has no name (q.met == nil) telemetry is disabled and this is a no-op
// returning ctx unchanged. Otherwise span.New still no-ops unless ctx already carries a
// recording span and the meter no-ops unless an exporter is configured.
//
// Nothing here builds an attribute set or a span name: those are constant for a given
// (queue, operation) and are computed once by newQueueMetrics. See opInstruments.
func (q *Queue[T]) instrument(ctx context.Context, op string) (context.Context, opTimer) {
	if q.met == nil {
		return ctx, opTimer{}
	}
	oi, ok := q.met.byOp[op]
	if !ok {
		// Not reachable from a caller: every operation passes its own literal. This catches the one
		// mistake a hand-written string invites — a new operation added without a row in
		// instrumentedOps, which would otherwise record nothing and say nothing about it. Same
		// guard, for the same reason, as core.ResolveOpOptions on an unregistered OpCall.
		panic("bug: queue.instrument called with an operation missing from instrumentedOps: " + op)
	}
	start := time.Now()
	ctx, sp := context.NewSpan(ctx, oi.spanOpts...)
	q.met.ops.Add(ctx, 1, oi.addOpts...)

	return ctx, opTimer{met: q.met, oi: oi, ctx: ctx, sp: sp, start: start}
}

// recordDepth emits the change in queue length as a delta on the depth UpDownCounter.
// The atomic Swap makes the running total self-correcting, so it converges to the true
// Len even under concurrent mutation. depth is a queue-level quantity, so it carries
// only the queue.name attribute (no per-operation split).
func (q *Queue[T]) recordDepth(ctx context.Context) {
	if q.met == nil {
		return
	}
	cur := q.backing.Len()
	old := q.met.lastDepth.Swap(cur)
	q.met.depth.Add(ctx, cur-old, q.met.depthAddOpts...)
}
