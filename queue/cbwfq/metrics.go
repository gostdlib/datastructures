package cbwfq

import (
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/telemetry/otel/trace/span"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

// schedMetrics holds the OTEL instruments shared by every scheduler operation. It
// is built once in New and panics on instrument-construction error, matching the
// queue package. Per-class depth is reported as a delta on a single UpDownCounter
// carrying the cbwfq.class attribute.
type schedMetrics struct {
	ops     metric.Int64Counter
	errs    metric.Int64Counter
	latency metric.Float64Histogram
	depth   metric.Int64UpDownCounter
}

func newSchedMetrics(m metric.Meter) *schedMetrics {
	ops, err := m.Int64Counter(
		"cbwfq.operations",
		metric.WithDescription("Total number of scheduler operations invoked."),
	)
	if err != nil {
		panic(err)
	}
	errs, err := m.Int64Counter(
		"cbwfq.operation.errors",
		metric.WithDescription("Total number of scheduler operations that returned an error."),
	)
	if err != nil {
		panic(err)
	}
	latency, err := m.Float64Histogram(
		"cbwfq.operation.duration",
		metric.WithDescription("Duration of scheduler operations."),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(err)
	}
	depth, err := m.Int64UpDownCounter(
		"cbwfq.class.depth",
		metric.WithDescription("Current number of items in a class queue."),
	)
	if err != nil {
		panic(err)
	}
	return &schedMetrics{ops: ops, errs: errs, latency: latency, depth: depth}
}

// instrument starts (or no-ops) a span for op and records the operation count. The
// returned func finishes it: it records latency and, on a non-nil *errp, increments
// the error counter and marks the span. When telemetry is disabled (name == "",
// s.met == nil) it is a no-op returning ctx unchanged.
func (s *CBWFQ[T]) instrument(ctx context.Context, op string) (context.Context, func(*error)) {
	if s.met == nil {
		return ctx, func(*error) {}
	}
	start := time.Now()
	ctx, sp := context.NewSpan(ctx, span.WithName("github.com/gostdlib/datastructures/queue/cbwfq.CBWFQ."+op))
	attrs := metric.WithAttributes(
		attribute.String("cbwfq.name", s.name),
		attribute.String("cbwfq.operation", op),
	)
	s.met.ops.Add(ctx, 1, attrs)

	return ctx, func(errp *error) {
		s.met.latency.Record(ctx, time.Since(start).Seconds(), attrs)
		err := *errp
		if err != nil {
			s.met.errs.Add(ctx, 1, attrs)
			if sp.IsRecording() {
				sp.Span.RecordError(err)
				sp.Status(codes.Error, err.Error())
			}
		}
		sp.End()
	}
}

// recordDepth emits the change in a class queue's length as a delta on the depth
// UpDownCounter. The atomic Swap makes the running total self-correcting. Caller
// holds s.mu. No-op when telemetry is disabled.
func (s *CBWFQ[T]) recordDepth(ctx context.Context, cs *classState[T]) {
	if s.met == nil {
		return
	}
	cur := cs.queue.Len()
	old := cs.lastDepth.Swap(cur)
	s.met.depth.Add(ctx, cur-old, metric.WithAttributes(
		attribute.String("cbwfq.name", s.name),
		attribute.String("cbwfq.class", cs.name),
	))
}
