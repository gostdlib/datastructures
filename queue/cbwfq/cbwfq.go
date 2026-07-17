package cbwfq

import (
	"fmt"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/telemetry/otel/metrics"
	"github.com/gostdlib/datastructures/queue"
)

// Classifier maps an item to the name of the class it belongs to. A result that
// names no registered class (including "") routes the item to the Default class.
type Classifier[T queue.Item[T]] func(item T) string

// Class defines one traffic class: a name, a scheduling weight, its queue, and its
// role (default sink and/or strict-priority). Construct the queue with the queue
// package and hand it to New; the scheduler owns it thereafter.
type Class[T queue.Item[T]] struct {
	// Name identifies the class in telemetry and is returned by Pull. It must be
	// non-empty and unique within the class set.
	Name string
	// Weight is the relative dequeue share among the weighted (non-Priority)
	// classes; it must be >= 1. A weight-3 class is served three items for every
	// one item of a weight-1 class. Ignored when Priority is true.
	Weight int
	// Queue is this class's backing queue. It must be non-nil. The scheduler owns
	// it after New; mutate it only through the scheduler.
	Queue *queue.Queue[T]
	// Default marks the catch-all class. Exactly one class in the set must set
	// this; classifier results that name no known class route here.
	Default bool
	// Priority marks the strict-priority (control) class. At most one class in the
	// set may set this. When non-empty it is drained ahead of every weighted class.
	Priority bool
}

// classState is the scheduler's mutable per-class bookkeeping. All fields except
// the immutable identity are guarded by CBWFQ.mu.
type classState[T queue.Item[T]] struct {
	name     string
	weight   int
	queue    *queue.Queue[T]
	priority bool
	// deficit is the DWRR deficit counter: the number of items the class may still
	// emit this round. Unused for the priority class.
	deficit int
	// lastDepth is the last queue depth reported to the depth gauge, enabling a
	// self-correcting delta. Only touched when telemetry is enabled.
	lastDepth atomic.Int64
}

// CBWFQ is a Class-Based Weighted Fair Queuing scheduler over a fixed set of class
// queues. It is safe for concurrent use.
type CBWFQ[T queue.Item[T]] struct {
	mu sync.Mutex

	classifier Classifier[T]
	byName     map[string]*classState[T]
	// weighted holds the non-priority classes in round-robin order; cursor indexes it.
	weighted []*classState[T]
	cursor   int
	// priority is the strict-priority class, or nil if the set has none.
	priority *classState[T]
	// def is the catch-all class classifier misses route to.
	def *classState[T]

	// pending is the total item count across all class queues. Writes happen under
	// mu (ordering the wake signal); reads by Len are lock-free.
	pending atomic.Int64
	closed  bool
	sig     *signal

	name string
	met  *schedMetrics
}

// New creates a scheduler over classes. name labels OTEL spans/metrics; an empty
// name disables scheduler telemetry (each class queue still carries its own).
// classes must be non-empty with unique, non-empty names, exactly one Default, at
// most one Priority, Weight >= 1 on every non-Priority class, and a non-nil Queue
// on each; classifier must be non-nil.
func New[T queue.Item[T]](ctx context.Context, name string, classifier Classifier[T], classes []Class[T]) (*CBWFQ[T], error) {
	if classifier == nil {
		return nil, fmt.Errorf("classifier cannot be nil")
	}
	if len(classes) == 0 {
		return nil, fmt.Errorf("at least one class is required")
	}

	s := &CBWFQ[T]{
		classifier: classifier,
		byName:     make(map[string]*classState[T], len(classes)),
		sig:        newSignal(),
		name:       name,
	}

	var defaults, priorities int
	var total int64
	for _, c := range classes {
		if c.Name == "" {
			return nil, fmt.Errorf("class name cannot be empty")
		}
		if _, ok := s.byName[c.Name]; ok {
			return nil, fmt.Errorf("duplicate class name %q", c.Name)
		}
		if c.Queue == nil {
			return nil, fmt.Errorf("class %q has a nil Queue", c.Name)
		}
		if !c.Priority && c.Weight < 1 {
			return nil, fmt.Errorf("class %q has Weight %d, want >= 1", c.Name, c.Weight)
		}

		cs := &classState[T]{name: c.Name, weight: c.Weight, queue: c.Queue, priority: c.Priority}
		s.byName[c.Name] = cs
		if c.Priority {
			priorities++
			s.priority = cs
		} else {
			s.weighted = append(s.weighted, cs)
		}
		if c.Default {
			defaults++
			s.def = cs
		}
		total += c.Queue.Len()
	}

	switch {
	case defaults == 0:
		return nil, fmt.Errorf("exactly one class must set Default, got none")
	case defaults > 1:
		return nil, fmt.Errorf("exactly one class must set Default, got %d", defaults)
	case priorities > 1:
		return nil, fmt.Errorf("at most one class may set Priority, got %d", priorities)
	}

	s.pending.Store(total)
	if name != "" {
		meter := context.MeterProvider(ctx).Meter(metrics.MeterName(2))
		s.met = newSchedMetrics(meter)
	}
	return s, nil
}

// Push classifies item and routes it to its class's queue. If the classifier names
// no registered class, the item goes to the Default class. Push blocks only if the
// target class is a bounded queue that is full (like queue.Push); it honors context
// cancellation and returns queue.ErrClosed after Close. options forward to the
// underlying queue.Push.
func (s *CBWFQ[T]) Push(ctx context.Context, item T, options ...queue.OpOption) (err error) {
	ctx, done := s.instrument(ctx, "Push")
	defer func() { done(&err) }()

	cs := s.byName[s.classifier(item)]
	if cs == nil {
		cs = s.def
	}

	// The class queue's Push must not run under s.mu: a bounded, full class blocks
	// until a Pull drains it, and Pull needs s.mu. Push into the queue first, then
	// take s.mu only to update pending and wake a parked Pull.
	ok, err := cs.queue.Push(ctx, []T{item}, options...)
	if !ok {
		return err
	}

	s.mu.Lock()
	s.pending.Add(1)
	s.recordDepth(ctx, cs)
	if s.sig.HasWaiters() {
		s.sig.Signal()
	}
	s.mu.Unlock()

	// A true ok with a non-nil err carries only the side-effect result (the item is
	// queued); propagate it.
	return err
}

// Pull returns up to n items from a single class: the Priority class if non-empty,
// otherwise the next class chosen by Deficit Weighted Round Robin. The returned
// slice is capped at min(n, items available) and, for weighted classes, the class's
// remaining round quantum, so a large n cannot exceed a class's weighted share. The
// second return value is the class the items came from. Pull blocks until some class
// is non-empty; it returns queue.ErrClosed after Close (precedence over context
// cancellation) and context.Cause(ctx) on cancellation. n must be >= 1 or Pull
// panics. options forward to the underlying queue.Pop.
func (s *CBWFQ[T]) Pull(ctx context.Context, n int, options ...queue.OpOption) (items []T, class string, err error) {
	if n < 1 {
		panic("invalid argument: n must be >= 1")
	}
	ctx, done := s.instrument(ctx, "Pull")
	defer func() { done(&err) }()

	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, "", queue.ErrClosed
		}

		if cs := s.serveable(); cs != nil {
			items, err = s.serve(ctx, cs, n, options...)
			s.mu.Unlock()
			return items, cs.name, err
		}

		// Every class is empty: park until a Push wakes us (or ctx/Close fires).
		// sig.Wait releases s.mu after registering as a waiter.
		if werr := s.sig.Wait(ctx, s.mu.Unlock); werr != nil {
			return nil, "", s.closedOrCause(ctx)
		}
	}
}

// serveable returns the class Pull should serve next, or nil if every class is
// empty. The priority class wins whenever it is non-empty; otherwise DWRR advances
// the cursor to the next non-empty weighted class, granting a fresh quantum on
// arrival. Caller must hold s.mu.
func (s *CBWFQ[T]) serveable() *classState[T] {
	if s.priority != nil && s.priority.queue.Len() > 0 {
		return s.priority
	}
	for scanned := 0; scanned < len(s.weighted); scanned++ {
		cs := s.weighted[s.cursor]
		if cs.queue.Len() == 0 {
			cs.deficit = 0
			s.cursor = (s.cursor + 1) % len(s.weighted)
			continue
		}
		if cs.deficit == 0 {
			cs.deficit = cs.weight
		}
		return cs
	}
	return nil
}

// serve pops from cs and updates DWRR/pending state. cs is guaranteed non-empty by
// serveable, so the underlying Pop returns immediately. Caller must hold s.mu.
func (s *CBWFQ[T]) serve(ctx context.Context, cs *classState[T], n int, options ...queue.OpOption) ([]T, error) {
	take := min(n, int(cs.queue.Len()))
	if !cs.priority {
		take = min(take, cs.deficit)
	}

	items, err := cs.queue.Pop(ctx, take, options...)
	s.pending.Add(-int64(len(items)))
	s.recordDepth(ctx, cs)
	if err != nil {
		return items, err
	}

	if cs.priority {
		return items, nil
	}

	cs.deficit -= len(items)
	// Advance the round-robin cursor when the class exhausts its quantum or empties;
	// reset the deficit on empty so the class starts fresh next visit (DWRR rule).
	if cs.deficit == 0 || cs.queue.Len() == 0 {
		if cs.queue.Len() == 0 {
			cs.deficit = 0
		}
		s.cursor = (s.cursor + 1) % len(s.weighted)
	}
	return items, nil
}

// Len returns the total number of items across all class queues.
func (s *CBWFQ[T]) Len() int64 { return s.pending.Load() }

// ClassLen returns the number of items in the named class's queue. It returns
// ErrUnknownClass if no class has that name.
func (s *CBWFQ[T]) ClassLen(name string) (int64, error) {
	cs, ok := s.byName[name]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownClass, name)
	}
	return cs.queue.Len(), nil
}

// Close closes every class queue and releases the scheduler's resources. Any Pull
// blocked waiting for an item unblocks and returns queue.ErrClosed. After Close the
// scheduler must not be used.
func (s *CBWFQ[T]) Close(ctx context.Context) (err error) {
	ctx, done := s.instrument(ctx, "Close")
	defer func() { done(&err) }()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return queue.ErrClosed
	}
	s.closed = true
	// Wake any parked Pull; it re-locks, sees closed, and returns ErrClosed.
	s.sig.Signal()
	s.mu.Unlock()

	for _, cs := range s.byName {
		if cerr := cs.queue.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// closedOrCause returns queue.ErrClosed if the scheduler is closed, else the
// context cause. Used after sig.Wait returns an error without holding s.mu.
func (s *CBWFQ[T]) closedOrCause(ctx context.Context) error {
	s.mu.Lock()
	c := s.closed
	s.mu.Unlock()
	if c {
		return queue.ErrClosed
	}
	return context.Cause(ctx)
}
