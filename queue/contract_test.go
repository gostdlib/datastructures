package queue

import (
	"errors"

	"iter"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/bbolt"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ctFail is what caller code returns in the error variant of a case here.
var ctFail = errors.New("caller code failed")

// ctLockProbe is how long a verification open waits for the bolt file lock before it reports the
// lock as still held. bbolt's default is to wait forever, which would turn "the handle leaked" into
// a hung test rather than a failing one — the whole point of these cases is that the second open
// gets an answer.
const ctLockProbe = 5 * time.Second

// ctUnwind is one way caller-supplied code can end a queue operation without returning. Goexit is
// listed separately from panic and is not optional: recover() reports nothing during a Goexit, so
// every guard in this package that decided on recover() covered only the panic half, and the tests
// that were written around recover() passed against the bug they were meant to catch.
type ctUnwind struct {
	name string
	// fn is what the caller's code does. nil means it returns normally.
	fn func()
}

func ctUnwinds() []ctUnwind {
	return []ctUnwind{
		{name: "panics", fn: func() { panic("caller code exploded") }},
		{name: "calls runtime.Goexit", fn: func() { runtime.Goexit() }},
	}
}

// ctProbe runs op on its own goroutine and waits for it to end however it ends. A Goexit takes the
// goroutine with it, so the probe cannot be a plain call; the recover is here only to keep a
// panicking row from taking the test binary down. It reports whether op returned normally.
func ctProbe(t *testing.T, name string, op func()) bool {
	t.Helper()

	returned := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recover() }()
		op()
		returned = true
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: the probe never returned", name)
	}
	return returned
}

// ctBackup is a Backup[Number[int]] whose OnLoad can be made to end the frame without returning.
// fakeBackup carries an onLoadErr but no hook, and the hydrate-unwind cases need OnLoad to panic or
// call runtime.Goexit rather than return.
type ctBackup struct {
	items  []Number[int]
	closed bool
	// onLoadHook, when non-nil, runs at the top of OnLoad. It is how a case makes hydrate end
	// without returning.
	onLoadHook func()
	// pushErr, when set, is what Push returns.
	pushErr error
}

var _ Backup[Number[int]] = (*ctBackup)(nil)

func (b *ctBackup) Push(ctx context.Context, vs []Number[int]) error {
	if b.pushErr != nil {
		return b.pushErr
	}
	b.items = append(b.items, vs...)
	return nil
}

func (b *ctBackup) Del(ctx context.Context, vs []Number[int]) error {
	for _, v := range vs {
		for i, it := range b.items {
			if it.Equal(v) {
				b.items = append(b.items[:i], b.items[i+1:]...)
				break
			}
		}
	}
	return nil
}

func (b *ctBackup) Restore(ctx context.Context, vs []Number[int]) error {
	b.items = append(append([]Number[int]{}, vs...), b.items...)
	return nil
}

func (b *ctBackup) Close(ctx context.Context) error {
	b.closed = true
	return nil
}

func (b *ctBackup) Clear(ctx context.Context) error {
	b.items = nil
	return nil
}

func (b *ctBackup) RangeAll(ctx context.Context) iter.Seq2[Number[int], error] {
	return func(yield func(Number[int], error) bool) {
		for _, it := range b.items {
			if !yield(it, nil) {
				return
			}
		}
	}
}

func (b *ctBackup) OnLoad(ctx context.Context, v Number[int]) error {
	if b.onLoadHook != nil {
		b.onLoadHook()
	}
	return nil
}

// ctReopens reports whether the bbolt store under root can be opened again, which is the only
// honest way to ask whether the file lock was released: bolt's lock is process-wide and held by the
// handle, so a leaked handle makes this fail (or, without a timeout, hang) for the life of the
// process with no queue in existence for anyone to Close.
func ctReopens(t *testing.T, ctx context.Context, root *os.Root) error {
	t.Helper()

	b, err := NewBboltFIFO[Number[int]](ctx, root, WithBoltTimeout(ctLockProbe))
	if err != nil {
		return err
	}
	q, err := New[Number[int]](ctx, "", b, 0)
	if err != nil {
		return err
	}
	return q.Close(ctx)
}

// TestBboltIndexRebuildUnwindReleasesTheFileLock covers the constructor's own body of caller code.
// With WithIndex the on-disk constructor scans the whole store and calls Item.Hash for every record
// (and a WithCodec decoder, when one is given) on the New caller's goroutine, before any queue
// exists. Only the returned error released the bolt handle, so caller code that panicked or called
// runtime.Goexit there skipped db.Close and left the store's file lock held with nothing in
// existence to close it: the next NewBboltFIFO on that path blocked, or with WithBoltTimeout failed
// with a bare "timeout", for the rest of the process's life.
//
// The assertion is that the store opens again, not merely that the constructor reported something.
// An error return was already there before the fix and told the caller nothing about the handle.
func TestBboltIndexRebuildUnwindReleasesTheFileLock(t *testing.T) {
	tests := []struct {
		name string
		// fn is what Item.Hash does during the index rebuild. nil means it returns normally.
		fn func()
		// wantErr is true when the reopening constructor is expected not to return normally.
		wantErr bool
	}{
		{
			// The anchor: without it nothing distinguishes "the fix released the handle" from
			// "this path never held one".
			name: "Success: an Item whose Hash returns cleanly leaves a usable queue behind",
		},
		{
			name:    "Error: an Item.Hash that panics during the index rebuild must not keep the file lock",
			fn:      func() { panic("Hash exploded") },
			wantErr: true,
		},
		{
			name:    "Error: an Item.Hash that calls runtime.Goexit during the index rebuild must not keep the file lock",
			fn:      func() { runtime.Goexit() },
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestBboltIndexRebuildUnwindReleasesTheFileLock(" + test.name + ")"
		root := diskRoot(t)

		// Seed the store so the rebuild has records to walk, then let go of it entirely.
		seed, err := NewBboltFIFO[unwindItem](ctx, root, WithIndex())
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		sq, err := New[unwindItem](ctx, "", seed, 0)
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		if _, err := sq.Push(ctx, []unwindItem{{V: 1}, {V: 2}, {V: 3}}); err != nil {
			t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
		}
		if err := sq.Close(ctx); err != nil {
			t.Fatalf("%s: seeding Close got err == %s, want err == nil", name, err)
		}

		if test.fn != nil {
			itemBoom.Store(&test.fn)
		}
		var reopened Backing[unwindItem]
		returned := ctProbe(t, name, func() {
			b, err := NewBboltFIFO[unwindItem](ctx, root, WithIndex())
			if err != nil {
				return
			}
			reopened = b
		})
		itemBoom.Store(nil)

		switch {
		case !returned && !test.wantErr:
			t.Errorf("%s: the constructor did not return, want it to succeed", name)
			continue
		case returned && test.wantErr:
			t.Errorf("%s: the constructor returned normally, want it to unwind", name)
			continue
		case returned:
			// The success row still owns a handle; hand it to a queue and close it properly so
			// the reopen below is measuring the fix and not this row's own leftovers.
			q, err := New[unwindItem](ctx, "", reopened, 0)
			if err != nil {
				t.Fatalf("%s: New got err == %s, want err == nil", name, err)
			}
			if err := q.Close(ctx); err != nil {
				t.Fatalf("%s: Close got err == %s, want err == nil", name, err)
			}
		}

		if err := ctReopens(t, ctx, root); err != nil {
			t.Errorf("%s: reopening the store got err == %s, want the file lock to have been released", name, err)
		}
	}
}

// TestNewHydrateUnwindClosesEverything covers the same hole one level up. New's hydrate runs four
// bodies of caller code on the caller's goroutine — Backup.RangeAll, Backup.OnLoad, the item codec
// and Item.Hash — and only the error return closed the backing and the Backup. An unwind out of any
// of them leaked both: the bolt file lock, and the caller's Backup, which nothing else will ever
// close because New is the only thing that knows a Backup was handed over and no queue is coming
// back.
//
// Both assertions matter and neither implies the other. A Backup that is never closed is the
// caller's resource leak; a bolt handle that is never closed is the whole store, unusable until the
// process exits.
func TestNewHydrateUnwindClosesEverything(t *testing.T) {
	tests := []struct {
		name string
		// fn is what the Backup's OnLoad does. nil means it returns normally.
		fn func()
		// wantErr is true when New is expected not to return normally.
		wantErr bool
	}{
		{
			// The anchor: a queue that does exist owns its Backup, so New must NOT have closed it.
			name: "Success: a Backup that hydrates cleanly is left open for the queue that owns it",
		},
		{
			name:    "Error: a Backup whose OnLoad panics is closed and the bolt file lock released",
			fn:      func() { panic("OnLoad exploded") },
			wantErr: true,
		},
		{
			name:    "Error: a Backup whose OnLoad calls runtime.Goexit is closed and the bolt file lock released",
			fn:      func() { runtime.Goexit() },
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestNewHydrateUnwindClosesEverything(" + test.name + ")"
		root := diskRoot(t)

		bu := &ctBackup{onLoadHook: test.fn}
		for _, v := range []int{1, 2, 3} {
			bu.items = append(bu.items, fifoItem(v))
		}
		b, err := NewBboltFIFO[Number[int]](ctx, root)
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}

		var q *Queue[Number[int]]
		returned := ctProbe(t, name, func() {
			got, err := New[Number[int]](ctx, "", b, 0, WithBackup(bu))
			if err != nil {
				return
			}
			q = got
		})

		switch {
		case !returned && !test.wantErr:
			t.Errorf("%s: New did not return, want it to succeed", name)
			continue
		case returned && test.wantErr:
			t.Errorf("%s: New returned normally, want it to unwind", name)
			continue
		case returned:
			if bu.closed {
				t.Errorf("%s: New closed the Backup of a queue it went on to return", name)
			}
			if err := q.Close(ctx); err != nil {
				t.Fatalf("%s: Close got err == %s, want err == nil", name, err)
			}
		default:
			if !bu.closed {
				t.Errorf("%s: the Backup was never closed, and New returned no queue to close it with", name)
			}
		}

		if err := ctReopens(t, ctx, root); err != nil {
			t.Errorf("%s: reopening the store got err == %s, want the file lock to have been released", name, err)
		}
	}
}

// TestBTreeWidth pins WithBTreeWidth's own documented range. The default used to be patched in
// after the options ran, which cannot tell "the caller never asked" from "the caller asked for 0",
// so WithBTreeWidth(0) was silently promoted to 32 while 1 and -1 were correctly rejected. The
// default is seeded before the options now, and the range check is unconditional.
//
// The third maker is the variant that had the same hole by a different route: NewBTreeFIFO without
// WithIndex returns the positional btype tree before newBTreeBacking's range check is ever reached,
// so every out-of-range width was accepted there while the identical call with WithIndex was
// refused. The range is checked in the option's own closure now, so all three agree.
func TestBTreeWidth(t *testing.T) {
	tests := []struct {
		name    string
		width   int
		wantErr bool
	}{
		{name: "Success: the minimum legal width is accepted", width: 2},
		{name: "Success: a typical width is accepted", width: 64},
		{name: "Error: a width of 0 is out of range, not a request for the default", width: 0, wantErr: true},
		{name: "Error: a width of 1 is below the minimum", width: 1, wantErr: true},
		{name: "Error: a negative width is below the minimum", width: -1, wantErr: true},
	}

	makers := []struct {
		name string
		make func(w int) (Backing[Number[int]], error)
	}{
		{"NewBTreeFIFO+index", func(w int) (Backing[Number[int]], error) {
			return NewBTreeFIFO[Number[int]](WithIndex(), WithBTreeWidth(w))
		}},
		{"NewBTreeFIFO", func(w int) (Backing[Number[int]], error) {
			return NewBTreeFIFO[Number[int]](WithBTreeWidth(w))
		}},
		{"NewBTreePriority", func(w int) (Backing[Number[int]], error) {
			return NewBTreePriority[Number[int]](WithBTreeWidth(w))
		}},
	}

	for _, mk := range makers {
		for _, test := range tests {
			name := "TestBTreeWidth(" + mk.name + "/" + test.name + ")"
			_, err := mk.make(test.width)
			switch {
			case err == nil && test.wantErr:
				t.Errorf("%s: got err == nil, want err != nil", name)
				continue
			case err != nil && !test.wantErr:
				t.Errorf("%s: got err == %s, want err == nil", name, err)
				continue
			case err != nil:
				if !errors.Is(err, ErrBadOption) {
					t.Errorf("%s: got err == %v, want it tagged ErrBadOption", name, err)
				}
			}
		}
	}

	// The default has to survive being seeded early, or "fixed the promotion" would mean
	// "broke every caller who never set a width".
	if _, err := NewBTreePriority[Number[int]](); err != nil {
		t.Errorf("TestBTreeWidth(Success: no WithBTreeWidth still gets the default): got err == %s, want err == nil", err)
	}
}

// ctErrMetric reads the queue.operation.errors counter out of reader, returning the total count and
// the queue.error_kind attribute of the recorded points. Reading the real instrument rather than a
// stub is the point: the bug was that the counter was never incremented at all, and a test that
// asserted on anything other than the exported metric could not have seen it.
func ctErrMetric(t *testing.T, ctx context.Context, name string, reader *sdkmetric.ManualReader) (int64, string) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("%s: collecting metrics got err == %s, want err == nil", name, err)
	}
	var total int64
	kind := ""
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "queue.operation.errors" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: queue.operation.errors is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
				if v, found := dp.Attributes.Value(attribute.Key("queue.error_kind")); found {
					kind = v.Emit()
				}
			}
		}
	}
	return total, kind
}

// TestOperationErrorMetrics covers what telemetry says about a failed operation, which is two
// separate defects with one fix.
//
// The counter used to decide on the deferred *errp alone. Every queue operation runs caller code
// that can end it without returning, and on that path the deferred call still runs with *errp nil —
// so an operation killed by a panicking side effect recorded zero errors and ended its span with OK
// status. The queue reported perfect health while failing every call. Goexit is a row of its own
// because it is what defeats a recover()-based fix.
//
// The counter also had no blame attribute, so "the caller's Backup is failing every Push" and "the
// queue itself failed" were the same data point — the exact distinction the five caller-code
// sentinels exist to draw, discarded at the one place an operator would look at it. The kind comes
// from a closed set of constants, never from error text, so cardinality stays bounded.
func TestOperationErrorMetrics(t *testing.T) {
	tests := []struct {
		name string
		// sideEffect, when non-nil, is passed as the Push's WithSideEffect func.
		sideEffect func() error
		// backupErr, when set, is what the Backup's Push returns.
		backupErr error
		// batch is how many items the Push carries. The default of 1 is always legal; a value
		// over the max batch is how a row makes the queue itself fail.
		batch    int
		wantErrs int64
		wantKind string
	}{
		{
			// The anchor: without it every row below would still agree if the counter had been
			// wired to increment unconditionally.
			name:     "Success: a Push that succeeds records no error at all",
			wantErrs: 0,
		},
		{
			name:     "Error: a side effect that returns an error is counted and blamed on the side effect",
			wantErrs: 1,
			wantKind: errKindSideEffect,
			sideEffect: func() error {
				return ctFail
			},
		},
		{
			name:       "Error: a side effect that panics is counted, not recorded as a success",
			sideEffect: func() error { panic("side effect exploded") },
			wantErrs:   1,
			wantKind:   errKindUnwind,
		},
		{
			name:       "Error: a side effect that calls runtime.Goexit is counted, not recorded as a success",
			sideEffect: func() error { runtime.Goexit(); return nil },
			wantErrs:   1,
			wantKind:   errKindUnwind,
		},
		{
			name:      "Error: a Backup that fails is counted and blamed on the Backup",
			backupErr: ctFail,
			wantErrs:  1,
			wantKind:  errKindBackup,
		},
		{
			name:     "Error: a batch over the maximum is counted and blamed on the queue",
			batch:    64,
			wantErrs: 1,
			wantKind: errKindQueue,
		},
	}

	for _, test := range tests {
		name := "TestOperationErrorMetrics(" + test.name + ")"
		reader := sdkmetric.NewManualReader()
		ctx := context.SetMeterProvider(t.Context(), sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

		b, err := NewFIFO[Number[int]]()
		if err != nil {
			t.Fatalf("%s: NewFIFO got err == %s, want err == nil", name, err)
		}
		bu := &ctBackup{pushErr: test.backupErr}
		q, err := New[Number[int]](ctx, "contract", b, 0, WithMaxBatch(8), WithBackup(bu))
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}

		batch := test.batch
		if batch == 0 {
			batch = 1
		}
		items := make([]Number[int], 0, batch)
		for i := 0; i < batch; i++ {
			items = append(items, fifoItem(i))
		}
		var opts []OpOption
		if test.sideEffect != nil {
			opts = append(opts, WithSideEffect(test.sideEffect))
		}
		ctProbe(t, name, func() { q.Push(ctx, items, opts...) })

		gotErrs, gotKind := ctErrMetric(t, ctx, name, reader)
		switch {
		case gotErrs != test.wantErrs:
			t.Errorf("%s: queue.operation.errors == %d, want %d", name, gotErrs, test.wantErrs)
		case gotKind != test.wantKind:
			t.Errorf("%s: queue.error_kind == %q, want %q", name, gotKind, test.wantKind)
		}
	}
}

// TestSentinelsArePermanent pins which sentinels stop a retry. The house pattern for a durable
// write is to wrap the call in github.com/gostdlib/base/retry/exponential, and none of these were
// marked, so a caller who pushed a batch larger than the maximum — or onto a closed queue, or with
// a nil hook — retried a programmer error until the budget ran out.
//
// The retryable rows are not filler. Marking ErrEmpty permanent would break every consumer that
// retries a Pop, which is the ordinary way to use this package, so the two that a retry can
// legitimately clear are asserted to have stayed retryable.
func TestSentinelsArePermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		// wantPermanent is whether errors.Is(err, exponential.ErrPermanent) must hold.
		wantPermanent bool
	}{
		{name: "Success: an empty queue is a state a retry can clear", err: ErrEmpty},
		{name: "Success: an incomplete shutdown is retryable, a later Close tries again", err: ErrShutdownIncomplete},
		{name: "Error: a closed queue never reopens", err: ErrClosed, wantPermanent: true},
		{name: "Error: a shutdown already in progress will not be re-run by a retry", err: ErrShutdownInProgress, wantPermanent: true},
		{name: "Error: an over-sized batch is over-sized on every attempt", err: ErrBatchTooLarge, wantPermanent: true},
		{name: "Error: a missing priority is a property of the item, not of the moment", err: ErrPriorityRequired, wantPermanent: true},
		{name: "Error: a forbidden priority is a property of the item, not of the moment", err: ErrPriorityNotAllowed, wantPermanent: true},
		{name: "Error: a missing codec is a construction mistake", err: ErrCodecRequired, wantPermanent: true},
		{name: "Error: an unsupported WithOnAdmit is a construction mistake", err: ErrOnAdmitUnsupported, wantPermanent: true},
		{name: "Error: WithOnAdmit on the wrong operation is a call-site mistake", err: ErrOnAdmitNotPush, wantPermanent: true},
		{name: "Error: a nil WithOnAdmit func is a call-site mistake", err: ErrNilOnAdmit, wantPermanent: true},
		{name: "Error: a nil WithSideEffect func is a call-site mistake", err: ErrNilSideEffect, wantPermanent: true},
		{name: "Error: a misused option is a call-site mistake", err: ErrBadOption, wantPermanent: true},
	}

	for _, test := range tests {
		name := "TestSentinelsArePermanent(" + test.name + ")"
		got := errors.Is(test.err, exponential.ErrPermanent)
		if got != test.wantPermanent {
			t.Errorf("%s: errors.Is(%v, exponential.ErrPermanent) == %t, want %t", name, test.err, got, test.wantPermanent)
		}
		// A sentinel that stopped matching itself would be a far worse regression than the one
		// this test exists for, and wrapping is exactly how that happens.
		if !errors.Is(test.err, test.err) {
			t.Errorf("%s: the sentinel no longer matches itself", name)
		}
	}
}

// TestBadOption pins the single question "did I configure this wrong?". The option-misuse errors
// were anonymous strings, and "max batch must be at least 1" existed as six separate errors.New
// values across six backings — six competing identities for one semantic error that no errors.Is
// could unify. They are all tagged ErrBadOption now, including the named option sentinels, which
// are built on top of it.
func TestBadOption(t *testing.T) {
	tests := []struct {
		name string
		// call performs the misuse and returns whatever it produced.
		call    func(t *testing.T, ctx context.Context) error
		wantErr bool
	}{
		{
			// The anchor: every row below would still pass if New had simply started failing.
			name: "Success: a correctly configured queue reports no option error",
			call: func(t *testing.T, ctx context.Context) error {
				b, err := NewBTreeFIFO[Number[int]](WithIndex(), WithBTreeWidth(16))
				if err != nil {
					return err
				}
				_, err = New[Number[int]](ctx, "", b, 0, WithMaxBatch(10))
				return err
			},
		},
		{
			name: "Error: WithMaxBatch below the minimum",
			call: func(t *testing.T, ctx context.Context) error {
				b, err := NewBTreeFIFO[Number[int]](WithIndex(), WithBTreeWidth(16))
				if err != nil {
					return err
				}
				_, err = New[Number[int]](ctx, "", b, 0, WithMaxBatch(0))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: WithBackup given something that is not a Backup for the item type",
			call: func(t *testing.T, ctx context.Context) error {
				b, err := NewBTreeFIFO[Number[int]](WithIndex(), WithBTreeWidth(16))
				if err != nil {
					return err
				}
				_, err = New[Number[int]](ctx, "", b, 0, WithBackup("not a backup"))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: a nil backing",
			call: func(t *testing.T, ctx context.Context) error {
				_, err := New[Number[int]](ctx, "", nil, 0)
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: WithBTreeWidth below the minimum",
			call: func(t *testing.T, ctx context.Context) error {
				_, err := NewBTreeFIFO[Number[int]](WithIndex(), WithBTreeWidth(1))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: WithBTreeWidth on a constructor that has no B-Tree",
			call: func(t *testing.T, ctx context.Context) error {
				_, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t), WithBTreeWidth(16))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: a bbolt-only option on an in-memory constructor",
			call: func(t *testing.T, ctx context.Context) error {
				_, err := NewBTreeFIFO[Number[int]](WithIndex(), WithNoSync())
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: a nil WithSideEffect func",
			call: func(t *testing.T, ctx context.Context) error {
				b, err := NewFIFO[Number[int]]()
				if err != nil {
					return err
				}
				q, err := New[Number[int]](ctx, "", b, 0)
				if err != nil {
					return err
				}
				_, err = q.Push(ctx, []Number[int]{fifoItem(1)}, WithSideEffect(nil))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: WithOnAdmit on an operation that has no admission step",
			call: func(t *testing.T, ctx context.Context) error {
				b, err := NewFIFO[Number[int]]()
				if err != nil {
					return err
				}
				q, err := New[Number[int]](ctx, "", b, 0)
				if err != nil {
					return err
				}
				if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
					return err
				}
				_, err = q.Pop(ctx, 1, WithOnAdmit(func() error { return nil }))
				return err
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestBadOption(" + test.name + ")"
		err := test.call(t, ctx)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("%s: got err == nil, want err != nil", name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("%s: got err == %s, want err == nil", name, err)
			continue
		case err != nil:
			if !errors.Is(err, ErrBadOption) {
				t.Errorf("%s: got err == %v, want it tagged ErrBadOption", name, err)
			}
		}
	}
}

// TestCloseDBFailureIsReportedAsIncompleteShutdown covers the one shutdown failure a caller can do
// nothing about. Releasing the bolt handle happens in a defer that runs after the performer's
// return expression has been evaluated, with complete and shutComplete already true, so no later
// Close retries it. The error reached the caller joined with the rest, as a bare bolt error, and
// nothing in it distinguished "the store is released" from "the file lock may still be held".
func TestCloseDBFailureIsReportedAsIncompleteShutdown(t *testing.T) {
	dbFail := errors.New("bolt handle refused to close")

	tests := []struct {
		name string
		// closeErr, when set, is what the dbClose seam reports instead of the real close's answer.
		closeErr error
		wantErr  bool
	}{
		{
			// The anchor: a shutdown that works has to keep working, and has to stay silent.
			name: "Success: a handle that releases cleanly reports nothing",
		},
		{
			name:     "Error: a handle that will not release is reported as an incomplete shutdown",
			closeErr: dbFail,
			wantErr:  true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestCloseDBFailureIsReportedAsIncompleteShutdown(" + test.name + ")"
		b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		closeErr := test.closeErr
		b.(*bbolt.Backing[Number[int]]).Hooks.DBClose = func(closeIt func() error) error {
			// The handle is still released — the seam replaces the answer, not the work.
			_ = closeIt()
			return closeErr
		}
		q, err := New[Number[int]](ctx, "", b, 0)
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}

		err = q.Close(ctx)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("%s: Close got err == nil, want err != nil", name)
		case err != nil && !test.wantErr:
			t.Errorf("%s: Close got err == %s, want err == nil", name, err)
		case err != nil:
			switch {
			case !errors.Is(err, ErrShutdownIncomplete):
				t.Errorf("%s: Close got err == %v, want it tagged ErrShutdownIncomplete", name, err)
			case !errors.Is(err, dbFail):
				t.Errorf("%s: Close got err == %v, want the underlying failure still reachable", name, err)
			}
		}
	}
}
