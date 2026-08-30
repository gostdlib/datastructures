package queue

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// hookInFlight starts a Push whose WithOnAdmit hook blocks, waits until that hook is running, runs
// probe while the reservation is outstanding and nothing is inserted, then releases the hook and
// returns the Push's error. The outcome is deliberately left to the caller: the tests sharing this
// harness disagree about what the Push should do, and baking one outcome in is what forced the
// others to hand-roll the same scaffolding.
func hookInFlight(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], v Number[int], probe func(), opts ...OpOption) error {
	t.Helper()

	inHook := make(chan struct{})
	release := make(chan struct{})
	pushed := make(chan error, 1)
	hook := func() error { close(inHook); <-release; return nil }
	options := append([]OpOption{WithOnAdmit(hook)}, opts...)
	go func() {
		_, err := q.Push(ctx, []Number[int]{v}, options...)
		pushed <- err
	}()

	select {
	case <-inHook:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: the hook never ran", name)
	}

	probe()

	close(release)
	select {
	case err := <-pushed:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: Push never returned after the hook was released", name)
		return nil
	}
}

// pushBlocked runs a Push that maxSize must refuse. It asserts the deadline expired rather than any
// error at all, so a Push that failed for some unrelated reason does not read as backpressure. What
// it pins is "not admitted within the timeout", which is the observable form of the bound holding.
func pushBlocked(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], vs []Number[int]) {
	t.Helper()

	// The short context is what makes a Push blocked on the bound give up; the outer timer is what
	// distinguishes that from a Push blocked in lk.lock(), which no context can interrupt. Without
	// the timer a wedged queue hangs this helper instead of failing it.
	short, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := q.Push(short, vs)
		done <- err
	}()
	select {
	case err := <-done:
		switch {
		case err == nil:
			t.Errorf("%s: Push was admitted, want it to block on maxSize", name)
		case !errors.Is(err, context.DeadlineExceeded):
			t.Errorf("%s: Push got err == %s, want it to block on maxSize", name, err)
		}
	case <-time.After(10 * time.Second):
		t.Errorf("%s: Push never returned, the queue is wedged rather than bounded", name)
	}
}

// pushAdmitted runs a Push that must be admitted, under a timeout so a queue that wrongly refuses
// fails the test rather than hanging it to the package timeout.
func pushAdmitted(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], vs []Number[int]) {
	t.Helper()

	// A goroutine and a timer, not a context deadline: a Push blocked in lk.lock() is waiting on
	// a plain mutex, which does not watch the context, so a ctx-bounded probe hangs here instead
	// of failing. Everything this helper guards can fail that way.
	done := make(chan error, 1)
	go func() {
		_, err := q.Push(ctx, vs)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: Push got err == %s, want it admitted", name, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Push never returned, the queue is wedged", name)
	}
}

// waitParked blocks until a producer is actually parked on the backing's notFull signal. A test
// pinning a wakeup has to know the producer reached the signal: one that has not parked yet is
// admitted by the free capacity itself and never exercises the wakeup, so the test would pass
// without testing anything.
func waitParked(t *testing.T, name string, q *Queue[Number[int]]) {
	t.Helper()
	waitSignal(t, name, q, false)
}

// waitParkedNotEmpty is the consumer's twin: it blocks until someone is parked waiting for the
// queue to become non-empty. Tests that park a consumer and then close were using a fixed sleep,
// which meant that if the consumer had not parked yet the Close marked the backing closed first,
// the Pop returned ErrClosed off the closed check, and the test went green having never exercised
// the wakeup it exists for.
func waitParkedNotEmpty(t *testing.T, name string, q *Queue[Number[int]]) {
	t.Helper()
	waitSignal(t, name, q, true)
}

// waitSignal blocks until a waiter is parked on the backing's notFull (or notEmpty) signal. The
// on-disk backing is included: both Close sites in the equivalence test run against it too, and a
// helper that fataled there would take the site with it.
func waitSignal(t *testing.T, name string, q *Queue[Number[int]], notEmpty bool) {
	t.Helper()

	// Asked for through core.Signals rather than by type-switching over every backing. The
	// switch this replaced had one identical arm per concrete type, so a backing added later
	// would have fallen through to the default and taken the test with it.
	sig, ok := q.backing.(core.Signals)
	if !ok {
		t.Fatalf("%s: backing %T has no signal to watch", name, q.backing)
	}
	s := sig.NotFullSignal()
	if notEmpty {
		s = sig.NotEmptySignal()
	}

	which := "notFull"
	if notEmpty {
		which = "notEmpty"
	}
	deadline := time.Now().Add(10 * time.Second)
	for !s.HasWaiters() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: nobody ever parked on %s", name, which)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestWithOnAdmit covers the hook's contract on both outcomes: it runs exactly once per admitted
// batch, and an error from it leaves the queue untouched while releasing the capacity it reserved.
// Each case sizes the queue so the batch is the entire bound, which is what makes the follow-up
// Push a real test of the release: a leaked reservation leaves no room and the Push blocks.
func TestWithOnAdmit(t *testing.T) {
	hookErr := errors.New("hook failed")

	const batch = 3

	tests := []struct {
		name    string
		err     error
		wantLen int64
		wantErr bool
	}{
		{
			name:    "Success: the hook runs once for the batch and the batch is queued",
			wantLen: batch,
		},
		{
			name:    "Error: a hook failure queues nothing and hands the capacity back",
			err:     hookErr,
			wantLen: 0,
			wantErr: true,
		},
	}

	for _, m := range memMakers() {
		for _, test := range tests {
			ctx := t.Context()
			q := m.make(t, ctx, batch)
			name := "TestWithOnAdmit(" + m.name + "/" + test.name + ")"

			vs := make([]Number[int], 0, batch)
			for i := 0; i < batch; i++ {
				vs = append(vs, m.item(i+1))
			}

			calls := 0
			ok, err := q.Push(ctx, vs, WithOnAdmit(func() error {
				calls++
				return test.err
			}))
			switch {
			case err == nil && test.wantErr:
				t.Errorf("%s: got err == nil, want err != nil", name)
				continue
			case err != nil && !test.wantErr:
				t.Errorf("%s: got err == %s, want err == nil", name, err)
				continue
			case err != nil && !errors.Is(err, hookErr):
				t.Errorf("%s: got err == %s, want the hook's error", name, err)
				continue
			}
			if ok != (err == nil) {
				t.Errorf("%s: got ok == %v, want %v", name, ok, err == nil)
			}
			// One call for the whole batch, not one per item — hence a batch of more than one.
			if calls != 1 {
				t.Errorf("%s: hook ran %d times, want 1", name, calls)
			}
			if got := q.Len(); got != test.wantLen {
				t.Errorf("%s: Len == %d, want %d", name, got, test.wantLen)
			}
			if !test.wantErr {
				continue
			}
			// The reservation the failed hook held must be gone, not merely unused: the batch is
			// the whole bound, so a leak would leave no room for this and it would block.
			pushAdmitted(t, ctx, name, q, vs)
			if got := q.Len(); got != batch {
				t.Errorf("%s: Len after the follow-up Push == %d, want %d", name, got, batch)
			}
		}
	}
}

// TestWithOnAdmitInFlight covers what must be true while a hook is running with the lock released.
// The three probes share one body because they differ only in what they check at that moment: that
// a reader still gets through (the whole reason the hook runs unlocked rather than in
// WithSideEffect), that the bound is still held by the reservation (dropping the lock must not let
// a concurrent Push overshoot), and that a Close landing mid-hook is reported rather than the batch
// being inserted into a closed queue.
func TestWithOnAdmitInFlight(t *testing.T) {
	tests := []struct {
		name    string
		maxSize int
		probe   func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], m qMaker)
		wantErr error
		wantLen int64
	}{
		{
			name:    "Success: a reader makes progress while the hook runs",
			maxSize: 10,
			probe: func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], m qMaker) {
				read := make(chan int64, 1)
				go func() { read <- q.Len() }()
				select {
				case got := <-read:
					// Nothing is inserted until the hook returns, so the reservation is invisible.
					if got != 0 {
						t.Errorf("%s: Len during the hook == %d, want 0", name, got)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("%s: Len blocked while the hook ran, so the lock was still held", name)
				}
			},
			wantLen: 1,
		},
		{
			name:    "Success: the reservation holds the bound against a concurrent Push",
			maxSize: 1, // exactly one slot, and the in-flight hook has reserved it
			probe: func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], m qMaker) {
				pushBlocked(t, ctx, name, q, []Number[int]{m.item(2)})
			},
			wantLen: 1,
		},
		{
			name:    "Error: a Close during the hook is reported and takes nothing",
			maxSize: 10,
			probe: func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], m qMaker) {
				if err := q.Close(ctx); err != nil {
					t.Fatalf("%s: Close got err == %s, want err == nil", name, err)
				}
			},
			wantErr: ErrClosed,
			wantLen: 0,
		},
	}

	for _, m := range memMakers() {
		for _, test := range tests {
			ctx := t.Context()
			q := m.make(t, ctx, test.maxSize)
			name := "TestWithOnAdmitInFlight(" + m.name + "/" + test.name + ")"

			err := hookInFlight(t, ctx, name, q, m.item(1), func() { test.probe(t, ctx, name, q, m) })
			switch {
			case err == nil && test.wantErr != nil:
				t.Errorf("%s: got err == nil, want err != nil", name)
			case err != nil && test.wantErr == nil:
				t.Errorf("%s: got err == %s, want err == nil", name, err)
			case err != nil && !errors.Is(err, test.wantErr):
				t.Errorf("%s: got err == %v, want %v", name, err, test.wantErr)
			}
			if got := q.Len(); got != test.wantLen {
				t.Errorf("%s: Len == %d, want %d", name, got, test.wantLen)
			}
		}
	}
}

// TestWithOnAdmitReleasesOnLaterFailure is the regression for a missed wakeup. When the hook
// succeeds but the side effect then fails, the reservation is handed back and nothing is inserted —
// so a producer that parked precisely because of that reservation has to be woken. Nothing else in
// that path will wake it, and it would otherwise sit there until an unrelated Pop or Del.
//
// The table is the point: the error return and the two abnormal unwinds all hand the reservation
// back, so all three owe the wakeup. Only the error return paid it until the unwind callback was
// given the same duty, and a queue with room in it kept a producer parked on every backing.
//
// The success row is what stops the three failure rows from agreeing about nothing. Each of them
// asserts that a Push which inserted nothing gives its slot back, so a Push that had stopped
// inserting at all would satisfy every one of them; the success row is the only one where the slot
// is genuinely consumed, and it insists the parked producer stays parked until a Pop frees it.
func TestWithOnAdmitReleasesOnLaterFailure(t *testing.T) {
	sideErr := errors.New("side effect failed")

	tests := []struct {
		name string
		fail func() error
		// admitted marks the row where the side effect returns nil, so the item is inserted and
		// keeps the one slot the queue has.
		admitted bool
	}{
		{name: "Success: an admitted Push keeps its slot, so the producer waits for a Pop", fail: func() error { return nil }, admitted: true},
		{name: "Error: an error return still wakes the parked producer", fail: func() error { return sideErr }},
		{name: "Error: a panic still wakes the parked producer", fail: func() error { panic("side effect exploded") }},
		{name: "Error: a Goexit still wakes the parked producer", fail: func() error { runtime.Goexit(); return nil }},
	}

	for _, m := range memMakers() {
		for _, test := range tests {
			ctx := t.Context()
			q := m.make(t, ctx, 1) // one slot, which the hook reserves
			name := "TestWithOnAdmitReleasesOnLaterFailure(" + m.name + "/" + test.name + ")"

			inHook := make(chan struct{})
			release := make(chan struct{})
			pushDone := make(chan struct{})
			go func() {
				// The unwind kinds never return, so the outcome of this Push is not the subject;
				// whether the producer behind it wakes is.
				defer func() { recover(); close(pushDone) }()
				q.Push(ctx, []Number[int]{m.item(1)},
					WithOnAdmit(func() error { close(inHook); <-release; return nil }),
					WithSideEffect(test.fail))
			}()

			select {
			case <-inHook:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: the hook never ran", name)
			}

			// Park a second producer on the reservation and wait until it has genuinely reached
			// the signal. One that had not parked yet would be admitted by the free capacity and
			// never exercise the wakeup this test exists for.
			parked := make(chan error, 1)
			go func() {
				_, err := q.Push(ctx, []Number[int]{m.item(2)})
				parked <- err
			}()
			waitParked(t, name, q)

			close(release)
			<-pushDone

			if test.admitted {
				// The item landed, so the only slot is spoken for and the producer behind it is
				// entitled to nothing yet. Checking Len is what pins the insert itself: without
				// it a Push that had stopped inserting would look exactly like every failure row.
				if got := q.Len(); got != 1 {
					t.Errorf("%s: Len == %d, want 1; the admitted Push inserted nothing", name, got)
					continue
				}
				select {
				case err := <-parked:
					t.Errorf("%s: the parked Push completed (err == %v) while the only slot was occupied", name, err)
					continue
				case <-time.After(250 * time.Millisecond):
				}
				// On a goroutine against a timer, like every other probe here: a Pop on a queue
				// that turned out to be empty parks on notEmpty, which no deadline in this test
				// reaches, so an unbounded call would hang the row instead of failing it.
				popped := make(chan error, 1)
				go func() {
					_, e := q.Pop(ctx, 1)
					popped <- e
				}()
				select {
				case err := <-popped:
					if err != nil {
						t.Errorf("%s: Pop got err == %s, want err == nil", name, err)
						continue
					}
				case <-time.After(10 * time.Second):
					t.Errorf("%s: Pop never returned though Len reported an item", name)
					continue
				}
			}

			select {
			case err := <-parked:
				if err != nil {
					t.Errorf("%s: parked Push got err == %s, want err == nil", name, err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("%s: parked Push never woke, Len == %d and the reservation was released", name, q.Len())
			}
		}
	}
}

// TestWithOnAdmitUnwind covers both ways a hook frame leaves without returning. The hook is where a
// caller does durable I/O, the most panic-prone thing it does, and runtime.Goexit reaches the same
// place through t.Fatal. Both must give back the reservation and the lock: losing the reservation
// shrinks the bound for the life of the process, and losing the lock wedges the queue outright,
// with nothing in Len or any error to explain either.
//
// The two were separate tests asserting different halves — the panic one checked the reservation,
// the Goexit one the lock. Merging applies both checks to both unwinds, which is why it is worth
// doing: recover() reports a panic and says nothing about a Goexit, so code that asks it the
// question handles exactly one of these.
func TestWithOnAdmitUnwind(t *testing.T) {
	for _, m := range memMakers() {
		for _, unwind := range sideEffectUnwinds() {
			if unwind.name == "normal" {
				continue
			}
			ctx := t.Context()
			q := m.make(t, ctx, 1) // one slot, so a leaked reservation wedges the queue at zero
			name := "TestWithOnAdmitUnwind(" + m.name + "/" + unwind.name + ")"

			outcome := make(chan any, 1)
			go func() {
				defer func() { outcome <- recover() }()
				q.Push(ctx, []Number[int]{m.item(1)}, WithOnAdmit(unwind.fn))
			}()

			select {
			case got := <-outcome:
				// A panic must still reach the caller; a Goexit has nothing to deliver.
				if unwind.name == "panic" && got == nil {
					t.Errorf("%s: the hook's panic did not propagate", name)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: Push never returned", name)
			}

			// One Push proves both: it needs the lock back to run at all, and the slot back to
			// be admitted rather than blocked on a bound that shrank.
			pushAdmitted(t, ctx, name, q, []Number[int]{m.item(2)})
			if got := q.Len(); got != 1 {
				t.Errorf("%s: Len == %d, want 1", name, got)
			}
		}
	}
}

// TestWithOnAdmitOnDisk covers the backing that cannot honor the option. It admits into a staging
// buffer and commits in groups, so it has no moment matching "capacity reserved, nothing written".
// Rejecting is the point: silently ignoring the hook would run the caller's durable write at a
// moment that does not mean what the option promises. The success case is what shows the rejection
// is about the option and not about the queue refusing pushes generally.
func TestWithOnAdmitOnDisk(t *testing.T) {
	tests := []struct {
		name string
		hook bool
		// closeFirst closes the queue before the Push, to pin which error wins.
		closeFirst bool
		wantLen    int64
		wantErr    error
	}{
		{
			name:    "Success: a Push carrying no hook is admitted",
			wantLen: 1,
		},
		{
			name:    "Error: a Push carrying WithOnAdmit is rejected",
			hook:    true,
			wantLen: 0,
			wantErr: ErrOnAdmitUnsupported,
		},
		{
			// Otherwise the error a caller sees for a closed queue would depend on which
			// backing is underneath, and closed is the more useful answer.
			name:       "Error: a closed queue reports ErrClosed, not ErrOnAdmitUnsupported",
			hook:       true,
			closeFirst: true,
			wantLen:    0,
			wantErr:    ErrClosed,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		backing, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("TestWithOnAdmitOnDisk(%s): NewBboltFIFO got err == %s, want err == nil", test.name, err)
		}
		q, err := New[Number[int]](ctx, "test", backing, 10)
		if err != nil {
			t.Fatalf("TestWithOnAdmitOnDisk(%s): New got err == %s, want err == nil", test.name, err)
		}

		if test.closeFirst {
			if err := q.Close(ctx); err != nil {
				t.Fatalf("TestWithOnAdmitOnDisk(%s): Close got err == %s, want err == nil", test.name, err)
			}
		}

		called := false
		var options []OpOption
		if test.hook {
			options = append(options, WithOnAdmit(func() error {
				called = true
				return nil
			}))
		}
		_, err = q.Push(ctx, []Number[int]{fifoItem(1)}, options...)
		switch {
		case err == nil && test.wantErr != nil:
			t.Errorf("TestWithOnAdmitOnDisk(%s): got err == nil, want err != nil", test.name)
		case err != nil && test.wantErr == nil:
			t.Errorf("TestWithOnAdmitOnDisk(%s): got err == %s, want err == nil", test.name, err)
		case err != nil && !errors.Is(err, test.wantErr):
			t.Errorf("TestWithOnAdmitOnDisk(%s): got err == %v, want %v", test.name, err, test.wantErr)
		}
		if called {
			t.Errorf("TestWithOnAdmitOnDisk(%s): the hook ran, want it never called on a backing that cannot honor it", test.name)
		}
		if got := q.Len(); got != test.wantLen {
			t.Errorf("TestWithOnAdmitOnDisk(%s): Len == %d, want %d", test.name, got, test.wantLen)
		}
		if test.closeFirst {
			continue
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestWithOnAdmitOnDisk(%s): Close got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestOpOptionValidation covers the two ways an operation option can be wrong at the call site: a
// nil func, and WithOnAdmit given to an operation that has no admission step. Both used to be
// accepted and silently do nothing, which for WithOnAdmit means dropping the caller's durable
// write. Every operation appears in a success row as well, so a regression that made an operation
// reject options wholesale cannot hide behind the error rows.
func TestOpOptionValidation(t *testing.T) {
	noop := func() error { return nil }

	tests := []struct {
		name    string
		op      func(ctx context.Context, q *Queue[Number[int]]) error
		wantErr error
		wantLen int64
	}{
		{
			name: "Success: WithOnAdmit on Push",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Push(ctx, []Number[int]{fifoItem(2)}, WithOnAdmit(noop))
				return err
			},
			wantLen: 2,
		},
		{
			name: "Success: WithSideEffect on Push",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Push(ctx, []Number[int]{fifoItem(2)}, WithSideEffect(noop))
				return err
			},
			wantLen: 2,
		},
		{
			name: "Success: WithSideEffect on Del",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Del(ctx, []Number[int]{queryItem(1)}, WithSideEffect(noop))
				return err
			},
			wantLen: 0,
		},
		{
			name: "Success: WithSideEffect on Pop",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Pop(ctx, 1, WithSideEffect(noop))
				return err
			},
			wantLen: 0,
		},
		{
			name: "Success: WithSideEffect on Exists",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Exists(ctx, queryItem(1), WithSideEffect(noop))
				return err
			},
			wantLen: 1,
		},
		{
			name: "Error: WithOnAdmit on Del",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Del(ctx, []Number[int]{queryItem(1)}, WithOnAdmit(noop))
				return err
			},
			wantErr: ErrOnAdmitNotPush,
			wantLen: 1,
		},
		{
			name: "Error: WithOnAdmit on Pop",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Pop(ctx, 1, WithOnAdmit(noop))
				return err
			},
			wantErr: ErrOnAdmitNotPush,
			wantLen: 1,
		},
		{
			name: "Error: WithOnAdmit on Exists",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Exists(ctx, queryItem(1), WithOnAdmit(noop))
				return err
			},
			wantErr: ErrOnAdmitNotPush,
			wantLen: 1,
		},
		{
			name: "Error: a nil WithOnAdmit func",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Push(ctx, []Number[int]{fifoItem(2)}, WithOnAdmit(nil))
				return err
			},
			wantErr: ErrNilOnAdmit,
			wantLen: 1,
		},
		{
			name: "Error: a nil WithSideEffect func",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Push(ctx, []Number[int]{fifoItem(2)}, WithSideEffect(nil))
				return err
			},
			wantErr: ErrNilSideEffect,
			wantLen: 1,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		backing, err := NewFIFO[Number[int]]()
		if err != nil {
			t.Fatalf("TestOpOptionValidation(%s): NewFIFO got err == %s, want err == nil", test.name, err)
		}
		q, err := New[Number[int]](ctx, "test", backing, 10)
		if err != nil {
			t.Fatalf("TestOpOptionValidation(%s): New got err == %s, want err == nil", test.name, err)
		}
		if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
			t.Fatalf("TestOpOptionValidation(%s): seeding Push got err == %s, want err == nil", test.name, err)
		}

		err = test.op(ctx, q)
		switch {
		case err == nil && test.wantErr != nil:
			t.Errorf("TestOpOptionValidation(%s): got err == nil, want err != nil", test.name)
		case err != nil && test.wantErr == nil:
			t.Errorf("TestOpOptionValidation(%s): got err == %s, want err == nil", test.name, err)
		case err != nil && !errors.Is(err, test.wantErr):
			t.Errorf("TestOpOptionValidation(%s): got err == %v, want %v", test.name, err, test.wantErr)
		}
		// A rejected option must leave the queue exactly as it was.
		if got := q.Len(); got != test.wantLen {
			t.Errorf("TestOpOptionValidation(%s): Len == %d, want %d", test.name, got, test.wantLen)
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestOpOptionValidation(%s): Close got err == %s, want err == nil", test.name, err)
		}
	}
}
