package queue

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/bbolt"
	"github.com/gostdlib/datastructures/queue/internal/backings/btype"
)

// sideEffectUnwind names a way a caller's side effect can leave without returning a value. Both
// skip the rest of the function, so any lock the operation released explicitly on its return paths
// is carried off by the unwind unless something else hands it back.
type sideEffectUnwind struct {
	name string
	fn   func() error
}

func sideEffectUnwinds() []sideEffectUnwind {
	return []sideEffectUnwind{
		{name: "normal", fn: func() error { return nil }},
		{name: "panic", fn: func() error { panic("side effect exploded") }},
		{name: "Goexit", fn: func() error { runtime.Goexit(); return nil }},
	}
}

// TestSideEffectUnwindReleasesLock pins that a side effect leaving abnormally cannot wedge the
// queue. WithSideEffect runs under the queue's lock by design, so a panic or runtime.Goexit out of
// it unwinds a frame that still holds that lock — and Push, Pop, NotEmpty, NotFull and Close
// release it explicitly on each return path rather than through a defer, so nothing would hand it
// back. Every later operation would then block forever.
//
// Two details make this test able to see the failure at all. Liveness is probed with a *writer*: a
// leaked read lock (NotEmpty and NotFull hold one) still admits readers, so a Len-based check
// reports a wedged queue as healthy. And every operation is covered, not only the five that were
// broken, so a future refactor that drops a defer from one of the safe four is caught here too.
func TestSideEffectUnwindReleasesLock(t *testing.T) {
	tests := []struct {
		name string
		call func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error)
	}{
		{"Push", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Push(ctx, []Number[int]{m.item(5)}, WithSideEffect(se))
		}},
		// An empty batch is answered by Queue.Push itself and never reaches a backing, so it runs
		// the side effect under the shared queue lock on a code path of its own. That is exactly
		// how it escaped the sweep that guarded every backing.
		{"PushEmpty", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Push(ctx, nil, WithSideEffect(se))
		}},
		{"Pop", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Pop(ctx, 1, WithSideEffect(se))
		}},
		{"Peek", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Peek(ctx, WithSideEffect(se))
		}},
		{"Exists", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Exists(ctx, queryItem(1), WithSideEffect(se))
		}},
		{"Del", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Del(ctx, []Number[int]{queryItem(1)}, WithSideEffect(se))
		}},
		{"NotEmpty", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.NotEmpty(ctx, WithSideEffect(se))
		}},
		{"NotFull", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.NotFull(ctx, WithSideEffect(se))
		}},
		{"Clear", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Clear(ctx, WithSideEffect(se))
		}},
		{"Close", func(ctx context.Context, q *Queue[Number[int]], m qMaker, se func() error) {
			q.Close(ctx, WithSideEffect(se))
		}},
	}

	for _, m := range queueMakers() {
		for _, unwind := range sideEffectUnwinds() {
			for _, test := range tests {
				// The on-disk backing runs Clear's side effect on its flusher goroutine, not the
				// caller's, so a panic there cannot be recovered by anyone and would take the test
				// binary down with it. That is a real and separate problem with that one path; it
				// is out of this test's reach, not out of scope by choice.
				if test.name == "Clear" && strings.Contains(m.name, "bbolt") {
					continue
				}

				ctx := t.Context()
				q := m.make(t, ctx, 10)
				name := "TestSideEffectUnwindReleasesLock(" + m.name + "/" + unwind.name + "/" + test.name + ")"
				if _, err := q.Push(ctx, []Number[int]{m.item(1)}); err != nil {
					t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
				}

				// Recording that the side effect ran is what stops this test passing vacuously:
				// an operation that silently dropped WithSideEffect would return normally, the
				// probe below would find the queue healthy, and the row would go green having
				// tested nothing at all.
				ran := false
				se := func() error {
					ran = true
					return unwind.fn()
				}

				done := make(chan struct{})
				go func() {
					// Swallow the panic; the point is what the queue looks like afterwards.
					defer func() { recover(); close(done) }()
					test.call(ctx, q, m, se)
				}()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatalf("%s: the operation never returned", name)
				}
				if !ran {
					t.Errorf("%s: the side effect never ran, so this case proves nothing", name)
				}

				// A writer, deliberately: a leaked read lock still lets readers through, so a
				// Len-based check would call a wedged queue healthy. The probe runs in its own
				// goroutine against a timer rather than a context deadline, because a queue that
				// lost its lock blocks inside lk.lock(), which is a plain mutex acquire and does
				// not watch the context — a ctx-based probe would hang here, not fail.
				alive := make(chan error, 1)
				go func() {
					_, err := q.Push(ctx, []Number[int]{m.item(2)})
					alive <- err
				}()
				select {
				case err := <-alive:
					// Checked, not discarded: a Push that returns early without ever reaching
					// lk.lock() would otherwise read as a healthy queue.
					if err != nil && !errors.Is(err, ErrClosed) {
						t.Errorf("%s: probe Push got err == %s, want nil or ErrClosed", name, err)
					}
				case <-time.After(3 * time.Second):
					// Reported without closing: Close parks on the very lock that was lost, and
					// the test would then die of the package timeout with the diagnosis unflushed.
					t.Errorf("%s: queue wedged, the lock outlived the side effect", name)
					continue
				}

				// The loop builds a queue per case; the on-disk ones hold a DB handle and a
				// flusher goroutine until something closes them.
				if test.name != "Close" {
					q.Close(ctx)
				}
			}
		}
	}
}

// TestBboltNotFullCountsStagedItems pins that NotFull and Push agree about what "full" means on the
// on-disk backing. Push admits into a staging buffer and counts those staged items against maxSize
// (count+inflight); NotFull testing count alone reports room for an item that the very next Push
// refuses, so a producer waiting on NotFull wakes up and immediately blocks. The success half is
// what shows NotFull still returns when there genuinely is room.
func TestBboltNotFullCountsStagedItems(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	first := true

	ctx := t.Context()
	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("TestBboltNotFullCountsStagedItems: NewBboltFIFO got err == %s, want err == nil", err)
	}
	// Installed before New, which is what starts the flusher goroutine.
	b.(*bbolt.Backing[Number[int]]).Hooks.CommitStart = func() {
		if first {
			first = false
			close(started)
		}
		<-release
	}
	q, err := New[Number[int]](ctx, "test", b, 1) // a bound of exactly one
	if err != nil {
		t.Fatalf("TestBboltNotFullCountsStagedItems: New got err == %s, want err == nil", err)
	}

	pushErr := make(chan error, 1)
	go func() {
		_, e := q.Push(ctx, []Number[int]{fifoItem(1)})
		pushErr <- e
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("TestBboltNotFullCountsStagedItems: the flusher never entered commit")
	}

	// The item is staged but not yet committed: count is 0 and inflight is 1, and the bound is 1.
	// NotFull must not claim there is room, because a Push behind it would be refused.
	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	err = q.NotFull(short)
	cancel()
	if err == nil {
		t.Errorf("TestBboltNotFullCountsStagedItems: NotFull returned while the only slot was staged, want it to block")
	}

	close(release)
	if e := <-pushErr; e != nil {
		t.Fatalf("TestBboltNotFullCountsStagedItems: Push got err == %s, want err == nil", e)
	}

	// Now committed: count is 1, inflight is 0, and the queue is genuinely full — still no room.
	short, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
	err = q.NotFull(short)
	cancel()
	if err == nil {
		t.Errorf("TestBboltNotFullCountsStagedItems: NotFull returned on a full queue, want it to block")
	}

	// Success half: drain the item and the bound really is free again.
	if _, err := q.Pop(ctx, 1); err != nil {
		t.Fatalf("TestBboltNotFullCountsStagedItems: Pop got err == %s, want err == nil", err)
	}
	short, cancel = context.WithTimeout(ctx, 10*time.Second)
	err = q.NotFull(short)
	cancel()
	if err != nil {
		t.Errorf("TestBboltNotFullCountsStagedItems: NotFull got err == %s, want it to return on an empty queue", err)
	}
	if err := q.Close(ctx); err != nil {
		t.Errorf("TestBboltNotFullCountsStagedItems: Close got err == %s, want err == nil", err)
	}
}

// TestBboltClearSideEffectUnwind covers the one side-effect site that does not run on the caller's
// goroutine. bbolt hands Clear's side effect to the queue's flusher, so an unguarded unwind there
// is not the caller's problem to catch: a panic leaves flushLoop, enters the shared worker pool
// that ran it and ends the process, and a runtime.Goexit ends the flusher, after which Clear waits
// forever on a result nobody will send and no later Push ever commits.
//
// A panic must therefore come back as an error with the flusher still running, and a Goexit — which
// cannot be stopped — must close the queue rather than leave it silently dead.
func TestBboltClearSideEffectUnwind(t *testing.T) {
	tests := []struct {
		name string
		fn   func() error
		// wantErr is nil when the Clear is expected to succeed.
		wantErr error
		// stillUsable is whether the queue is expected to keep working afterwards.
		stillUsable bool
	}{
		{
			// Without this the table says nothing: a Clear that always failed would pass every
			// other row.
			name:        "Success: a side effect that returns cleanly clears the queue",
			fn:          func() error { return nil },
			stillUsable: true,
		},
		{
			name:        "Error: a panicking side effect is reported and the flusher survives",
			fn:          func() error { panic("clear side effect exploded") },
			wantErr:     ErrSideEffectFailed,
			stillUsable: true,
		},
		{
			name:    "Error: a Goexit ends the flusher, so the queue closes rather than hanging",
			fn:      func() error { runtime.Goexit(); return nil },
			wantErr: ErrSideEffectFailed,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		backing, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("TestBboltClearSideEffectUnwind(%s): NewBboltFIFO got err == %s, want nil", test.name, err)
		}
		q, err := New[Number[int]](ctx, "test", backing, 10)
		if err != nil {
			t.Fatalf("TestBboltClearSideEffectUnwind(%s): New got err == %s, want nil", test.name, err)
		}
		if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
			t.Fatalf("TestBboltClearSideEffectUnwind(%s): seeding Push got err == %s, want nil", test.name, err)
		}

		// Clear must return at all — that is the first thing the old code failed to do.
		cleared := make(chan error, 1)
		go func() { cleared <- q.Clear(ctx, WithSideEffect(test.fn)) }()
		select {
		case err := <-cleared:
			switch {
			case test.wantErr == nil && err != nil:
				t.Errorf("TestBboltClearSideEffectUnwind(%s): Clear got err == %s, want err == nil", test.name, err)
			case test.wantErr != nil && !errors.Is(err, test.wantErr):
				t.Errorf("TestBboltClearSideEffectUnwind(%s): Clear got err == %v, want %v", test.name, err, test.wantErr)
			case test.wantErr == nil && q.Len() != 0:
				t.Errorf("TestBboltClearSideEffectUnwind(%s): Len == %d after a successful Clear, want 0", test.name, q.Len())
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("TestBboltClearSideEffectUnwind(%s): Clear never returned", test.name)
		}

		// And the queue must not be silently dead: either it still works, or it says it is closed.
		done := make(chan error, 1)
		go func() {
			_, err := q.Push(ctx, []Number[int]{fifoItem(2)})
			done <- err
		}()
		select {
		case err := <-done:
			switch {
			case test.stillUsable && err != nil:
				t.Errorf("TestBboltClearSideEffectUnwind(%s): Push got err == %s, want the flusher to have survived", test.name, err)
			case !test.stillUsable && err == nil:
				t.Errorf("TestBboltClearSideEffectUnwind(%s): Push succeeded, want ErrClosed after the flusher died", test.name)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("TestBboltClearSideEffectUnwind(%s): Push never returned, the queue is silently dead", test.name)
		}
	}
}

// TestBackupUnwindReleasesLock is the sibling of TestSideEffectUnwindReleasesLock for the other
// body of caller-supplied code the queue runs under its lock: the Backup. Push, Pop and Close call
// into it while holding the write lock and release that lock explicitly on each return path, so a
// Backup that panics or calls runtime.Goexit carries the lock off exactly as a side effect would,
// and every later operation blocks forever.
//
// The on-disk backing's Push is deliberately absent: it does not call the Backup on the caller's
// goroutine at all, but from its flusher during commit, which is a different failure and has its
// own test below.
func TestBackupUnwindReleasesLock(t *testing.T) {
	tests := []struct {
		name string
		// arm points the unwind at one Backup method.
		arm func(bu *fakeBackup, fn func() error)
		// call performs the queue operation that reaches that method.
		call func(ctx context.Context, q *Queue[Number[int]], m qMaker)
		// diskSafe is false for operations the on-disk backing routes elsewhere.
		diskSafe bool
	}{
		{
			name:     "Push reaches Backup.Push",
			arm:      func(bu *fakeBackup, fn func() error) { bu.pushHook = fn },
			call:     func(ctx context.Context, q *Queue[Number[int]], m qMaker) { q.Push(ctx, []Number[int]{m.item(5)}) },
			diskSafe: false,
		},
		{
			name:     "Pop reaches Backup.Del",
			arm:      func(bu *fakeBackup, fn func() error) { bu.delHook = fn },
			call:     func(ctx context.Context, q *Queue[Number[int]], m qMaker) { q.Pop(ctx, 1) },
			diskSafe: true,
		},
		{
			name:     "Close reaches Backup.Close",
			arm:      func(bu *fakeBackup, fn func() error) { bu.closeHook = fn },
			call:     func(ctx context.Context, q *Queue[Number[int]], m qMaker) { q.Close(ctx) },
			diskSafe: true,
		},
	}

	for _, m := range queueMakers() {
		onDisk := strings.Contains(m.name, "bbolt")
		for _, unwind := range sideEffectUnwinds() {
			if unwind.name == "normal" {
				continue
			}
			for _, test := range tests {
				if onDisk && !test.diskSafe {
					continue
				}
				ctx := t.Context()
				bu := &fakeBackup{}
				q := m.make(t, ctx, 10, WithBackup(bu))
				shortenShutdownBudgets(t, q)
				name := "TestBackupUnwindReleasesLock(" + m.name + "/" + unwind.name + "/" + test.name + ")"

				if _, err := q.Push(ctx, []Number[int]{m.item(1)}); err != nil {
					t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
				}
				test.arm(bu, unwind.fn)

				done := make(chan struct{})
				go func() {
					defer func() { recover(); close(done) }()
					test.call(ctx, q, m)
				}()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatalf("%s: the operation never returned", name)
				}
				bu.pushHook, bu.delHook, bu.closeHook = nil, nil, nil

				// A writer, and a timer rather than a context: a queue that lost its lock blocks
				// inside lk.lock(), which does not watch the context.
				alive := make(chan struct{})
				go func() {
					q.Push(ctx, []Number[int]{m.item(2)})
					close(alive)
				}()
				select {
				case <-alive:
				case <-time.After(3 * time.Second):
					// Reported without closing: Close parks on the very lock that was lost, and
					// the test would then die of the package timeout with the diagnosis unflushed.
					t.Errorf("%s: queue wedged, the lock outlived the Backup", name)
					continue
				}

				// One queue per cell, and the on-disk ones hold a DB handle and a flusher
				// goroutine until closed.
				if test.name != "Close reaches Backup.Close" {
					q.Close(ctx)
				}
			}
		}
	}
}

// TestBboltBackupUnwindOnFlusher covers the Backup call the on-disk backing does not make on the
// caller's goroutine. Push stages the batch and the flusher commits it, mirroring to the Backup
// there — so a Backup that unwinds does not merely fail one Push: an escaping panic ends the
// process, and either kind leaves every pusher waiting on a flush result that will never be sent
// and the capacity their batch reserved never given back.
//
// A panic must therefore come back as that batch's error with the flusher still running, and a
// Goexit — which cannot be stopped — must close the queue rather than leave it silently dead.
func TestBboltBackupUnwindOnFlusher(t *testing.T) {
	tests := []struct {
		name string
		fn   func() error
		// wantOK is true when the Push is expected to succeed.
		wantOK bool
		// stillUsable is whether the queue is expected to keep working afterwards.
		stillUsable bool
	}{
		{
			// The anchor: without it nothing shows ErrBackupFailed means the Backup failed
			// rather than that this path always fails.
			name:        "Success: a Backup that returns cleanly commits the batch",
			fn:          func() error { return nil },
			wantOK:      true,
			stillUsable: true,
		},
		{
			name:        "Error: a panicking Backup fails the batch and the flusher survives",
			fn:          func() error { panic("backup exploded") },
			stillUsable: true,
		},
		{
			name: "Error: a Goexit ends the flusher, so the queue closes rather than hanging",
			fn:   func() error { runtime.Goexit(); return nil },
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		backing, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("TestBboltBackupUnwindOnFlusher(%s): NewBboltFIFO got err == %s, want nil", test.name, err)
		}
		bu := &fakeBackup{}
		q, err := New[Number[int]](ctx, "test", backing, 10, WithBackup(bu))
		if err != nil {
			t.Fatalf("TestBboltBackupUnwindOnFlusher(%s): New got err == %s, want nil", test.name, err)
		}

		bu.pushHook = test.fn
		pushed := make(chan error, 1)
		go func() {
			_, e := q.Push(ctx, []Number[int]{fifoItem(1)})
			pushed <- e
		}()
		select {
		case err := <-pushed:
			switch {
			case test.wantOK && err != nil:
				t.Errorf("TestBboltBackupUnwindOnFlusher(%s): Push got err == %s, want err == nil", test.name, err)
			case !test.wantOK && !errors.Is(err, ErrBackupFailed):
				t.Errorf("TestBboltBackupUnwindOnFlusher(%s): Push got err == %v, want ErrBackupFailed", test.name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("TestBboltBackupUnwindOnFlusher(%s): Push never returned, the flusher is gone", test.name)
		}
		bu.pushHook = nil

		done := make(chan error, 1)
		go func() {
			_, err := q.Push(ctx, []Number[int]{fifoItem(2)})
			done <- err
		}()
		select {
		case err := <-done:
			switch {
			case test.stillUsable && err != nil:
				t.Errorf("TestBboltBackupUnwindOnFlusher(%s): Push got err == %s, want the flusher to have survived", test.name, err)
			case !test.stillUsable && err == nil:
				t.Errorf("TestBboltBackupUnwindOnFlusher(%s): Push succeeded, want ErrClosed after the flusher died", test.name)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("TestBboltBackupUnwindOnFlusher(%s): Push never returned, the queue is silently dead", test.name)
		}
	}
}

// bboltWithBackup builds an on-disk queue with a fakeBackup and a commitStart hook, so a test can
// hold a batch inside commit and act while it is in flight.
func bboltWithBackup(t *testing.T, ctx context.Context, name string, hook func()) (*Queue[Number[int]], *fakeBackup) {
	t.Helper()

	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	if hook != nil {
		// Installed before New, which is what starts the flusher.
		b.(*bbolt.Backing[Number[int]]).Hooks.CommitStart = hook
	}
	bu := &fakeBackup{}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}
	return q, bu
}

// TestBboltFlusherDeathSettlesBufferedBatch covers the batch that was never in flight. doFlush
// rotates the flush result before committing, so a Push arriving during a commit buffers against a
// fresh one and waits there. If the flusher then dies, closing only the batch it was committing
// leaves that second Push waiting on a channel nobody is left to close — and its wait is
// deliberately not context-cancelable, so it never returns at all.
func TestBboltFlusherDeathSettlesBufferedBatch(t *testing.T) {
	ctx := t.Context()
	name := "TestBboltFlusherDeathSettlesBufferedBatch"

	inCommit := make(chan struct{})
	release := make(chan struct{})
	first := true
	q, bu := bboltWithBackup(t, ctx, name, func() {
		if first {
			first = false
			close(inCommit)
			<-release
		}
	})

	// Batch one enters commit and stops there.
	one := make(chan error, 1)
	go func() {
		_, err := q.Push(ctx, []Number[int]{fifoItem(1)})
		one <- err
	}()
	select {
	case <-inCommit:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal(name + ": the flusher never entered commit")
	}

	// Batch two buffers against the rotated flush result while batch one is stuck.
	two := make(chan error, 1)
	go func() {
		_, err := q.Push(ctx, []Number[int]{fifoItem(2)})
		two <- err
	}()
	time.Sleep(200 * time.Millisecond) // let it reach the buffer

	// Now kill the flusher from inside batch one's commit.
	bu.pushHook = func() error { runtime.Goexit(); return nil }
	close(release)

	for i, ch := range []chan error{one, two} {
		select {
		case err := <-ch:
			if err == nil {
				t.Errorf("%s: Push %d got err == nil, want the flusher's failure", name, i+1)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s: Push %d never returned, its batch was never settled", name, i+1)
		}
	}
}

// TestBboltCloseBackupUnwindWakesWaiters covers what a Close owes its parked waiters when the
// Backup ends the frame without returning. By that point the backing is already marked closed, so
// a Pop parked on notEmpty is entitled to wake and see it — but the broadcasts live after the
// Backup call, and an unwind skips them. Releasing the lock is not enough on its own.
func TestBboltCloseBackupUnwindWakesWaiters(t *testing.T) {
	for _, unwind := range sideEffectUnwinds() {
		if unwind.name == "normal" {
			continue
		}
		ctx := t.Context()
		name := "TestBboltCloseBackupUnwindWakesWaiters(" + unwind.name + ")"
		q, bu := bboltWithBackup(t, ctx, name, nil)
		shortenShutdownBudgets(t, q)

		// Park a consumer on an empty queue.
		popped := make(chan error, 1)
		go func() {
			_, err := q.Pop(ctx, 1)
			popped <- err
		}()
		waitParkedNotEmpty(t, name, q)

		bu.closeHook = unwind.fn
		closed := make(chan struct{})
		go func() {
			defer func() { recover(); close(closed) }()
			q.Close(ctx)
		}()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Close never returned", name)
		}

		select {
		case err := <-popped:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("%s: parked Pop got err == %v, want ErrClosed", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s: parked Pop never woke, though the backing was already closed", name)
		}
	}
}

// TestBboltClearDrainBlamesTheBackup pins which caller the error names. Clear drains the buffered
// pushes before it looks at its own side effect, and that drain runs the Backup. An unwind from the
// drain is therefore not the side effect's fault — and reporting it as one, especially when no side
// effect was supplied at all, sends the caller looking in the wrong place.
//
// Whether the drain is what dies depends on which arm the flusher's select happens to take once it
// finishes the batch it is holding: the queued Clear, or the pending flush of the second push. Both
// are ready, so it is roughly a coin flip and the scenario is run repeatedly — the assertion that
// the side effect is never blamed holds on every attempt either way, and the run is required to
// land on the drain at least once so this cannot quietly stop covering it.
func TestBboltClearDrainBlamesTheBackup(t *testing.T) {
	const attempts = 20
	name := "TestBboltClearDrainBlamesTheBackup"
	sawDrain := false

	for i := 0; i < attempts; i++ {
		ctx := t.Context()
		inCommit := make(chan struct{})
		release := make(chan struct{})
		first := true
		q, bu := bboltWithBackup(t, ctx, name, func() {
			if first {
				first = false
				close(inCommit)
				<-release
			}
		})

		// The second Backup.Push is the one that dies: the first is the batch being held, which
		// has to succeed so that the buffered second batch is what the drain (or the flush) hits.
		calls := 0
		bu.pushHook = func() error {
			calls++
			if calls >= 2 {
				runtime.Goexit()
			}
			return nil
		}

		pushA := make(chan error, 1)
		go func() {
			_, err := q.Push(ctx, []Number[int]{fifoItem(1)})
			pushA <- err
		}()
		select {
		case <-inCommit:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatalf("%s: the flusher never entered commit", name)
		}

		// Buffered behind the held batch, so it is still uncommitted when Clear arrives.
		pushB := make(chan error, 1)
		go func() {
			_, err := q.Push(ctx, []Number[int]{fifoItem(2)})
			pushB <- err
		}()
		time.Sleep(50 * time.Millisecond)

		// No side effect at all, so anything blaming one is plainly wrong.
		cleared := make(chan error, 1)
		go func() { cleared <- q.Clear(ctx) }()
		time.Sleep(50 * time.Millisecond)
		close(release)

		select {
		case err := <-cleared:
			if errors.Is(err, ErrSideEffectFailed) {
				t.Fatalf("%s: Clear blamed the side effect, but none was given: %v", name, err)
			}
			if errors.Is(err, ErrBackupFailed) {
				sawDrain = true
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Clear never returned", name)
		}
		<-pushA
		<-pushB
		if sawDrain {
			break
		}
	}

	if !sawDrain {
		t.Errorf("%s: the drain never died in %d attempts, so the blame path went untested", name, attempts)
	}
}

// TestBboltCloseTearsDownAfterFlusherDeath covers the shutdown a caller cannot see failing. When
// caller code ends the flusher goroutine, the backing marks itself closed — but closing is not the
// same fact as releasing, and the flusher cannot release anything from where it dies. If Close then
// treats "already closed" as "nothing to do" it returns nil having closed neither the Backup nor
// the bolt handle, and the file lock survives for the life of the process: reopening the same
// directory blocks forever, because bolt's default timeout is zero.
func TestBboltCloseTearsDownAfterFlusherDeath(t *testing.T) {
	ctx := t.Context()
	name := "TestBboltCloseTearsDownAfterFlusherDeath"
	root := diskRoot(t)

	b, err := NewBboltFIFO[Number[int]](ctx, root)
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	bu := &fakeBackup{}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}

	// Kill the flusher from inside its commit.
	bu.pushHook = func() error { runtime.Goexit(); return nil }
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Push(ctx, []Number[int]{fifoItem(1)})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: the Push never returned", name)
	}
	bu.pushHook = nil

	if err := q.Close(ctx); err != nil {
		t.Errorf("%s: Close got err == %s, want err == nil", name, err)
	}
	if !bu.closed {
		t.Errorf("%s: the Backup was never closed, though Close reported success", name)
	}

	// The real proof: the bolt handle is released, so the same directory can be opened again.
	reopened := make(chan error, 1)
	go func() {
		// Bounded, so a still-held lock fails rather than leaving this goroutine and its flock
		// attempt alive for the rest of the binary.
		b2, err := NewBboltFIFO[Number[int]](ctx, root, WithBoltTimeout(3*time.Second))
		if err == nil {
			b2.Close(ctx)
		}
		reopened <- err
	}()
	select {
	case err := <-reopened:
		if err != nil {
			t.Errorf("%s: reopening got err == %s, want the file lock to have been released", name, err)
		}
	case <-time.After(10 * time.Second):
		t.Errorf("%s: reopening blocked, so Close left the bolt file locked", name)
	}
}

// TestBboltCloseReleasesDBOnBackupUnwind covers the same resource on the other route to losing it.
// Close runs the caller's Backup.Close, and an unwind there must not take the bolt handle with it:
// the backing is already marked closed by that point, so a retried Close cannot recover it either.
func TestBboltCloseReleasesDBOnBackupUnwind(t *testing.T) {
	for _, unwind := range sideEffectUnwinds() {
		if unwind.name == "normal" {
			continue
		}
		ctx := t.Context()
		name := "TestBboltCloseReleasesDBOnBackupUnwind(" + unwind.name + ")"
		root := diskRoot(t)

		b, err := NewBboltFIFO[Number[int]](ctx, root)
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		bu := &fakeBackup{closeHook: unwind.fn}
		q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		shortenShutdownBudgets(t, q)

		done := make(chan struct{})
		go func() {
			defer func() { recover(); close(done) }()
			q.Close(ctx)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Close never returned", name)
		}

		reopened := make(chan error, 1)
		go func() {
			b2, err := NewBboltFIFO[Number[int]](ctx, root, WithBoltTimeout(3*time.Second))
			if err == nil {
				b2.Close(ctx)
			}
			reopened <- err
		}()
		select {
		case err := <-reopened:
			if err != nil {
				t.Errorf("%s: reopening got err == %s, want the file lock to have been released", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s: reopening blocked, so the unwind took the bolt file lock with it", name)
		}
	}
}

// TestBboltClearBlamesTheCodeThatFailed pins attribution across the three bodies of caller code a
// Clear runs in sequence. Reporting whichever one happened to run most recently is not the same as
// reporting the one that ended the frame — and told a caller who passed no side effect at all that
// their side effect had failed.
func TestBboltClearBlamesTheCodeThatFailed(t *testing.T) {
	tests := []struct {
		name string
		// arm points an unwind at one of the three, leaving the others well-behaved.
		arm        func(bu *fakeBackup) []OpOption
		wantErr    error
		wantNotErr error
	}{
		{
			name: "Success: nothing fails and Clear reports no failure",
			arm:  func(bu *fakeBackup) []OpOption { return nil },
		},
		{
			name: "Error: a panicking side effect is blamed on the side effect",
			arm: func(bu *fakeBackup) []OpOption {
				return []OpOption{WithSideEffect(func() error { panic("side effect exploded") })}
			},
			wantErr:    ErrSideEffectFailed,
			wantNotErr: ErrBackupFailed,
		},
		{
			name: "Error: a panicking Backup.Clear is blamed on the Backup, with no side effect given",
			arm: func(bu *fakeBackup) []OpOption {
				bu.clearHook = func() error { panic("backup clear exploded") }
				return nil
			},
			wantErr:    ErrBackupFailed,
			wantNotErr: ErrSideEffectFailed,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestBboltClearBlamesTheCodeThatFailed(" + test.name + ")"
		b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		bu := &fakeBackup{}
		q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
			t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
		}

		options := test.arm(bu)
		cleared := make(chan error, 1)
		go func() { cleared <- q.Clear(ctx, options...) }()
		select {
		case err := <-cleared:
			switch {
			case test.wantErr == nil && err != nil:
				t.Errorf("%s: Clear got err == %s, want err == nil", name, err)
			case test.wantErr != nil && !errors.Is(err, test.wantErr):
				t.Errorf("%s: Clear got err == %v, want %v", name, err, test.wantErr)
			case test.wantNotErr != nil && errors.Is(err, test.wantNotErr):
				t.Errorf("%s: Clear blamed the wrong caller: %v", name, err)
			case test.wantErr == nil && q.Len() != 0:
				// Otherwise a Clear that returned nil and cleared nothing passes this row.
				t.Errorf("%s: Len == %d after a successful Clear, want 0", name, q.Len())
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Clear never returned", name)
		}
		q.Close(ctx)
	}
}

// TestBackupErrorIsTaggedAsTheBackups pins that a Backup's own error cannot pass itself off as the
// queue's. A Backup is caller-supplied code like a side effect or an admit hook, and those two
// already tag what they return; leaving the third untagged meant a Backup returning ErrClosed made
// an open, healthy queue report that it had closed, with nothing to say where that came from.
//
// The tag is additive: the Backup's own error stays reachable, exactly as it does for the other
// two, so a caller who wants the original still has it. What changes is that ErrBackupFailed is
// there to be asked about first. All three tagged methods are covered — an earlier version drove
// only Push, so two thirds of the sites it claimed were untested — and each is driven once with a
// Backup that succeeds, because a queue that tagged everything, or refused everything, would
// satisfy the failing rows on its own.
func TestBackupErrorIsTaggedAsTheBackups(t *testing.T) {
	tests := []struct {
		name string
		// arm points the failure at one Backup method and returns the operation that reaches it.
		arm func(bu *fakeBackup) func(ctx context.Context, q *Queue[Number[int]], m qMaker) error
		// wantErr says whether the armed Backup fails. When it does, the error must carry both
		// ErrBackupFailed and the Backup's own ErrClosed.
		wantErr bool
	}{
		{
			name: "Success: Push with a Backup that accepts",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					_, err := q.Push(ctx, []Number[int]{m.item(2)})
					return err
				}
			},
		},
		{
			name: "Success: Del with a Backup that accepts",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					_, err := q.Del(ctx, []Number[int]{queryItem(1)})
					return err
				}
			},
		},
		{
			name: "Success: Close with a Backup that accepts",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					return q.Close(ctx)
				}
			},
		},
		{
			name: "Error: Backup.Push, reached by Push",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				bu.pushHook = func() error { return ErrClosed }
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					_, err := q.Push(ctx, []Number[int]{m.item(1)})
					return err
				}
			},
			wantErr: true,
		},
		{
			name: "Error: Backup.Del, reached by Del",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				bu.delHook = func() error { return ErrClosed }
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					_, err := q.Del(ctx, []Number[int]{queryItem(1)})
					return err
				}
			},
			wantErr: true,
		},
		{
			name: "Error: Backup.Close, reached by Close",
			arm: func(bu *fakeBackup) func(context.Context, *Queue[Number[int]], qMaker) error {
				bu.closeHook = func() error { return ErrClosed }
				return func(ctx context.Context, q *Queue[Number[int]], m qMaker) error {
					return q.Close(ctx)
				}
			},
			wantErr: true,
		},
	}

	for _, m := range queueMakers() {
		for _, test := range tests {
			ctx := t.Context()
			bu := &fakeBackup{}
			q := m.make(t, ctx, 10, WithBackup(bu))
			name := "TestBackupErrorIsTaggedAsTheBackups(" + m.name + "/" + test.name + ")"

			// Seed through the Backup before arming it, so Del has something to remove.
			if _, err := q.Push(ctx, []Number[int]{m.item(1)}); err != nil {
				t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
			}

			// A Backup that returns one of the queue's own sentinels is the sharp case.
			op := test.arm(bu)
			err := op(ctx, q, m)
			switch {
			case err == nil && test.wantErr:
				t.Errorf("%s: got err == nil, want the Backup's failure", name)
			case err != nil && !test.wantErr:
				t.Errorf("%s: got err == %s, want err == nil", name, err)
			case err == nil:
			case !errors.Is(err, ErrBackupFailed):
				t.Errorf("%s: got err == %v, want it tagged ErrBackupFailed", name, err)
			case !errors.Is(err, ErrClosed):
				t.Errorf("%s: got err == %v, want the Backup's own error still reachable", name, err)
			}
			bu.pushHook, bu.delHook, bu.closeHook = nil, nil, nil
			q.Close(ctx)
		}
	}
}

// TestBboltCloseIsOnceNotSkip covers what a second Close is entitled to. Shutdown has to happen
// once, but "once" means later callers wait for it and inherit its result — not that they sail past
// a flag the first caller set on its way in. Waving them through let a Close return nil while the
// first was still inside flushGroup.Wait, so a caller was told the store was released while the
// bolt file lock was still held, and let the second caller tear the db down under a live flusher:
// Backup.Close running inside its own in-flight Backup.Push, and an accepted batch dying with
// "database not open".
func TestBboltCloseIsOnceNotSkip(t *testing.T) {
	const closers = 8
	name := "TestBboltCloseIsOnceNotSkip"
	ctx := t.Context()
	root := diskRoot(t)

	b, err := NewBboltFIFO[Number[int]](ctx, root)
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	// A Backup.Close slow enough that every other closer is still waiting while it runs. The
	// reopen below is given a much shorter bolt timeout than this, so "the lock was still held
	// when a Close claimed success" fails fast instead of simply waiting the release out.
	const backupCloseTakes = 2 * time.Second
	var closeCalls atomic.Int64
	bu := &fakeBackup{closeHook: func() error {
		closeCalls.Add(1)
		time.Sleep(backupCloseTakes)
		return nil
	}}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}
	if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
		t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
	}

	errs := make(chan error, closers)
	for i := 0; i < closers; i++ {
		go func() { errs <- q.Close(ctx) }()
	}

	// Take the FIRST Close to come back, and act on it immediately. Waiting for all of them would
	// hide the defect entirely: the caller doing the work finishes last, so by then the release
	// has happened no matter how the others behaved. The claim under test is about whichever
	// caller returns first — it reported success, so the lock has to be gone already.
	select {
	case err := <-errs:
		if err != nil {
			t.Errorf("%s: Close got err == %s, want err == nil", name, err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: no Close returned", name)
	}

	// A bolt timeout so a still-held lock fails rather than blocking the test.
	reopened := make(chan error, 1)
	go func() {
		b2, err := NewBboltFIFO[Number[int]](ctx, root, WithBoltTimeout(200*time.Millisecond))
		if err == nil {
			b2.Close(ctx)
		}
		reopened <- err
	}()
	select {
	case err := <-reopened:
		if err != nil {
			t.Errorf("%s: a Close reported success while the file lock was still held: %s", name, err)
		}
	case <-time.After(20 * time.Second):
		t.Errorf("%s: reopening blocked, so a Close reported success before the release", name)
	}

	for i := 1; i < closers; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("%s: Close got err == %s, want err == nil", name, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("%s: a Close never returned", name)
		}
	}
	if got := closeCalls.Load(); got != 1 {
		t.Errorf("%s: Backup.Close ran %d times, want exactly 1", name, got)
	}
}

// TestBboltShutdownDrainIsBounded covers the drain the flusher runs on its way out. It exists to
// unblock pushers still waiting on their batch, and it reaches the caller's Backup — so the context
// it hands over must be alive, or a Backup that honors ctx fails every one of them, and it must be
// bounded, or a Backup that waits on ctx.Done() hangs Close forever with nothing able to break the
// tie (sync.Group.Wait cannot be cancelled).
//
// The first version of this test asserted neither. It selected on the test's own context rather
// than the drain's — fakeBackup's plain hook is handed no context at all, so it could not have seen
// it — and it let the batch commit before Close, so the drain found an empty buffer and never
// called the Backup. Both halves survived their mutants. This one holds a batch in the buffer until
// Close, reads the context the Backup is actually given, and shortens the bound so the deadline is
// reachable inside the test's own budget.
func TestBboltShutdownDrainIsBounded(t *testing.T) {
	name := "TestBboltShutdownDrainIsBounded"

	ctx := t.Context()
	inCommit := make(chan struct{})
	release := make(chan struct{})
	first := true
	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	bb := b.(*bbolt.Backing[Number[int]])
	// Shortened so the bound is reachable inside this test's own budget, and set here rather than
	// on a package global: the flusher this backing is about to start is the thing that reads it.
	bb.SetDrainBudget(750 * time.Millisecond)
	bb.Hooks.CommitStart = func() {
		if first {
			first = false
			close(inCommit)
			<-release
		}
	}
	// The drain's Backup blocks until its context ends, then reports why. That is the shape
	// anything network-backed has, and it is the shape that hangs an unbounded drain.
	// Only the drain's call is the subject. The first call belongs to the batch already in
	// commit, which captured the live flush context before Close cancelled it — that one seeing
	// a cancelled context is expected, and inspecting it is what made an earlier version of this
	// test fail against correct code.
	var calls atomic.Int64
	drainErr := make(chan error, 4)
	bu := &fakeBackup{pushCtxHook: func(bctx context.Context) error {
		if calls.Add(1) == 1 {
			return nil
		}
		select {
		case <-bctx.Done():
			drainErr <- bctx.Err()
			return bctx.Err()
		case <-time.After(30 * time.Second):
			drainErr <- nil
			return nil
		}
	}}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}

	// Batch one parks the flusher inside commit; batch two buffers behind it, so it is still
	// uncommitted when Close cancels and the shutdown drain is what finally flushes it.
	go func() { q.Push(ctx, []Number[int]{fifoItem(1)}) }()
	select {
	case <-inCommit:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("%s: the flusher never entered commit", name)
	}
	go func() { q.Push(ctx, []Number[int]{fifoItem(2)}) }()
	time.Sleep(100 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- q.Close(ctx) }()
	close(release)

	// What the drain's Backup saw decides both halves: DeadlineExceeded means it was handed a
	// live context that was bounded. Canceled would mean it inherited the cancellation Close
	// triggered — the defect that fails every pusher the drain exists to serve.
	deadline := time.After(20 * time.Second)
	sawDeadline := false
	for !sawDeadline {
		select {
		case err := <-drainErr:
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				sawDeadline = true
			case errors.Is(err, context.Canceled):
				t.Fatalf("%s: the drain's Backup got a cancelled context, so it fails every pusher it exists to unblock", name)
			}
		case <-deadline:
			t.Fatalf("%s: the drain's Backup never reported its context ending, so the bound never fired", name)
		}
	}

	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		t.Errorf("%s: Close never returned, so the drain is unbounded", name)
	}
}

// TestBboltCloseSurvivesReentrantBackup covers caller code that calls Close on the queue it is
// already inside. It is forbidden — the Backup contract says so — but forbidden should cost the
// caller its own operation, not the queue. Running the release inside a sync.Once made this a
// queue-wide deadlock: the re-entrant call parked on the Once's mutex, and so did every unrelated
// Close behind it.
//
// "It came back at all" is not enough to assert, which is what an earlier version of this stopped
// at. Coming back is compatible with the shutdown having been abandoned: the outer Close returning
// ErrShutdownIncomplete, the Backup never closed and the bolt file lock still held would all have
// passed. So the outer Close has to report success, the Backup has to be closed, the handle has to
// be released, and the re-entrant call itself has to be told it failed rather than handed a nil it
// did nothing to earn.
//
// The cost is asserted too. The re-entrant Close cannot do anything but wait out joinBudget — it is
// waiting for a shutdown that is waiting for it — so this test is only observable inside a test
// deadline because shortenShutdownBudgets shortened that budget. With the production budgets it
// still passed, it just took 60 seconds doing it, and a bound nobody can see is not a bound.
func TestBboltCloseSurvivesReentrantBackup(t *testing.T) {
	ctx := t.Context()
	name := "TestBboltCloseSurvivesReentrantBackup"
	const bound = 10 * time.Second

	root := diskRoot(t)
	b, err := NewBboltFIFO[Number[int]](ctx, root)
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	bb := b.(*bbolt.Backing[Number[int]])
	var q *Queue[Number[int]]
	reentrant := make(chan error, 1)
	bu := &fakeBackup{}
	bu.closeHook = func() error {
		// Re-entrant: Close, from inside the Backup.Close that Close is running.
		reentrant <- q.Close(ctx)
		return nil
	}
	q, err = New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}
	shortenShutdownBudgets(t, q)

	// Checked rather than trusted: every wait below is bounded by a constant, and that constant is
	// only generous if the budget really is the shortened one.
	if bb.JoinBudgetFor() > 2*time.Second {
		t.Fatalf("%s: join budget is %s, so the shortened budgets did not take effect and nothing below is observable", name, bb.JoinBudgetFor())
	}

	done := make(chan error, 1)
	go func() { done <- q.Close(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s: Close got err == %s, want err == nil; the caller's mistake cost the shutdown", name, err)
		}
	case <-time.After(bound):
		t.Fatalf("%s: Close never returned; a reentrant Backup.Close deadlocked the queue", name)
	}

	// The re-entrant call is the one that has to pay. It released nothing and cannot be told it
	// did — it gave up on a shutdown that was blocked on its own return.
	select {
	case err := <-reentrant:
		if err == nil {
			t.Errorf("%s: the reentrant Close got err == nil, want the error that says it released nothing", name)
		}
	case <-time.After(bound):
		t.Fatalf("%s: the reentrant Close never returned", name)
	}

	if !bu.closed {
		t.Errorf("%s: the Backup was never closed, though Close reported success", name)
	}

	// An unrelated Close must not have been dragged down with it, and must inherit the same answer.
	other := make(chan error, 1)
	go func() { other <- q.Close(ctx) }()
	select {
	case err := <-other:
		if err != nil {
			t.Errorf("%s: a later, unrelated Close got err == %s, want the same nil the first caller got", name, err)
		}
	case <-time.After(bound):
		t.Errorf("%s: a later, unrelated Close was wedged by the reentrant one", name)
	}

	// The resource, not just the return value: a shutdown that reported success with the bolt
	// handle still held would pass everything above.
	reopened := make(chan error, 1)
	go func() {
		b2, err := NewBboltFIFO[Number[int]](ctx, root, WithBoltTimeout(3*time.Second))
		if err == nil {
			b2.Close(ctx)
		}
		reopened <- err
	}()
	select {
	case err := <-reopened:
		if err != nil {
			t.Errorf("%s: reopening got err == %s, want the file lock to have been released", name, err)
		}
	case <-time.After(bound):
		t.Errorf("%s: reopening blocked, so Close left the bolt file locked", name)
	}
}

// TestBboltCloseReportsPanickingBackupClose covers what late callers are told when the first
// caller's Backup.Close panics. Marking the shutdown done and handing everyone a zero error made
// Close report success with the Backup never closed — the same "told it succeeded" failure the
// shutdown rewrite was meant to end, arriving by a different route.
func TestBboltCloseReportsPanickingBackupClose(t *testing.T) {
	ctx := t.Context()
	name := "TestBboltCloseReportsPanickingBackupClose"

	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	bu := &fakeBackup{closeHook: func() error { panic("backup close exploded") }}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}

	first := make(chan any, 1)
	go func() {
		defer func() { first <- recover() }()
		q.Close(ctx)
	}()
	select {
	case got := <-first:
		if got == nil {
			t.Errorf("%s: the Backup's panic did not reach its own caller", name)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: the first Close never returned", name)
	}

	// The next caller must be told the Backup did not close, not handed a nil.
	second := make(chan error, 1)
	go func() { second <- q.Close(ctx) }()
	select {
	case err := <-second:
		if err == nil {
			t.Errorf("%s: a later Close reported success though Backup.Close never returned", name)
		}
		if bu.closed {
			t.Errorf("%s: fakeBackup recorded a clean close, so this case proves nothing", name)
		}
	case <-time.After(20 * time.Second):
		t.Errorf("%s: a later Close never returned", name)
	}
}

// TestCodecErrorIsTaggedAsTheCodecs pins the third body of caller code. The Backup's errors are
// tagged and a side effect's are tagged; leaving the codec untagged meant a caller could not tell
// "my decoder rejected this row" from "the store is corrupt" — the same ambiguity ErrBackupFailed
// was introduced to remove. Both directions of the codec are covered, because a returned error and
// an unwind are different paths to the same question.
func TestCodecErrorIsTaggedAsTheCodecs(t *testing.T) {
	decodeErr := errors.New("decoder rejected the row")
	encodeErr := errors.New("encoder refused the item")

	tests := []struct {
		name string
		// op runs after one item is already stored, with the codec armed to fail or not.
		op func(ctx context.Context, q *Queue[Number[int]]) error
		// wantErr arms the decoder to fail. The succeeding rows are what show the tagging is a
		// classification and not a blanket one: an operation that reported ErrCodecFailed for
		// everything, or a codec the operation stopped calling, satisfies the failing rows alone.
		wantErr bool
	}{
		{
			name: "Success: a working decoder lets Pop through",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Pop(ctx, 1)
				return err
			},
		},
		{
			name: "Success: a working decoder lets Exists through",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Exists(ctx, queryItem(1))
				return err
			},
		},
		{
			name: "Success: a working decoder lets Del through",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Del(ctx, []Number[int]{queryItem(1)})
				return err
			},
		},
		{
			name: "Error: a decoder failure during Pop is the codec's",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Pop(ctx, 1)
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: a decoder failure during Exists is the codec's",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Exists(ctx, queryItem(1))
				return err
			},
			wantErr: true,
		},
		{
			name: "Error: a decoder failure during Del is the codec's",
			op: func(ctx context.Context, q *Queue[Number[int]]) error {
				_, err := q.Del(ctx, []Number[int]{queryItem(1)})
				return err
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestCodecErrorIsTaggedAsTheCodecs(" + test.name + ")"
		failDecode := false
		b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t),
			WithCodec(
				func(dst *bytes.Buffer, v Number[int]) error { return JSONEncode(dst, v) },
				func(src []byte, dst *Number[int]) error {
					if failDecode {
						return decodeErr
					}
					return JSONDecode(src, dst)
				},
			),
		)
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		q, err := New[Number[int]](ctx, "test", b, 10)
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}
		if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
			t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
		}

		failDecode = test.wantErr
		err = test.op(ctx, q)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("%s: got err == nil, want the codec's failure", name)
		case err != nil && !test.wantErr:
			t.Errorf("%s: got err == %s, want err == nil", name, err)
		case err == nil:
		case !errors.Is(err, ErrCodecFailed):
			t.Errorf("%s: got err == %v, want it tagged ErrCodecFailed", name, err)
		case !errors.Is(err, decodeErr):
			t.Errorf("%s: got err == %v, want the codec's own error still reachable", name, err)
		}
		failDecode = false
		q.Close(ctx)
	}

	// The encode direction, on the path where the codec runs on the flusher.
	ctx := t.Context()
	name := "TestCodecErrorIsTaggedAsTheCodecs(Error: an encoder failure during Push is the codec's)"
	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t),
		WithCodec(
			func(dst *bytes.Buffer, v Number[int]) error { return encodeErr },
			func(src []byte, dst *Number[int]) error { return JSONDecode(src, dst) },
		),
	)
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	q, err := New[Number[int]](ctx, "test", b, 10)
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}
	switch _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); {
	case err == nil:
		t.Errorf("%s: got err == nil, want the codec's failure", name)
	case !errors.Is(err, ErrCodecFailed):
		t.Errorf("%s: got err == %v, want it tagged ErrCodecFailed", name, err)
	}
	q.Close(ctx)
}

// TestFlusherBlamesTheCodecNotTheBackup pins attribution on the two flusher paths that used to name
// the Backup whatever died. Both run on a queue with NO Backup configured, so any mention of one is
// plainly wrong: the batch whose own codec unwound, and the batch buffered behind it, which did not
// fail on its own merits at all and was simply told the Backup had failed.
//
// Getting the second path under test needs a batch actually waiting when the flusher dies. An
// earlier version let the first batch finish before the second died, so nothing was ever collateral
// and the mutation survived — the flusher is held inside the doomed batch's own commit while a
// third push queues up behind it.
func TestFlusherBlamesTheCodecNotTheBackup(t *testing.T) {
	ctx := t.Context()
	name := "TestFlusherBlamesTheCodecNotTheBackup"

	var commits atomic.Int64
	var boom atomic.Bool
	inCommit := make(chan struct{})
	release := make(chan struct{})

	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t),
		WithCodec(
			func(dst *bytes.Buffer, v Number[int]) error {
				if boom.Load() {
					runtime.Goexit()
				}
				return JSONEncode(dst, v)
			},
			func(src []byte, dst *Number[int]) error { return JSONDecode(src, dst) },
		),
	)
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	// Hold the flusher inside the SECOND commit, so a third push can buffer behind the batch that
	// is about to die.
	b.(*bbolt.Backing[Number[int]]).Hooks.CommitStart = func() {
		if commits.Add(1) == 2 {
			close(inCommit)
			<-release
		}
	}
	q, err := New[Number[int]](ctx, "test", b, 10) // no WithBackup at all
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}

	if _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); err != nil {
		t.Fatalf("%s: the first Push got err == %s, want err == nil", name, err)
	}

	doomed := make(chan error, 1)
	go func() { _, e := q.Push(ctx, []Number[int]{fifoItem(2)}); doomed <- e }()
	select {
	case <-inCommit:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("%s: the flusher never entered the second commit", name)
	}

	collateral := make(chan error, 1)
	go func() { _, e := q.Push(ctx, []Number[int]{fifoItem(3)}); collateral <- e }()
	time.Sleep(150 * time.Millisecond) // let it reach the buffer behind the doomed batch

	boom.Store(true)
	close(release)

	for _, c := range []struct {
		what string
		ch   chan error
	}{{"the batch whose codec died", doomed}, {"the batch buffered behind it", collateral}} {
		select {
		case err := <-c.ch:
			switch {
			case err == nil:
				t.Errorf("%s: %s got err == nil, want the flusher's failure", name, c.what)
			case errors.Is(err, ErrBackupFailed):
				t.Errorf("%s: %s blamed a Backup on a queue that has none: %v", name, c.what, err)
			case !errors.Is(err, ErrCodecFailed):
				t.Errorf("%s: %s got err == %v, want it to name the codec", name, c.what, err)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("%s: %s never returned", name, c.what)
		}
	}
}

// shortenShutdownBudgets makes one queue's shutdown waits observable inside a test's own deadline.
// The production values are minutes-scale on purpose; a test asserting that a wait is bounded
// cannot wait that long, which is how an earlier version of the drain test ended up asserting
// nothing.
//
// It takes a queue rather than shortening a package global, and nothing is restored afterwards,
// because the budgets belong to the backing. A global written here would be read by the flusher of
// every queue any other test left running: they wake when their t.Context() is cancelled, which
// happens just before cleanups run, so the restore raced the read with no parallel test in sight.
//
// A queue on any other backing has no such budgets and is left alone, so the maker-driven tests
// can call this for every row without knowing which backing they are on. It must run before
// anything can cancel the flush context — right after the queue is built — since that
// cancellation is what orders this write ahead of the flusher's read.
func shortenShutdownBudgets[T Item[T]](t *testing.T, q *Queue[T]) {
	t.Helper()

	b, ok := q.backing.(*bbolt.Backing[T])
	if !ok {
		return
	}
	b.SetDrainBudget(300 * time.Millisecond)
	b.SetJoinSlack(300 * time.Millisecond)
}

// TestCallerCodeUnderLockNeverLeaksIt covers the two bodies of caller-supplied code that run while
// a queue lock is held and that no other test in this package reaches: the body of the AllCOW
// iterator, and the item codec inside the on-disk Pop. Both sit on paths that release the lock with
// plain statements rather than a defer, so caller code that ends the frame without returning
// carries the lock off and every later operation blocks forever, with no error and no race report.
//
// It is deliberately not a sweep of every such site, and it must not claim to be one. The side
// effect sites are covered operation-by-operation and maker-by-maker by
// TestSideEffectUnwindReleasesLock, the Backup sites by TestBackupUnwindReleasesLock, and the Item
// methods — eleven call sites across three failure modes — by itemlock_test.go. An earlier version
// of this comment described the table below as "a mechanical sweep for every call into a Backup, a
// side effect, a codec, an Item method or an iterator body that sits inside a lock without a
// deferred release". That was false in both directions: there was never an Item-method case here at
// all, and the two side-effect cases there were are each killed on their own by
// TestSideEffectUnwindReleasesLock. The sentence is what made the Item-method gap read as covered
// for several review rounds, which is worse than having had no test.
func TestCallerCodeUnderLockNeverLeaksIt(t *testing.T) {
	tests := []struct {
		name string
		// run reaches one caller-code site. It must call ran at the point the caller's code
		// actually runs: a site the operation quietly stopped reaching would otherwise leave a
		// healthy-looking queue behind and the row would go green having tested nothing. That is
		// not hypothetical — making NotEmpty pass a nil side effect left the previous version of
		// this table passing in 2.5s.
		run func(t *testing.T, ctx context.Context, q *Queue[Number[int]], m qMaker, ran func())
	}{
		{
			// The success row, and not only for the rule that says a table needs one: it is what
			// shows a queue whose AllCOW still works, against a panic row that would look the same
			// if AllCOW had stopped yielding altogether.
			//
			// What it does NOT do is pin AllCOW's explicit releases. Every one of them is shadowed
			// by the deferred release, so this row runs them and passes with all of them deleted —
			// it observes the lock only after the iteration is over, by which point the defer has
			// released it either way. TestRangeAllCOWReleasesLock and
			// TestRangeAllCOWReleasesLockWhenClosed in cow_test.go are what actually kill that
			// mutant, by observing a writer while the iteration is still running. An earlier
			// version of this comment claimed the pinning for this row, which is the same false
			// claim that made the Item-method gap read as covered for several review rounds.
			name: "Success: an iterator body that returns normally during AllCOW",
			run: func(t *testing.T, ctx context.Context, q *Queue[Number[int]], m qMaker, ran func()) {
				for range q.RangeAllCOW(ctx) {
					ran()
				}
			},
		},
		{
			name: "Error: a panicking iterator body during AllCOW",
			run: func(t *testing.T, ctx context.Context, q *Queue[Number[int]], m qMaker, ran func()) {
				for range q.RangeAllCOW(ctx) {
					ran()
					panic("iterator body exploded")
				}
			},
		},
	}

	for _, m := range queueMakers() {
		for _, test := range tests {
			ctx := t.Context()
			q := m.make(t, ctx, 10)
			name := "TestCallerCodeUnderLockNeverLeaksIt(" + m.name + "/" + test.name + ")"
			for i := 1; i <= 3; i++ {
				if _, err := q.Push(ctx, []Number[int]{m.item(i)}); err != nil {
					t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
				}
			}

			// Written on the goroutine below and read after done closes, which orders the two.
			reached := false
			done := make(chan struct{})
			go func() {
				defer func() { recover(); close(done) }()
				test.run(t, ctx, q, m, func() { reached = true })
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: the operation never returned", name)
			}
			if !reached {
				t.Errorf("%s: the caller's code never ran, so this case proves nothing", name)
			}

			// A writer, against a timer: a lost lock produces no error, only silence, and a
			// context deadline cannot interrupt a goroutine parked in lk.lock().
			alive := make(chan struct{})
			go func() {
				q.Push(ctx, []Number[int]{m.item(99)})
				close(alive)
			}()
			select {
			case <-alive:
			case <-time.After(5 * time.Second):
				// Report and move on without closing: Close would park on the very lock that
				// was lost, and the test would die of the package timeout with the diagnosis
				// stuck in an unflushed log instead of naming the site.
				t.Errorf("%s: queue wedged, the caller's panic carried the lock away", name)
				continue
			}
			q.Close(ctx)
		}
	}

	// The codec is caller code too, and reaching it takes a WithCodec the maker matrix cannot
	// supply. Its panic path is what the guard inside Pop exists for, and the normal-return row
	// is what stops that from being the only thing this half asserts: Pop holds the write lock
	// across the decode and unlocks by plain statement on every path, so a Pop that had stopped
	// decoding under the lock — or stopped returning what it decoded — would leave a perfectly
	// healthy queue behind and satisfy the panic row on its own.
	codecCases := []struct {
		name string
		// boom is whether the decoder panics when Pop reaches it.
		boom bool
	}{
		{name: "Success: a decoder that returns normally during Pop"},
		{name: "Error: a panicking decoder during Pop", boom: true},
	}

	for _, priority := range []bool{false, true} {
		kind := "fifo-bbolt"
		mk := NewBboltFIFO[Number[int]]
		if priority {
			kind = "priority-bbolt"
			mk = NewBboltPriority[Number[int]]
		}
		for _, test := range codecCases {
			ctx := t.Context()
			// The kind is interpolated rather than fixed: both iterations reported under one
			// literal, so a failure could not be attributed to either and a mutation printed the
			// identical line twice.
			name := "TestCallerCodeUnderLockNeverLeaksIt(" + kind + "/" + test.name + ")"
			boom := false
			decoded := false
			b, err := mk(ctx, diskRoot(t),
				WithCodec(
					func(dst *bytes.Buffer, v Number[int]) error { return JSONEncode(dst, v) },
					func(src []byte, dst *Number[int]) error {
						decoded = true
						if boom {
							panic("decoder exploded")
						}
						return JSONDecode(src, dst)
					},
				),
			)
			if err != nil {
				t.Fatalf("%s: constructing the backing got err == %s, want err == nil", name, err)
			}
			q, err := New[Number[int]](ctx, "test", b, 10)
			if err != nil {
				t.Fatalf("%s: New got err == %s, want err == nil", name, err)
			}
			item := itemFor(priority, 1)
			if _, err := q.Push(ctx, []Number[int]{item}); err != nil {
				t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
			}

			boom = test.boom
			decoded = false
			var got []Number[int]
			var popErr error
			done := make(chan struct{})
			go func() {
				defer func() { recover(); close(done) }()
				got, popErr = q.Pop(ctx, 1)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s: Pop never returned", name)
			}
			boom = false
			// Same guard as the table above: a Pop that stopped decoding under the lock would
			// leave a perfectly healthy queue and the case would prove nothing.
			if !decoded {
				t.Errorf("%s: the decoder never ran, so this case proves nothing", name)
			}
			if !test.boom {
				switch {
				case popErr != nil:
					t.Errorf("%s: Pop got err == %s, want err == nil", name, popErr)
				case len(got) != 1:
					t.Errorf("%s: Pop returned %d items, want 1", name, len(got))
				case got[0].V != 1:
					t.Errorf("%s: Pop returned item %d, want 1", name, got[0].V)
				}
			}

			alive := make(chan struct{})
			go func() {
				q.Push(ctx, []Number[int]{item})
				close(alive)
			}()
			select {
			case <-alive:
			case <-time.After(5 * time.Second):
				t.Errorf("%s: queue wedged, the decode carried the write lock away", name)
				continue
			}
			q.Close(ctx)
		}
	}
}

// TestBboltShutdownBudgetsContain covers the relationship between the two budgets. The join waits
// for the flusher, and the flusher's last act is the drain — so a join budget that does not contain
// the drain budget fails a shutdown that was going to succeed. When they were both 30s, a drain
// finishing comfortably inside its own limit still blew the join: Close returned
// ErrShutdownIncomplete having released nothing, and an immediate retry succeeded in milliseconds.
//
// The end-to-end half has to make the shutdown drain do real work, and an earlier version did not.
// It pushed one item and closed, so the slow Backup ran during the ordinary commit and the buffer
// was already empty when Close arrived: the drain called the Backup zero times. That version
// asserted nothing about the drain at all — swapping the join's budget for the drain's left it
// green even with the Backup sleeping 390ms of a 400ms budget — and it is the same mistake
// TestBboltShutdownDrainIsBounded's own comment records fixing.
//
// Here the flusher is pinned inside the first batch's commit while a second batch buffers behind
// it, and it is not let go until Close has cancelled the flush context. The drain is therefore what
// commits the second batch, and the flusher cannot stop until two slow Backups have run: longer
// than the drain budget on its own, comfortably shorter than the drain budget plus its slack.
func TestBboltShutdownBudgetsContain(t *testing.T) {
	name := "TestBboltShutdownBudgetsContain"

	// Two commits of backupWork each is what the join has to outlast. Sized so the total lands
	// between the drain budget and the join budget: a join handed only the drain's budget gives
	// up on a flusher that is still working, and the real one does not.
	const backupWork = 250 * time.Millisecond

	ctx := t.Context()
	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	bb := b.(*bbolt.Backing[Number[int]])

	// Asserted on the backing as constructed, before the shortening below replaces the production
	// values: containment is a property of the defaults, not of whatever this test picks.
	if bb.JoinBudgetFor() <= bb.DrainBudgetFor() {
		t.Errorf("%s: join budget %s does not contain the drain budget %s; a drain that uses its own limit will fail a healthy Close",
			name, bb.JoinBudgetFor(), bb.DrainBudgetFor())
	}
	bb.SetDrainBudget(300 * time.Millisecond)
	bb.SetJoinSlack(500 * time.Millisecond)

	inCommit := make(chan struct{})
	release := make(chan struct{})
	first := true
	bb.Hooks.CommitStart = func() {
		if first {
			first = false
			close(inCommit)
			<-release
		}
	}

	// A Backup slow enough to use most of the drain budget, but well inside it. Counted, because
	// "the drain committed a batch" is the whole point of the second half and a drain that ran on
	// an empty buffer would never call this a second time.
	var pushes atomic.Int64
	bu := &fakeBackup{pushHook: func() error {
		pushes.Add(1)
		time.Sleep(backupWork)
		return nil
	}}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}

	// Batch one pins the flusher inside commit; batch two buffers behind it and is still
	// uncommitted when Close cancels, so the shutdown drain is what commits it.
	pushed := make(chan error, 2)
	go func() { _, err := q.Push(ctx, []Number[int]{fifoItem(1)}); pushed <- err }()
	select {
	case <-inCommit:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("%s: the flusher never entered commit", name)
	}
	go func() { _, err := q.Push(ctx, []Number[int]{fifoItem(2)}); pushed <- err }()
	time.Sleep(100 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- q.Close(ctx) }()
	// The barrier that makes the drain the thing under test. Released any earlier, the flusher can
	// finish both batches before the cancel lands and the drain commits nothing — which is exactly
	// how the previous version of this test came to assert nothing. flushCtx is written once at
	// construction; Close only cancels it, so reading it here races nothing.
	select {
	case <-bb.FlushCtx().Done():
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("%s: Close never cancelled the flusher", name)
	}
	close(release)

	select {
	case err := <-done:
		if errors.Is(err, ErrShutdownIncomplete) {
			t.Errorf("%s: Close reported the shutdown incomplete for a drain that finished inside its budget: %v", name, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: Close never returned", name)
	}

	// Both pushes were accepted, and the second one is only ever committed by the drain. Without
	// this the test would pass on a shutdown that abandoned the buffered batch.
	for i := 0; i < 2; i++ {
		select {
		case err := <-pushed:
			if err != nil {
				t.Errorf("%s: a Push the drain owed a result got err == %s, want err == nil", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: a Push never returned", name)
		}
	}
	if n := pushes.Load(); n < 2 {
		t.Errorf("%s: the Backup was pushed %d time(s), so the drain committed nothing and none of the above is about a drain", name, n)
	}
}

// TestBboltShutdownReportsTheSameAnswer covers the contract the whole design exists for: the caller
// that performs the shutdown and the callers that waited for it must be told the same thing.
//
// Which failure is injected is the entire test. The performer used to return its result before the
// deferred db.Close had recorded its error, so it alone saw nil while everyone else saw the
// failure — and only db.Close's error can show that, because it is set by a defer that runs after
// the performer's return expression has already been evaluated. An earlier version injected a
// Backup.Close failure instead, which is set before that return expression: deleting the line in
// the deferred bookkeeping that re-joins the errors left that version green. Both failures are
// injected here, and every caller has to see both.
//
// The other ordering is the caller who arrives once the shutdown is already finished and takes the
// "already complete" path, waiting on nobody. Every caller in the earlier version was concurrent,
// so all four landed on the waiting path and that one returned whatever it liked — nil included.
func TestBboltShutdownReportsTheSameAnswer(t *testing.T) {
	ctx := t.Context()
	name := "TestBboltShutdownReportsTheSameAnswer"
	b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
	}
	backupErr := errors.New("backup close refused")
	dbErr := errors.New("bolt handle refused to close")
	bu := &fakeBackup{closeHook: func() error {
		time.Sleep(150 * time.Millisecond) // hold the token so the others really wait
		return backupErr
	}}
	b.(*bbolt.Backing[Number[int]]).Hooks.DBClose = func(closeIt func() error) error {
		// The handle is still released — the seam replaces the answer, not the work. What the
		// real close reports is not this test's subject; dbErr is, and it is the one error that
		// arrives after the performer has already decided what to return.
		_ = closeIt()
		return dbErr
	}
	q, err := New[Number[int]](ctx, "test", b, 10, WithBackup(bu))
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", name, err)
	}
	shortenShutdownBudgets(t, q)

	const closers = 4
	errs := make(chan error, closers)
	for i := 0; i < closers; i++ {
		go func() { errs <- q.Close(ctx) }()
	}
	got := make([]error, 0, closers+1)
	for i := 0; i < closers; i++ {
		select {
		case err := <-errs:
			got = append(got, err)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: a Close never returned", name)
		}
	}
	// Arrives after the shutdown is over, so it takes the "already complete" path instead of
	// waiting for anyone. It is entitled to the same answer as the four who waited.
	got = append(got, q.Close(ctx))

	for i, err := range got {
		switch {
		case !errors.Is(err, backupErr):
			t.Errorf("%s: Close %d got err == %v, want every caller to see the Backup's failure", name, i, err)
		case !errors.Is(err, dbErr):
			t.Errorf("%s: Close %d got err == %v, want every caller to see the db.Close failure the performer records last", name, i, err)
		}
	}
}

// itemBoom, when set, is what unwindItem.Hash calls instead of returning. Atomic because the
// on-disk backing calls Item.Hash on its flusher goroutine while the test arms it from its own.
var itemBoom atomic.Pointer[func()]

// unwindItem is an Item whose Hash can be made to end the frame without returning. Item methods
// are the one body of caller code a queue cannot be configured without — the type constraint
// requires them, so there is no option to omit and no hook to install — which is why reaching the
// site needs a purpose-built item rather than a fakeBackup-style field.
type unwindItem struct {
	V int
}

func (u unwindItem) Less(o unwindItem) bool  { return u.V < o.V }
func (u unwindItem) Equal(o unwindItem) bool { return u.V == o.V }
func (u unwindItem) Priority() uint64        { return 0 }

func (u unwindItem) Hash() uint64 {
	if f := itemBoom.Load(); f != nil {
		(*f)()
	}
	return uint64(u.V)
}

// TestItemMethodUnwindOnFlusherBlamesTheItem pins the fifth body of caller code. The on-disk
// backing calls Item.Hash on its flusher while committing, and the flusher is not the caller's
// goroutine to lose — so an unwind there has to come back as the batch's error, tagged with
// whichever piece of caller code ran it.
//
// The codec's blame window used to span the whole transaction and was narrowed to the encode call,
// correctly: it also covered bbolt's own code and the caller's Item methods, and a queue with no
// WithCodec was being told its codec had failed. But nothing took over the vacated ground, so an
// Item that panicked came back as "the queue panicked while committing" carrying no sentinel at
// all — the queue wearing the blame for the caller's code, which is the exact failure the other
// four sentinels exist to prevent.
//
// Nothing else is configured on this queue: no Backup, no WithCodec, no side effect and no hook.
// That is what makes the assertions sharp — every other caller-code sentinel is plainly a wrong
// answer, and "the queue" is the only other thing left for it to say.
func TestItemMethodUnwindOnFlusherBlamesTheItem(t *testing.T) {
	tests := []struct {
		name string
		// fn is what Item.Hash does instead of returning. nil means it returns normally.
		fn func()
		// wantErr is true when the Push is expected to fail.
		wantErr bool
		// stillUsable says whether the flusher is expected to survive.
		stillUsable bool
	}{
		{
			// The anchor: without it nothing shows ErrItemFailed means the Item failed rather
			// than that this path always fails.
			name:        "Success: an Item whose Hash returns cleanly commits the batch",
			stillUsable: true,
		},
		{
			name:        "Error: a panicking Item.Hash fails the batch as the Item's failure, not the queue's",
			fn:          func() { panic("Hash exploded") },
			wantErr:     true,
			stillUsable: true,
		},
		{
			name:    "Error: an Item.Hash that ends the flusher goroutine is still the Item's failure",
			fn:      func() { runtime.Goexit() },
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		name := "TestItemMethodUnwindOnFlusherBlamesTheItem(" + test.name + ")"
		// WithIndex is what makes commit call Item.Hash at all.
		b, err := NewBboltFIFO[unwindItem](ctx, diskRoot(t), WithIndex())
		if err != nil {
			t.Fatalf("%s: NewBboltFIFO got err == %s, want err == nil", name, err)
		}
		q, err := New[unwindItem](ctx, "test", b, 10)
		if err != nil {
			t.Fatalf("%s: New got err == %s, want err == nil", name, err)
		}

		if test.fn != nil {
			itemBoom.Store(&test.fn)
		}
		pushed := make(chan error, 1)
		go func() {
			_, e := q.Push(ctx, []unwindItem{{V: 1}})
			pushed <- e
		}()
		var pushErr error
		select {
		case pushErr = <-pushed:
		case <-time.After(10 * time.Second):
			itemBoom.Store(nil)
			t.Fatalf("%s: Push never returned, the flusher is gone", name)
		}
		itemBoom.Store(nil)

		switch {
		case pushErr == nil && test.wantErr:
			t.Errorf("%s: Push got err == nil, want err != nil", name)
		case pushErr != nil && !test.wantErr:
			t.Errorf("%s: Push got err == %s, want err == nil", name, pushErr)
		case pushErr != nil:
			switch {
			case !errors.Is(pushErr, ErrItemFailed):
				t.Errorf("%s: Push got err == %v, want it tagged ErrItemFailed", name, pushErr)
			case errors.Is(pushErr, ErrBackupFailed), errors.Is(pushErr, ErrCodecFailed), errors.Is(pushErr, ErrSideEffectFailed):
				t.Errorf("%s: Push blamed a Backup, codec or side effect this queue does not have: %v", name, pushErr)
			case strings.Contains(pushErr.Error(), "the queue"):
				t.Errorf("%s: Push blamed the queue for the caller's Item: %v", name, pushErr)
			}
		}

		// A panic must leave the flusher running; a Goexit cannot be stopped, so the backing is
		// closed deliberately rather than left silently unable to commit.
		done := make(chan error, 1)
		go func() {
			_, e := q.Push(ctx, []unwindItem{{V: 2}})
			done <- e
		}()
		select {
		case err := <-done:
			switch {
			case test.stillUsable && err != nil:
				t.Errorf("%s: the next Push got err == %s, want the flusher to have survived", name, err)
			case !test.stillUsable && err == nil:
				t.Errorf("%s: the next Push succeeded, want ErrClosed after the flusher died", name)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s: the next Push never returned, the queue is silently dead", name)
		}
		q.Close(ctx)
	}
}

// TestNewClosesTheBackupWhenHydrateFails covers the one path where a queue is never handed back:
// New fails during hydrate, so the caller has nothing to call Close on and whatever the Backup
// still holds open is theirs to leak.
//
// Joining the backing's Close into the returned error was supposed to cover that, and did not.
// Every backing attaches the Backup as the last statement of a successful Hydrate, so on this path
// the backing's own field is still nil and its Close skips the Backup entirely — the join reported
// a Backup close that never happened. The Backup is closed here instead, by the one caller that
// knows a Backup was handed over and no queue is coming back.
//
// The success rows are the other half of the property: a queue that does exist owns its Backup, so
// New must not have closed it and Close must.
func TestNewClosesTheBackupWhenHydrateFails(t *testing.T) {
	built := func(t *testing.T, b Backing[Number[int]], err error) Backing[Number[int]] {
		t.Helper()
		if err != nil {
			t.Fatalf("TestNewClosesTheBackupWhenHydrateFails: building the backing got err == %s, want err == nil", err)
		}
		return b
	}

	// One per Hydrate implementation: the four in-memory backings and the on-disk one. They were
	// each read to confirm the attach-last shape before the fix, and a sixth backing that got it
	// wrong would show up here rather than as a leaked Backup in production.
	backings := []struct {
		name     string
		priority bool
		make     func(t *testing.T, ctx context.Context) Backing[Number[int]]
	}{
		{"fifo-slice", false, func(t *testing.T, ctx context.Context) Backing[Number[int]] {
			b, err := NewFIFO[Number[int]]()
			return built(t, b, err)
		}},
		{"fifo-btype", false, func(t *testing.T, ctx context.Context) Backing[Number[int]] {
			b, err := btype.New[Number[int]]()
			return built(t, b, err)
		}},
		{"fifo-btree", false, func(t *testing.T, ctx context.Context) Backing[Number[int]] {
			b, err := NewBTreeFIFO[Number[int]]()
			return built(t, b, err)
		}},
		{"priority-heap", true, func(t *testing.T, ctx context.Context) Backing[Number[int]] {
			b, err := NewPriority[Number[int]]()
			return built(t, b, err)
		}},
		{"fifo-bbolt", false, func(t *testing.T, ctx context.Context) Backing[Number[int]] {
			b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
			return built(t, b, err)
		}},
	}

	onLoadFailed := errors.New("OnLoad refused the item")

	tests := []struct {
		name string
		// onLoadErr, when set, is what the Backup's OnLoad returns, which aborts the hydrate.
		onLoadErr error
		wantErr   bool
	}{
		{
			name: "Success: a Backup that hydrates cleanly belongs to the queue, not to New",
		},
		{
			name:      "Error: a Backup whose OnLoad fails is closed, because no queue is coming back",
			onLoadErr: onLoadFailed,
			wantErr:   true,
		},
	}

	for _, bk := range backings {
		for _, test := range tests {
			ctx := t.Context()
			name := "TestNewClosesTheBackupWhenHydrateFails(" + bk.name + "/" + test.name + ")"
			fb := &fakeBackup{onLoadErr: test.onLoadErr}
			for _, v := range []int{1, 2, 3} {
				fb.items = append(fb.items, itemFor(bk.priority, v))
			}

			q, err := New[Number[int]](ctx, "test", bk.make(t, ctx), 0, WithBackup(fb))
			switch {
			case err == nil && test.wantErr:
				t.Errorf("%s: New got err == nil, want err != nil", name)
				continue
			case err != nil && !test.wantErr:
				t.Errorf("%s: New got err == %s, want err == nil", name, err)
				continue
			case err != nil:
				if !fb.closed {
					t.Errorf("%s: the Backup was never closed, and New returned no queue to close it with", name)
				}
				continue
			}

			if fb.closed {
				t.Errorf("%s: New closed the Backup of a queue it went on to return", name)
			}
			if err := q.Close(ctx); err != nil {
				t.Errorf("%s: Close got err == %s, want err == nil", name, err)
			}
			if !fb.closed {
				t.Errorf("%s: the Backup was never closed by Close", name)
			}
		}
	}

	// The call is half of it; the error is the other half. A Backup that also failed to close is
	// precisely what the join in New exists to surface, and it has nowhere else to be reported.
	ctx := t.Context()
	name := "TestNewClosesTheBackupWhenHydrateFails(Error: a Backup that also fails to close reports both)"
	closeFailed := errors.New("Backup close refused")
	fb := &fakeBackup{onLoadErr: onLoadFailed, closeHook: func() error { return closeFailed }}
	fb.items = append(fb.items, fifoItem(1))
	b, err := NewFIFO[Number[int]]()
	if err != nil {
		t.Fatalf("%s: NewFIFO got err == %s, want err == nil", name, err)
	}
	switch _, err := New[Number[int]](ctx, "test", b, 0, WithBackup(fb)); {
	case err == nil:
		t.Errorf("%s: New got err == nil, want err != nil", name)
	case !errors.Is(err, onLoadFailed):
		t.Errorf("%s: New got err == %v, want the hydrate failure still reachable", name, err)
	case !errors.Is(err, closeFailed):
		t.Errorf("%s: New got err == %v, want the Backup's close failure reported too", name, err)
	case !errors.Is(err, ErrBackupFailed):
		t.Errorf("%s: New got err == %v, want it tagged ErrBackupFailed", name, err)
	}
}
