package queue

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

// failErr is what a caller's code returns in the error variant of each case.
var failErr = errors.New("caller code failed")

// probeDeadline is how long a probe push waits before it counts as refused, and waiterDeadline is
// how long a parked waiter gets to wake before it counts as still parked. They are the entire cost
// of this test: the matrix is deadlines multiplied by cells and essentially nothing else, so their
// size is what decides whether the file is a few seconds of the suite or half of it.
//
// The floor is real. The on-disk backing's flush interval is 100ms, so a probe push against it can
// legitimately take that long before its batch commits; anything at or under 100ms would start
// calling a healthy queue refused. These sit just above it.
const (
	probeDeadline  = 150 * time.Millisecond
	waiterDeadline = 250 * time.Millisecond
)

// admitsWithParkedConsumer is how many probe pushes a bounded queue accepts while a consumer is
// still parked on it: the bound's worth, plus the one the consumer takes on its way out. Derived
// rather than written down, because a literal one greater than the queue's maxSize reads as a typo
// and invites a future editor to "fix" it.
func admitsWithParkedConsumer(maxSize int) int { return maxSize + 1 }

// onceHook wraps a failure so it fires for the first caller and lets every later one through.
// Arming a fakeBackup hook and then clearing it would be a data race: fakeBackup carries no lock
// because backings call it single-threaded under the queue lock, and a test goroutine writing the
// field is not holding that lock — meanwhile a producer parked inside the queue is reading it.
// Deciding once, inside the hook, keeps the field write-once.
func onceHook(fail func() error) func() error {
	var fired atomic.Bool
	return func() error {
		if fired.Swap(true) {
			return nil
		}
		return fail()
	}
}

// failureMode is one of the ways caller-supplied code can end a queue operation. Returning an error
// is the reference among the three that fail: it is the path the package was written for, the one
// every code review reads, and the one the other two have to match.
type failureMode struct {
	name string
	// succeeds marks the mode where the caller's code returns nil and the operation runs to
	// completion. It is not a failure at all, which is exactly why it is here: without it every
	// row asserts what a broken operation leaves behind and none asserts that a working one still
	// does its job, so a site whose operation had stopped working entirely would still agree with
	// itself across the three failures and pass.
	succeeds bool
	fn       func() error
}

func failureModes() []failureMode {
	return []failureMode{
		{name: "returns nil", succeeds: true, fn: func() error { return nil }},
		{name: "returns an error", fn: func() error { return failErr }},
		{name: "panics", fn: func() error { panic("caller code exploded") }},
		{name: "calls runtime.Goexit", fn: func() error { runtime.Goexit(); return nil }},
	}
}

// queueState is everything a caller can still observe about a queue once an operation has failed.
// Comparing it as a value is the whole point: it turns "did the unwind path forget something?"
// into a diff against the path that did not forget.
type queueState struct {
	// Len is the queue's own count.
	Len int64
	// Responds is false when a later writer never returns at all, which is what a lost lock
	// looks like from outside. It is listed first because everything below is meaningless
	// without it.
	Responds bool
	// Admits is how many further single-item pushes the queue accepts before refusing one. It
	// catches a reservation that was never handed back: the bound silently shrinks.
	Admits int
	// PushErr classifies what a later push reports, so a queue that quietly closed is not
	// mistaken for one that stayed open.
	PushErr string
	// Waiter is what became of a consumer or producer parked before the operation ran. It
	// catches a wakeup the unwind path owed and skipped.
	Waiter string
}

// classify reduces an error to the distinction a caller acts on, so unrelated wording differences
// do not show up as state differences.
func classify(err error) string {
	switch {
	case err == nil:
		return "nil"
	case errors.Is(err, ErrClosed):
		return "closed"
	case errors.Is(err, context.DeadlineExceeded):
		return "refused"
	default:
		return "error"
	}
}

// observe builds a queueState. Every wait here is a goroutine against a timer rather than a context
// deadline, because a queue that lost its lock blocks inside lk.lock(), which no context can
// interrupt — a ctx-bounded probe would hang instead of recording the fact.
func observe(t *testing.T, ctx context.Context, q *Queue[Number[int]], m qMaker, waiter chan error) queueState {
	t.Helper()

	// The parked waiter is settled first, before anything else competes for room. It and the
	// probe pushes below are both trying to get into the same queue, and on a bound of one
	// whichever asks first wins — measuring in a fixed order is what keeps this a test of the
	// queue rather than of the scheduler.
	st := queueState{Waiter: "none"}
	st = finishWaiter(st, waiter)

	// Len takes the read lock, so on a lost WRITE lock it never returns. Reading it directly on
	// this goroutine — which is what the first version did, two lines below a comment explaining
	// why that is wrong — hung the binary to the package timeout on exactly the defect this test
	// exists to record. Same goroutine-and-timer shape as everything else here.
	lenDone := make(chan int64, 1)
	go func() { lenDone <- q.Len() }()
	select {
	case n := <-lenDone:
		st.Len = n
	case <-time.After(5 * time.Second):
		st.Responds = false
		st.PushErr = "wedged"
		return st
	}

	// A short context so a push refused by the bound gives up quickly; the outer timer is what
	// separates "refused" from "never came back".
	for i := 0; i < 8; i++ {
		short, cancel := context.WithTimeout(ctx, probeDeadline)
		got := make(chan error, 1)
		go func() {
			_, err := q.Push(short, []Number[int]{m.item(900 + i)})
			got <- err
		}()
		select {
		case err := <-got:
			cancel()
			st.Responds = true
			st.PushErr = classify(err)
			if err != nil {
				return st
			}
			st.Admits++
		case <-time.After(5 * time.Second):
			cancel()
			st.Responds = false
			st.PushErr = "wedged"
			return st
		}
	}
	return st
}

func finishWaiter(st queueState, waiter chan error) queueState {
	if waiter == nil {
		return st
	}
	select {
	case err := <-waiter:
		st.Waiter = "woke:" + classify(err)
	case <-time.After(waiterDeadline):
		st.Waiter = "parked"
	}
	return st
}

// unwindSite is one place caller-supplied code runs while the queue holds its lock. run performs
// the operation with that code failing in the given way, parks whatever waiter the site can strand,
// and returns that waiter's channel (nil if the site has none).
type unwindSite struct {
	name    string
	maxSize int
	// seed is how many items are in the queue before the operation runs.
	seed int
	// onDisk says whether the on-disk backing reaches this site on the caller's goroutine. Its
	// Push does not — it stages and the flusher commits — so those sites are memory-only here
	// and have their own tests.
	onDisk bool
	// want is the state every failing mode must produce, spelled out rather than borrowed from
	// whichever mode ran first. Comparing the unwinds against the error return alone can only
	// catch a break that is asymmetric between them, and the release callbacks share most of their
	// work with the error path — so the missed-wakeup bug this file was written for was invisible
	// here: break releaseReservation and all three modes agree, and agreement was the whole
	// assertion.
	want queueState
	// wantOK is the state the succeeding mode must produce. It is a different state for most
	// sites, because a side effect that returns nil lets the operation finish: the Pop actually
	// removes an item, the Close actually closes. Asserting it is what stops a site from passing
	// on an operation that no longer works at all.
	wantOK queueState
	run    func(t *testing.T, ctx context.Context, q *Queue[Number[int]], bu *fakeBackup, m qMaker, fail func() error) chan error
}

// pushWithReservation drives a Push whose WithOnAdmit hook holds the queue's only slot while a
// second producer parks on it, then fails the given way. It is the shape that produced two separate
// missed-wakeup bugs: the reservation is handed back, so the parked producer is owed a signal.
func pushWithReservation(armBackup bool) func(*testing.T, context.Context, *Queue[Number[int]], *fakeBackup, qMaker, func() error) chan error {
	return func(t *testing.T, ctx context.Context, q *Queue[Number[int]], bu *fakeBackup, m qMaker, fail func() error) chan error {
		t.Helper()

		inHook := make(chan struct{})
		release := make(chan struct{})
		options := []OpOption{WithOnAdmit(func() error { close(inHook); <-release; return nil })}
		if armBackup {
			bu.pushHook = onceHook(fail)
		} else {
			options = append(options, WithSideEffect(fail))
		}

		done := make(chan struct{})
		go func() {
			defer func() { recover(); close(done) }()
			q.Push(ctx, []Number[int]{m.item(1)}, options...)
		}()
		<-inHook

		parked := make(chan error, 1)
		go func() {
			_, err := q.Push(ctx, []Number[int]{m.item(2)})
			parked <- err
		}()
		waitParked(t, "pushWithReservation", q)

		close(release)
		<-done
		return parked
	}
}

// closeWithParked drives a Close while a waiter is parked on the queue: a consumer on an empty one,
// or — when producer is true — a producer on a full one.
//
// The producer half is why this is one parameterized helper rather than a consumer-only one.
// Nothing else in the package parks a producer across a Close, so the notFull wakeup that Close and
// its unwind path owe a producer had no test anywhere: dropping that Signal from the four in-memory
// backings' closeAndWake changed nothing any test could see. The consumer's wakeup is a different
// signal on a different path and does not stand in for it.
func closeWithParked(armBackup, producer bool) func(*testing.T, context.Context, *Queue[Number[int]], *fakeBackup, qMaker, func() error) chan error {
	return func(t *testing.T, ctx context.Context, q *Queue[Number[int]], bu *fakeBackup, m qMaker, fail func() error) chan error {
		t.Helper()

		parked := make(chan error, 1)
		if producer {
			// The site seeds the queue to its bound, so this Push has nowhere to go and parks on
			// notFull. waitParked is what proves it got there: a producer still on its way to the
			// signal is admitted by the free capacity itself and never exercises the wakeup.
			go func() {
				_, err := q.Push(ctx, []Number[int]{m.item(7)})
				parked <- err
			}()
			waitParked(t, "closeWithParked", q)
		} else {
			go func() {
				_, err := q.Pop(ctx, 1)
				parked <- err
			}()
			waitParkedNotEmpty(t, "closeWithParked", q)
		}

		var options []OpOption
		if armBackup {
			bu.closeHook = onceHook(fail)
		} else {
			options = append(options, WithSideEffect(fail))
		}
		done := make(chan struct{})
		go func() {
			defer func() { recover(); close(done) }()
			q.Close(ctx, options...)
		}()
		<-done
		return parked
	}
}

// plainOp drives an operation with no waiter parked, for the sites whose only debt is the lock.
func plainOp(armBackup bool, call func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption)) func(*testing.T, context.Context, *Queue[Number[int]], *fakeBackup, qMaker, func() error) chan error {
	return func(t *testing.T, ctx context.Context, q *Queue[Number[int]], bu *fakeBackup, m qMaker, fail func() error) chan error {
		t.Helper()

		var options []OpOption
		if armBackup {
			bu.delHook = onceHook(fail)
		} else {
			options = append(options, WithSideEffect(fail))
		}
		done := make(chan struct{})
		go func() {
			defer func() { recover(); close(done) }()
			call(ctx, q, m, options...)
		}()
		<-done
		return nil
	}
}

func unwindSites() []unwindSite {
	// Two states recur across the sites below, so they are named once. seededQueueUntouched is
	// what a bound of four holding two items looks like when the operation changed nothing: the
	// pair stays and the remaining two slots take two probes. closedAfterWakeup is a queue that
	// went through with its close, with whoever was parked told so.
	seededQueueUntouched := queueState{Len: 2, Responds: true, Admits: 2, PushErr: "refused", Waiter: "none"}
	closedAfterWakeup := queueState{Len: 0, Responds: true, Admits: 0, PushErr: "closed", Waiter: "woke:closed"}

	return []unwindSite{
		{
			name:    "WithSideEffect during Push, reservation held, producer parked",
			maxSize: 1,
			// Nothing inserted, the reservation handed back, and the producer parked on it woken
			// and admitted — which fills the single slot, so nothing more fits.
			want: queueState{Len: 1, Responds: true, Admits: 0, PushErr: "refused", Waiter: "woke:nil"},
			// The side effect returning nil lets this Push finish, so its own item fills the slot
			// and the producer parked behind it has no reason to wake at all.
			wantOK: queueState{Len: 1, Responds: true, Admits: 0, PushErr: "refused", Waiter: "parked"},
			run:    pushWithReservation(false),
		},
		{
			name:    "Backup.Push during Push, reservation held, producer parked",
			maxSize: 1,
			want:    queueState{Len: 1, Responds: true, Admits: 0, PushErr: "refused", Waiter: "woke:nil"},
			wantOK:  queueState{Len: 1, Responds: true, Admits: 0, PushErr: "refused", Waiter: "parked"},
			run:     pushWithReservation(true),
		},
		{
			name:    "WithSideEffect during Close, consumer parked",
			maxSize: 4,
			onDisk:  true,
			// A side effect that fails aborts the close, so the queue stays open and the consumer
			// stays parked on an empty queue. It wakes on the first probe push and takes that item,
			// which is why the queue admits one more than its bound.
			want: queueState{Len: 0, Responds: true, Admits: admitsWithParkedConsumer(4), PushErr: "refused", Waiter: "parked"},
			// A side effect that succeeds lets the close through, and the consumer is told so
			// instead of being fed.
			wantOK: closedAfterWakeup,
			run:    closeWithParked(false, false),
		},
		{
			name:    "Backup.Close during Close, consumer parked",
			maxSize: 4,
			onDisk:  true,
			// By the time the Backup runs the queue is already marked closed, so the consumer is
			// owed the wakeup that says so and nothing more is admitted. A Backup.Close that
			// succeeds produces the same thing: its error is reported, never obeyed.
			want:   closedAfterWakeup,
			wantOK: closedAfterWakeup,
			run:    closeWithParked(true, false),
		},
		{
			// The producer twin of the site above, and the only place in the package a producer is
			// parked across a Close. The in-memory backings' unwind path hands back the lock, marks
			// the queue closed and wakes both signals; with nothing parked on notFull, dropping
			// that one Signal was invisible everywhere else.
			name: "Backup.Close during Close, producer parked",
			// A bound of one, seeded full, so the second producer has nowhere to go but the signal.
			maxSize: 1,
			seed:    1,
			// The seeded item stays — the woken producer re-takes the lock, sees the close and
			// leaves rather than inserting — and every later push is refused as closed.
			want:   queueState{Len: 1, Responds: true, Admits: 0, PushErr: "closed", Waiter: "woke:closed"},
			wantOK: queueState{Len: 1, Responds: true, Admits: 0, PushErr: "closed", Waiter: "woke:closed"},
			run:    closeWithParked(true, true),
		},
		{
			// A read-lock site: the release is runlock, not unlock. Nothing here changes state,
			// which is the point — the two unwinds must leave a queue as untouched as the error
			// return does, and must not leave the read lock behind either.
			name:    "WithSideEffect during NotEmpty",
			maxSize: 4,
			want:    seededQueueUntouched,
			wantOK:  seededQueueUntouched,
			seed:    2,
			onDisk:  true,
			run: plainOp(false, func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption) {
				q.NotEmpty(ctx, options...)
			}),
		},
		{
			name:    "WithSideEffect during NotFull",
			maxSize: 4,
			want:    seededQueueUntouched,
			wantOK:  seededQueueUntouched,
			seed:    2,
			onDisk:  true,
			run: plainOp(false, func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption) {
				q.NotFull(ctx, options...)
			}),
		},
		{
			// Queue.Push answers an empty batch itself and never reaches a backing, running the
			// side effect under the shared queue lock on a path of its own. That is exactly how
			// it escaped the sweep that guarded every backing, so it earns a site here.
			name:    "WithSideEffect during an empty-batch Push",
			maxSize: 4,
			want:    seededQueueUntouched,
			wantOK:  seededQueueUntouched,
			seed:    2,
			onDisk:  true,
			run: plainOp(false, func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption) {
				q.Push(ctx, nil, options...)
			}),
		},
		{
			name:    "WithSideEffect during Pop",
			maxSize: 4,
			want:    seededQueueUntouched,
			// A side effect that succeeds lets the Pop take its item, freeing a slot: one fewer
			// left behind, one more admitted.
			wantOK: queueState{Len: 1, Responds: true, Admits: 3, PushErr: "refused", Waiter: "none"},
			seed:   2,
			onDisk: true,
			run: plainOp(false, func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption) {
				q.Pop(ctx, 1, options...)
			}),
		},
		{
			name:    "Backup.Del during Pop",
			maxSize: 4,
			want:    seededQueueUntouched,
			wantOK:  queueState{Len: 1, Responds: true, Admits: 3, PushErr: "refused", Waiter: "none"},
			seed:    2,
			onDisk:  true,
			run: plainOp(true, func(ctx context.Context, q *Queue[Number[int]], m qMaker, options ...OpOption) {
				q.Pop(ctx, 1, options...)
			}),
		},
	}
}

// unwindMakers is the maker matrix reduced to one name per backing implementation. queueMakers
// returns eleven names but they reach only five files: NewBTreeFIFO without WithIndex returns the
// btype tree, so "fifo-btree" and "fifo-btype" are the same backing, and the +index and priority
// variants of the btree and on-disk backings share every release closure the caller code this test
// drives — Backups, side effects and codecs — can reach. Running all eleven cost 95s of a 186s
// suite and killed nothing the five do not.
//
// That is not the same as saying no guarded path branches on index-ness: btree.go's Push and
// bbolt.go's Pop both wrap idx.add/idx.remove in a guard that a non-indexed backing never executes.
// Those two are reached through the caller's Item.Hash, which this test does not arm, and they are
// covered by TestItemMethodLockRelease in itemlock_test.go over its own +index sites. Restoring the
// six dropped makers here would not add them; it would only re-run the same closures.
//
// Selected by name rather than by construction so that a maker being renamed or dropped is a
// failure here instead of a silent hole.
func unwindMakers(t *testing.T) []qMaker {
	t.Helper()

	want := map[string]bool{
		"fifo-slice":       true, // fifo.go
		"fifo-btype":       true, // btype_fifo.go
		"fifo-btree+index": true, // btree.go
		"priority-heap":    true, // heap.go
		"fifo-bbolt":       true, // bbolt.go
	}
	out := make([]qMaker, 0, len(want))
	for _, m := range queueMakers() {
		if want[m.name] {
			out = append(out, m)
		}
	}
	if len(out) != len(want) {
		t.Fatalf("unwindMakers: got %d of the %d named makers; a backing lost its coverage to a rename", len(out), len(want))
	}
	return out
}

// TestUnwindMatchesErrorReturn is the structural guard for the one class of bug this package kept
// producing, one instance at a time. Every place caller-supplied code runs while the queue holds
// its lock has a hand-written release callback for the case where that code never returns, and
// nothing forces the callback to undo everything the ordinary error return undoes. Three separate
// defects came from exactly that omission — a reservation never handed back, a close never
// broadcast, a buffered batch never settled — and every one was found by tripping over it rather
// than by a test.
//
// The rule asserted here needs no per-site knowledge, which is what makes it hold for sites nobody
// has written yet: returning an error is the reference behavior, and a panic or a runtime.Goexit
// out of the same caller code must leave the queue in the same observable condition. Same Len, same
// willingness to accept work, same capacity, same answer for anyone already waiting. When it fails
// it prints the difference, so the omission names itself instead of having to be diagnosed.
//
// A fourth mode returns nil, and it is not a failure at all. Without it every row would describe
// what a broken operation leaves behind and none would describe a working one, so a site whose
// operation had stopped doing its job entirely would still agree with itself across the three
// failures and pass.
func TestUnwindMatchesErrorReturn(t *testing.T) {
	for _, m := range unwindMakers(t) {
		onDisk := strings.Contains(m.name, "bbolt")
		for _, site := range unwindSites() {
			if onDisk && !site.onDisk {
				continue
			}
			for _, mode := range failureModes() {
				ctx := t.Context()
				name := "TestUnwindMatchesErrorReturn(" + m.name + "/" + site.name + "/" + mode.name + ")"
				bu := &fakeBackup{}
				// Without this the failing modes pass vacuously. If the caller's code is never
				// reached — an operation that quietly stops honoring the option, a backing that
				// stops calling the Backup — all three of them agree on a state that means
				// nothing, and "the unwind path is correct" is indistinguishable from "there was
				// no unwind". Recorded before the call because two of the three never come back
				// from it.
				var fired atomic.Bool
				fail := func() error {
					fired.Store(true)
					return mode.fn()
				}
				q := m.make(t, ctx, site.maxSize, WithBackup(bu))
				// The Close sites drive a Backup.Close that never returns; without this each of
				// those cells waits out the production budget.
				shortenShutdownBudgets(t, q)
				for i := 0; i < site.seed; i++ {
					if _, err := q.Push(ctx, []Number[int]{m.item(i + 1)}); err != nil {
						t.Fatalf("%s: seeding Push got err == %s, want err == nil", name, err)
					}
				}

				waiter := site.run(t, ctx, q, bu, m, fail)
				if !fired.Load() {
					t.Errorf("%s: the caller's code never ran, so this case proves nothing", name)
					continue
				}
				got := observe(t, ctx, q, m, waiter)

				q.Close(ctx)
				// Compared against the site's declared expectation, not against whichever mode
				// ran first. Every failing mode has to produce it, so a break the modes share —
				// which is most of the release callback — shows up instead of cancelling out.
				// The succeeding mode has its own expectation because it is not a failure: the
				// operation runs to completion, which for most sites is a different state.
				want := site.want
				if mode.succeeds {
					want = site.wantOK
				}
				if diff := pretty.Compare(want, got); diff != "" {
					t.Errorf("%s: wrong state after the operation failed.\n-want +got:\n%s", name, diff)
				}
			}
		}
	}
}
