package fifo

import (
	"testing"

	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// num is a minimal core.Item for this package's tests. The queue package's own Number is not
// reachable from here — queue imports this package, not the other way round.
type num struct {
	V int
	P uint64
}

func (n num) Less(o num) bool  { return n.P < o.P }
func (n num) Equal(o num) bool { return n.V == o.V }
func (n num) Priority() uint64 { return n.P }
func (n num) Hash() uint64     { return uint64(n.V) }

var _ core.Item[num] = num{}

// newBacking returns a Backing at maxSize, already usable: New installs a placeholder lock, so a
// test can drive the backing without going through queue.New.
func newBacking(t *testing.T, maxSize int) *Backing[num] {
	t.Helper()

	b, err := New[num]()
	if err != nil {
		t.Fatalf("%s: New got err == %s, want err == nil", t.Name(), err)
	}
	f := b.(*Backing[num])
	if err := f.SetMaxSize(maxSize); err != nil {
		t.Fatalf("%s: SetMaxSize got err == %s, want err == nil", t.Name(), err)
	}
	return f
}

// TestPushReusesPoppedSpace pins the reason head exists. Pop used to reslice the storage forward,
// which moves the base pointer and abandons the space it just freed; Push then ran out of tail and
// let append allocate a fresh, larger array on very nearly every call. At the shallow depth a small
// bounded FIFO sits at under load that was one allocation per Push, and the array walked through
// memory instead of being reused.
//
// No correctness test can see this. The queue returns exactly the right items either way — the only
// symptom is allocation pressure, so the assertion has to be a count. The remaining allocation is
// Pop's output slice, which is deliberate: it escapes to the caller and only an API change could
// remove it.
func TestPushReusesPoppedSpace(t *testing.T) {
	ctx := t.Context()
	f := newBacking(t, 16)

	// Hoisted so the input slice is not itself measured, and pre-seeded so the queue never empties
	// and Pop never has to block.
	vs := []num{{V: 1}}
	if err := f.Push(ctx, vs); err != nil {
		t.Fatalf("TestPushReusesPoppedSpace: seeding Push got err == %s, want err == nil", err)
	}

	var pushErr, popErr error
	got := testing.AllocsPerRun(1000, func() {
		if err := f.Push(ctx, vs); err != nil {
			pushErr = err
			return
		}
		if _, err := f.Pop(ctx, 1); err != nil {
			popErr = err
		}
	})
	switch {
	case pushErr != nil:
		t.Fatalf("TestPushReusesPoppedSpace: Push got err == %s, want err == nil", pushErr)
	case popErr != nil:
		t.Fatalf("TestPushReusesPoppedSpace: Pop got err == %s, want err == nil", popErr)
	}

	// One for Pop's output slice, none for Push. Before head existed this was two.
	const want = 1
	if got > want {
		t.Errorf("TestPushReusesPoppedSpace: got %v allocs per Push/Pop pair, want <= %d; Push is allocating, so the space Pop freed is not being reused", got, want)
	}
}

// TestStorageDoesNotGrowWithThroughput pins the other half, and they really are two separate
// properties: head alone stops Push reallocating on every call, but nothing then reuses the dead
// prefix, so append keeps extending buf and both len and head climb forever. A queue holding one
// item would end up sitting on an array proportional to every item that had ever passed through
// it. Sliding the live contents down over the dead prefix is what bounds it.
//
// Removing the slide leaves TestPushReusesPoppedSpace green — the reallocations are on a doubling
// schedule and amortize away — so an allocation count cannot see this. The observable is the size
// of the storage after a lot of throughput at a small depth.
func TestStorageDoesNotGrowWithThroughput(t *testing.T) {
	ctx := t.Context()
	const (
		maxSize = 8
		cycles  = 20000
	)
	f := newBacking(t, maxSize)

	vs := []num{{V: 1}}
	for i := 0; i < cycles; i++ {
		if err := f.Push(ctx, vs); err != nil {
			t.Fatalf("TestStorageDoesNotGrowWithThroughput: Push got err == %s, want err == nil", err)
		}
		if _, err := f.Pop(ctx, 1); err != nil {
			t.Fatalf("TestStorageDoesNotGrowWithThroughput: Pop got err == %s, want err == nil", err)
		}
	}

	// Generous: the depth never exceeds one item, so the storage should be a handful of slots.
	// Without the slide it is on the order of cycles, so any bound in this neighbourhood
	// discriminates.
	const bound = 4 * maxSize
	if got := cap(f.buf); got > bound {
		t.Errorf("TestStorageDoesNotGrowWithThroughput: storage grew to cap %d after %d cycles at depth 1, want <= %d; the dead prefix is never being reclaimed", got, cycles, bound)
	}
}

// TestCompactionPreservesContents covers the slide itself. Push reclaims the dead prefix by copying
// the live items down over it, which is the one place in this backing where the contents are moved
// rather than appended to or handed out — a wrong length or a wrong source there would reorder or
// drop items, and it only happens once the queue has both popped and refilled enough to run out of
// tail. The loop below is sized to force many slides.
func TestCompactionPreservesContents(t *testing.T) {
	ctx := t.Context()
	const (
		maxSize = 8
		cycles  = 200
	)
	f := newBacking(t, maxSize)

	// Keep a model of what should be in the queue and compare the drain against it. Push two and
	// pop one per cycle so the queue both grows and drains, which is what drives head forward and
	// then forces the slide.
	var want []int
	next := 0
	for i := 0; i < cycles; i++ {
		for j := 0; j < 2; j++ {
			if len(want) == maxSize {
				break
			}
			next++
			if err := f.Push(ctx, []num{{V: next}}); err != nil {
				t.Fatalf("TestCompactionPreservesContents: Push(%d) got err == %s, want err == nil", next, err)
			}
			want = append(want, next)
		}
		items, err := f.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("TestCompactionPreservesContents: Pop got err == %s, want err == nil", err)
		}
		if items[0].V != want[0] {
			t.Fatalf("TestCompactionPreservesContents: cycle %d popped %d, want %d", i, items[0].V, want[0])
		}
		want = want[1:]
	}

	if got := f.Len(); got != int64(len(want)) {
		t.Fatalf("TestCompactionPreservesContents: Len got %d, want %d", got, len(want))
	}
	for i, w := range want {
		items, err := f.Pop(ctx, 1)
		if err != nil {
			t.Fatalf("TestCompactionPreservesContents: draining Pop got err == %s, want err == nil", err)
		}
		if items[0].V != w {
			t.Errorf("TestCompactionPreservesContents: drain position %d got %d, want %d", i, items[0].V, w)
		}
	}
}

// TestSlideDoesNotPinRemovedItems covers the zeroing the slide owes. Copying the live items down
// leaves duplicates of them above the new length; left there they would keep those items reachable
// through the array long after the queue had handed them back, which is the same leak the explicit
// zeroing in Pop and Clear exists to prevent.
// The shape has to be built deliberately. A push/pop loop at depth one settles at len == cap == 1,
// where buf[len:cap] is empty and the check below iterates nothing — the first version of this test
// did exactly that and stayed green with the clear deleted. The slide only strands duplicates when
// there are several live items to copy down and the append afterwards does not refill the vacated
// region, so both guards below fail loudly rather than let the test go quiet again.
func TestSlideDoesNotPinRemovedItems(t *testing.T) {
	ctx := t.Context()
	f := newBacking(t, 64)

	// Fill until the tail is exhausted, so the next Push after head has moved must slide.
	const seed = 8
	for i := 1; i <= seed; i++ {
		if err := f.Push(ctx, []num{{V: i}}); err != nil {
			t.Fatalf("TestSlideDoesNotPinRemovedItems: Push(%d) got err == %s, want err == nil", i, err)
		}
	}
	if len(f.buf) != cap(f.buf) {
		t.Fatalf("TestSlideDoesNotPinRemovedItems: setup left spare tail (len %d, cap %d), so the Push below will not slide and this test checks nothing", len(f.buf), cap(f.buf))
	}

	// Drive head most of the way up, leaving one live item for the slide to copy down.
	if _, err := f.Pop(ctx, seed-1); err != nil {
		t.Fatalf("TestSlideDoesNotPinRemovedItems: Pop got err == %s, want err == nil", err)
	}
	if err := f.Push(ctx, []num{{V: seed + 1}}); err != nil {
		t.Fatalf("TestSlideDoesNotPinRemovedItems: the sliding Push got err == %s, want err == nil", err)
	}
	if len(f.buf) == cap(f.buf) {
		t.Fatalf("TestSlideDoesNotPinRemovedItems: no spare tail after the slide (len %d, cap %d), so the check below iterates nothing", len(f.buf), cap(f.buf))
	}

	// Every slot outside the live contents must be zero: below head because Pop cleared them,
	// above len(buf) because the slide cleared the duplicates it left behind.
	var zero num
	for i := 0; i < f.head; i++ {
		if f.buf[i] != zero {
			t.Errorf("TestSlideDoesNotPinRemovedItems: buf[%d] below head is %v, want the zero value", i, f.buf[i])
		}
	}
	for i, v := range f.buf[len(f.buf):cap(f.buf)] {
		if v != zero {
			t.Errorf("TestSlideDoesNotPinRemovedItems: buf[%d] past the length is %v, want the zero value; the slide left a duplicate that pins a removed item", len(f.buf)+i, v)
		}
	}
}
