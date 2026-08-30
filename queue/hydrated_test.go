package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

// hydratedQ builds a bounded queue restored from a backup holding restored items, and hands back
// the backup so a caller can drive it (injecting a mirror failure, say). Every in-memory backing is
// covered: the exemption has to decay on each of Pop, Del and Clear, and those live in each backing
// separately, so a decrement missed in one would otherwise go unnoticed.
func hydratedQ(t *testing.T, ctx context.Context, m qMaker, maxSize, restored int) (*Queue[Number[int]], *fakeBackup) {
	t.Helper()

	bu := &fakeBackup{}
	for i := 1; i <= restored; i++ {
		bu.items = append(bu.items, m.item(i))
	}
	return m.make(t, ctx, maxSize, WithBackup(bu)), bu
}

// pushN pushes n items one at a time, each of which must be admitted.
func pushN(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], m qMaker, base, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		pushAdmitted(t, ctx, name, q, []Number[int]{m.item(base + i)})
	}
}

// TestHydrationMaxSizeExemption covers what a restore means for a bounded queue. Hydrate loads
// whatever the backup holds, which can exceed maxSize, and those items were never admitted — they
// already existed. Counting them against the bound would leave a queue that accepted nothing until
// the backlog drained, so a restart with a large backlog would stop accepting new work exactly when
// it is most needed. The exemption also has to decay as the restored entries leave, by whichever
// route they leave, or one restore would disable the limit for the life of the process.
func TestHydrationMaxSizeExemption(t *testing.T) {
	const (
		maxSize  = 3
		restored = 5 // deliberately more than the bound
	)

	drain := func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], bu *fakeBackup) {
		t.Helper()
		if got := pop(t, ctx, name, q, restored); len(got) != restored {
			t.Fatalf("%s: popped %d items, want %d", name, len(got), restored)
		}
	}
	clear := func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], bu *fakeBackup) {
		t.Helper()
		if err := q.Clear(ctx); err != nil {
			t.Fatalf("%s: Clear got err == %s, want err == nil", name, err)
		}
	}

	failedPop := func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], bu *fakeBackup) {
		t.Helper()
		injected := errors.New("backup mirror failed")
		bu.delErr = injected
		defer func() { bu.delErr = nil }()
		// A Pop mirrors to the backup before removing anything and rolls back if that fails, so
		// nothing leaves the queue — and the exemption has to be exactly where it was. Dropping it
		// before the mirror would let a transient backup error permanently understate the bound.
		done := make(chan error, 1)
		go func() { _, err := q.Pop(ctx, restored); done <- err }()
		select {
		case err := <-done:
			if !errors.Is(err, injected) {
				t.Fatalf("%s: Pop got err == %v, want the injected mirror error", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: Pop never returned", name)
		}
	}

	tests := []struct {
		name       string
		remove     func(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]], bu *fakeBackup)
		wantRemain int64
		wantLen    int64
	}{
		{
			name:       "Error: a rolled-back Pop leaves the exemption exactly where it was",
			remove:     failedPop,
			wantRemain: restored,
			wantLen:    restored + maxSize,
		},
		{
			name:       "Success: restored items are exempt so the whole bound is still free",
			wantRemain: restored,
			wantLen:    restored + maxSize,
		},
		{
			name:       "Success: the exemption decays once the restored surplus has drained",
			remove:     drain,
			wantRemain: 0,
			wantLen:    maxSize,
		},
		{
			name:       "Success: Clear takes the exemption with it",
			remove:     clear,
			wantRemain: 0,
			wantLen:    maxSize,
		},
	}

	for _, m := range memMakers() {
		for _, test := range tests {
			ctx := t.Context()
			q, bu := hydratedQ(t, ctx, m, maxSize, restored)
			name := "TestHydrationMaxSizeExemption(" + m.name + "/" + test.name + ")"

			if got := q.Len(); got != restored {
				t.Fatalf("%s: Len after hydrate == %d, want %d", name, got, restored)
			}
			if test.remove != nil {
				test.remove(t, ctx, name, q, bu)
			}
			if got := q.Len(); got != test.wantRemain {
				t.Fatalf("%s: Len after the removal == %d, want %d", name, got, test.wantRemain)
			}

			// Whatever is left, the full bound is available for items that are actually admitted.
			pushN(t, ctx, name, q, m, 100, maxSize)
			if got := q.Len(); got != test.wantLen {
				t.Errorf("%s: Len == %d, want %d", name, got, test.wantLen)
			}
			// And the bound is real: one more must not fit.
			pushBlocked(t, ctx, name, q, []Number[int]{m.item(999)})
		}
	}
}

// TestHydrationCreditFollowsRemovedEntries pins that the exemption tracks *which* entries left, not
// merely how many. Restored items are exempt and admitted ones are not, so removing an admitted item
// has to give its capacity back. Charging that removal to the restored surplus instead leaves the
// bound understated — the queue refuses work it has room for — and crediting it to both leaves the
// bound overstated, so the test checks the bound from both directions.
func TestHydrationCreditFollowsRemovedEntries(t *testing.T) {
	const (
		maxSize  = 2
		restored = 3 // more than the bound, so the surplus is what would absorb the removals
	)

	for _, m := range memMakers() {
		ctx := t.Context()
		q, _ := hydratedQ(t, ctx, m, maxSize, restored)
		name := "TestHydrationCreditFollowsRemovedEntries(" + m.name + ")"

		// Fill the bound with admitted items on top of the restored ones.
		pushN(t, ctx, name, q, m, 100, maxSize)

		// Remove exactly the admitted items; every restored item stays put.
		queries := make([]Number[int], 0, maxSize)
		for i := 0; i < maxSize; i++ {
			queries = append(queries, queryItem(100+i))
		}
		switch n, err := q.Del(ctx, queries); {
		case err != nil:
			t.Fatalf("%s: Del got err == %s, want err == nil", name, err)
		case n != maxSize:
			t.Fatalf("%s: Del removed %d, want %d", name, n, maxSize)
		}
		// The right entries have to remain, not merely the right number of them.
		want := make([]int, 0, restored)
		for i := 1; i <= restored; i++ {
			want = append(want, i)
		}
		if diff := pretty.Compare(want, sortedQueueVals(t, ctx, q)); diff != "" {
			t.Errorf("%s: remaining entries -want +got:\n%s", name, diff)
		}

		// Nothing admitted is left, so the whole bound must be free again...
		pushN(t, ctx, name, q, m, 200, maxSize)
		// ...and no more than the bound, or the removal was credited twice.
		pushBlocked(t, ctx, name, q, []Number[int]{m.item(999)})
	}
}
