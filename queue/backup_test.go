package queue

import (
	"context"
	"errors"
	"iter"
	"os"
	"sort"
	"testing"

	"github.com/gostdlib/datastructures/queue/internal/backings/bbolt"
	"github.com/gostdlib/datastructures/queue/internal/backings/btype"

	"github.com/kylelemons/godebug/pretty"
)

// fakeBackup is an in-memory Backup[Number[int]] used to exercise hydrate-restore and the
// mirror-on-mutation path. Backings call the backup synchronously under the queue lock, so
// the tests are single-threaded and no internal locking is needed.
type fakeBackup struct {
	items       []Number[int]
	onLoad      []int // values passed to OnLoad during hydrate, in order
	onLoadCalls int   // number of times OnLoad was invoked (contract: exactly once)
	closed      bool
	pushErr     error // when set, Push fails (to test backup-failure abort)
	delErr      error // when set, Del fails (to test the mirror-failure rollback on Pop/Del)
	restoreErr  error // when set, Restore fails (to test the both-failed compensating path)
	// pushHook/delHook/closeHook, when set, run at the top of that method and their error is
	// returned. They are how the tests make a Backup fail in each of the three ways it can:
	// returning an error, panicking, or ending the goroutine with runtime.Goexit. One hook type
	// covers all three so a harness can hold the failure mode as data.
	// pushCtxHook, when set, takes precedence over pushHook and receives the context the queue
	// handed the Backup. It is the only way to observe that context, which is what a test of the
	// shutdown drain's bound has to assert on.
	pushCtxHook func(context.Context) error
	pushHook    func() error
	delHook     func() error
	closeHook   func() error
	clearHook   func() error
	onLoadErr   error // when set, OnLoad fails (to test hydrate-abort on OnLoad error)
}

// compile-time check that fakeBackup implements the full Backup interface.
var _ Backup[Number[int]] = (*fakeBackup)(nil)

func (b *fakeBackup) Push(ctx context.Context, vs []Number[int]) error {
	if b.pushCtxHook != nil {
		if err := b.pushCtxHook(ctx); err != nil {
			return err
		}
	} else if b.pushHook != nil {
		if err := b.pushHook(); err != nil {
			return err
		}
	}
	if b.pushErr != nil {
		return b.pushErr
	}
	b.items = append(b.items, vs...)
	return nil
}

// Del removes exactly one matching (Equal) occurrence per element of vs — the contract
// the backings rely on to mirror the precise items popped/deleted.
func (b *fakeBackup) Del(ctx context.Context, vs []Number[int]) error {
	if b.delHook != nil {
		if err := b.delHook(); err != nil {
			return err
		}
	}
	if b.delErr != nil {
		return b.delErr
	}
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

// Restore re-inserts vs at the front, in order (the compensating undo of Del).
func (b *fakeBackup) Restore(ctx context.Context, vs []Number[int]) error {
	if b.restoreErr != nil {
		return b.restoreErr
	}
	b.items = append(append([]Number[int]{}, vs...), b.items...)
	return nil
}

func (b *fakeBackup) Len() int64 { return int64(len(b.items)) }
func (b *fakeBackup) Close(ctx context.Context) error {
	if b.closeHook != nil {
		if err := b.closeHook(); err != nil {
			return err
		}
	}
	b.closed = true
	return nil
}
func (b *fakeBackup) Clear(ctx context.Context) error {
	if b.clearHook != nil {
		if err := b.clearHook(); err != nil {
			return err
		}
	}
	b.items = nil
	return nil
}

func (b *fakeBackup) RangeAll(ctx context.Context) iter.Seq2[Number[int], error] {
	return func(yield func(Number[int], error) bool) {
		for _, it := range b.items {
			if !yield(it, nil) {
				return
			}
		}
	}
}

func (b *fakeBackup) OnLoad(ctx context.Context, v Number[int]) error {
	b.onLoadCalls++
	if b.onLoadErr != nil {
		return b.onLoadErr
	}
	b.onLoad = append(b.onLoad, v.V)
	return nil
}

// TestBackupHydrate verifies New restores a pre-populated backup into every backing: the
// items become the queue's contents (in the backing's order), OnLoad fires once per item,
// and draining mirrors Pops back so the backup empties in lock-step.
func TestBackupHydrate(t *testing.T) {
	vals := []int{5, 1, 4, 2, 3}
	for _, m := range queueMakers() {
		ctx := t.Context()
		fb := &fakeBackup{}
		for _, v := range vals {
			fb.items = append(fb.items, m.item(v))
		}

		q := m.make(t, ctx, 0, WithBackup(fb))

		if got := q.Len(); got != int64(len(vals)) {
			t.Errorf("TestBackupHydrate(%s): Len after hydrate got %d, want %d", m.name, got, len(vals))
		}
		if diff := pretty.Compare(vals, fb.onLoad); diff != "" {
			t.Errorf("TestBackupHydrate(%s): OnLoad calls -want +got:\n%s", m.name, diff)
		}
		if fb.onLoadCalls != len(vals) {
			t.Errorf("TestBackupHydrate(%s): OnLoad call count got %d, want %d (once per item)", m.name, fb.onLoadCalls, len(vals))
		}

		want := wantOrder(m.priority, vals)
		got := pop(t, ctx, m.name, q, len(want))
		if diff := pretty.Compare(want, got); diff != "" {
			t.Errorf("TestBackupHydrate(%s): drain order -want +got:\n%s", m.name, diff)
		}
		if fb.Len() != 0 {
			t.Errorf("TestBackupHydrate(%s): backup Len after drain got %d, want 0", m.name, fb.Len())
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestBackupHydrate(%s): Close got err == %s, want err == nil", m.name, err)
		}
		if !fb.closed {
			t.Errorf("TestBackupHydrate(%s): backup not closed after queue Close", m.name)
		}
	}
}

// restartBbolt populates and persists an on-disk bbolt FIFO at dir with vals, then closes
// it and returns a fresh backing reopened against the same store (count > 0 restart path).
func restartBbolt(t *testing.T, ctx context.Context, dir string, vals []int) Backing[Number[int]] {
	t.Helper()
	openRoot := func() *os.Root {
		r, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatalf("restartBbolt: os.OpenRoot got err == %s", err)
		}
		return r
	}
	b, err := NewBboltFIFO[Number[int]](ctx, openRoot())
	if err != nil {
		t.Fatalf("restartBbolt: populate backing got err == %s", err)
	}
	q, err := New[Number[int]](ctx, "test", b, 0)
	if err != nil {
		t.Fatalf("restartBbolt: populate New got err == %s", err)
	}
	for _, v := range vals {
		if ok, err := q.Push(ctx, []Number[int]{fifoItem(v)}); err != nil || !ok {
			t.Fatalf("restartBbolt: Push(%d) got (ok=%v err=%v)", v, ok, err)
		}
	}
	if err := q.Close(ctx); err != nil {
		t.Fatalf("restartBbolt: Close got err == %s", err)
	}
	rb, err := NewBboltFIFO[Number[int]](ctx, openRoot())
	if err != nil {
		t.Fatalf("restartBbolt: reopen backing got err == %s", err)
	}
	return rb
}

// TestBackupHydrateOnLoad verifies the OnLoad contract during hydrate across every backing:
// it is invoked once per item, an OnLoad failure on the first item aborts New (the
// side-effect hook failed), and the on-disk restart path (a populated bbolt store, count>0)
// drives OnLoad per persisted item in stored order just like the from-backup path.
func TestBackupHydrateOnLoad(t *testing.T) {
	boom := errors.New("onload boom")
	restartVals := []int{10, 20, 30}
	tests := []struct {
		name string
		// setup returns a backing in its to-be-hydrated state and the backup to attach.
		setup func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup)
		// wantOnLoad is the values OnLoad must receive in order (only checked when !wantErr).
		wantOnLoad []int
		wantErr    bool
	}{
		{
			name: "Error: fifo slice OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := NewFIFO[Number[int]]()
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(false, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: btype fifo OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := btype.New[Number[int]]()
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(false, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: indexed btree fifo OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := NewBTreeFIFO[Number[int]](WithIndex())
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(false, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: priority heap OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := NewPriority[Number[int]]()
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(true, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: indexed btree priority OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := NewBTreePriority[Number[int]](WithIndex())
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(true, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: indexed bbolt fifo OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				b, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t), WithIndex())
				if err != nil {
					t.Fatalf("backing build got err == %s", err)
				}
				return b, fromBackup(false, boom)
			},
			wantErr: true,
		},
		{
			name: "Error: bbolt restart OnLoad failure aborts New",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				return restartBbolt(t, ctx, t.TempDir(), restartVals), &fakeBackup{onLoadErr: boom}
			},
			wantErr: true,
		},
		{
			name: "Success: bbolt restart drives OnLoad over persisted items in order",
			setup: func(t *testing.T, ctx context.Context) (Backing[Number[int]], *fakeBackup) {
				return restartBbolt(t, ctx, t.TempDir(), restartVals), &fakeBackup{}
			},
			wantOnLoad: restartVals,
			wantErr:    false,
		},
	}
	for _, test := range tests {
		ctx := t.Context()
		b, fb := test.setup(t, ctx)
		q, err := New[Number[int]](ctx, "test", b, 0, WithBackup(fb))
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestBackupHydrateOnLoad(%s): New got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestBackupHydrateOnLoad(%s): New got err == %s, want err == nil", test.name, err)
			continue
		}
		// On error OnLoad fails on the first item and aborts; on success it runs once
		// per loaded item.
		wantCalls := len(test.wantOnLoad)
		if test.wantErr {
			wantCalls = 1
		}
		if fb.onLoadCalls != wantCalls {
			t.Errorf("TestBackupHydrateOnLoad(%s): OnLoad call count got %d, want %d", test.name, fb.onLoadCalls, wantCalls)
		}
		if test.wantErr {
			continue
		}
		if diff := pretty.Compare(test.wantOnLoad, fb.onLoad); diff != "" {
			t.Errorf("TestBackupHydrateOnLoad(%s): OnLoad items -want +got:\n%s", test.name, diff)
		}
		if got := q.Len(); got != int64(len(test.wantOnLoad)) {
			t.Errorf("TestBackupHydrateOnLoad(%s): Len got %d, want %d", test.name, got, len(test.wantOnLoad))
		}
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestBackupHydrateOnLoad(%s): Close got err == %s", test.name, err)
		}
	}
}

// fromBackup builds a fakeBackup pre-populated with three items of the maker's kind and a
// failing OnLoad, used to exercise the from-backup hydrate-abort path.
func fromBackup(priority bool, onLoadErr error) *fakeBackup {
	fb := &fakeBackup{onLoadErr: onLoadErr}
	for _, v := range []int{1, 2, 3} {
		fb.items = append(fb.items, itemFor(priority, v))
	}
	return fb
}

// sortedVals returns the V values of items, sorted, for multiset comparison.
func sortedVals(items []Number[int]) []int {
	out := make([]int, 0, len(items))
	for _, it := range items {
		out = append(out, it.V)
	}
	sort.Ints(out)
	return out
}

// queueVals reads the queue's live contents (without removing) via RangeAll, sorted.
func queueVals(t *testing.T, ctx context.Context, name string, q *Queue[Number[int]]) []int {
	t.Helper()
	var got []Number[int]
	for v, err := range q.RangeAll(ctx) {
		if err != nil {
			t.Fatalf("TestBackupMirror(%s): RangeAll got err == %s, want err == nil", name, err)
		}
		got = append(got, v)
	}
	return sortedVals(got)
}

// TestBackupMirror verifies every mutation mirrors the *exact* items to the backup: after
// Push/Pop/Del the backup's contents are a true multiset mirror of the queue's live
// contents (not just equal length) across all backings — including priority, where the
// popped item is not the backup's insertion-front. Clear empties it and Close closes it.
func TestBackupMirror(t *testing.T) {
	for _, m := range queueMakers() {
		ctx := t.Context()
		fb := &fakeBackup{}
		q := m.make(t, ctx, 0, WithBackup(fb))

		checkParity := func(stage string) {
			qv := queueVals(t, ctx, m.name, q)
			fv := sortedVals(fb.items)
			if diff := pretty.Compare(qv, fv); diff != "" {
				t.Errorf("TestBackupMirror(%s): %s: queue vs backup contents -queue +backup:\n%s", m.name, stage, diff)
			}
		}

		for i := 0; i < 6; i++ {
			if ok, err := q.Push(ctx, []Number[int]{m.item(i)}); err != nil || !ok {
				t.Fatalf("TestBackupMirror(%s): Push(%d) got (ok=%v err=%v), want (true,nil)", m.name, i, ok, err)
			}
		}
		checkParity("after pushes")

		popped, err := q.Pop(ctx, 2)
		if err != nil || len(popped) != 2 {
			t.Fatalf("TestBackupMirror(%s): Pop got (items=%v err=%v), want 2 items, nil", m.name, popped, err)
		}
		checkParity("after Pop")
		for _, it := range popped {
			for _, b := range fb.items {
				if b.V == it.V {
					t.Errorf("TestBackupMirror(%s): popped value %d still present in backup", m.name, it.V)
				}
			}
		}

		if _, err := q.Del(ctx, []Number[int]{queryItem(3)}); err != nil {
			t.Fatalf("TestBackupMirror(%s): Del got err == %s, want err == nil", m.name, err)
		}
		checkParity("after Del")

		if err := q.Clear(ctx); err != nil {
			t.Fatalf("TestBackupMirror(%s): Clear got err == %s, want err == nil", m.name, err)
		}
		if q.Len() != 0 || fb.Len() != 0 {
			t.Errorf("TestBackupMirror(%s): after Clear q.Len=%d backup.Len=%d, want both 0", m.name, q.Len(), fb.Len())
		}

		if err := q.Close(ctx); err != nil {
			t.Errorf("TestBackupMirror(%s): Close got err == %s, want err == nil", m.name, err)
		}
		if !fb.closed {
			t.Errorf("TestBackupMirror(%s): backup not closed after queue Close", m.name)
		}
	}
}

// TestBackupHydrateKind verifies Hydrate enforces the same kind rule as Push: a backup item whose
// Priority() does not match the backing kind fails New. The accepted rows are what show the rule is
// a rule and not a blanket refusal — a Hydrate that rejected every item would satisfy the error
// rows on its own.
func TestBackupHydrateKind(t *testing.T) {
	tests := []struct {
		name    string
		backing func() (Backing[Number[int]], error)
		item    Number[int]
		wantErr error
	}{
		{
			name:    "Success: FIFO backing accepts a hydrated Priority()==0 item",
			backing: func() (Backing[Number[int]], error) { return NewFIFO[Number[int]]() },
			item:    fifoItem(1),
		},
		{
			name:    "Success: priority backing accepts a hydrated Priority()>0 item",
			backing: func() (Backing[Number[int]], error) { return NewPriority[Number[int]]() },
			item:    prioItem(1),
		},
		{
			name:    "Error: FIFO backing rejects hydrated Priority()>0 item",
			backing: func() (Backing[Number[int]], error) { return NewFIFO[Number[int]]() },
			item:    prioItem(1),
			wantErr: ErrPriorityNotAllowed,
		},
		{
			name:    "Error: priority backing rejects hydrated Priority()==0 item",
			backing: func() (Backing[Number[int]], error) { return NewPriority[Number[int]]() },
			item:    fifoItem(1),
			wantErr: ErrPriorityRequired,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		b, err := test.backing()
		if err != nil {
			t.Fatalf("TestBackupHydrateKind(%s): backing got err == %s, want err == nil", test.name, err)
		}
		fb := &fakeBackup{items: []Number[int]{test.item}}
		q, err := New[Number[int]](ctx, "test", b, 0, WithBackup(fb))
		switch {
		case test.wantErr != nil:
			if !errors.Is(err, test.wantErr) {
				t.Errorf("TestBackupHydrateKind(%s): New got err == %v, want %v", test.name, err, test.wantErr)
			}
			continue
		case err != nil:
			t.Errorf("TestBackupHydrateKind(%s): New got err == %s, want err == nil", test.name, err)
			continue
		}
		// Accepted means restored, not merely not-rejected.
		if n := q.Len(); n != 1 {
			t.Errorf("TestBackupHydrateKind(%s): Len after Hydrate got %d, want 1", test.name, n)
		}
		q.Close(ctx)
	}
}

// TestBackupPushError verifies a backup Push failure aborts the queue Push: the error is
// returned and the item is not added.
func TestBackupPushError(t *testing.T) {
	ctx := t.Context()
	sentinel := errors.New("backup write failed")
	b, err := NewFIFO[Number[int]]()
	if err != nil {
		t.Fatalf("TestBackupPushError: NewFIFO got err == %s, want err == nil", err)
	}
	fb := &fakeBackup{pushErr: sentinel}
	q, err := New[Number[int]](ctx, "test", b, 0, WithBackup(fb))
	if err != nil {
		t.Fatalf("TestBackupPushError: New got err == %s, want err == nil", err)
	}

	ok, err := q.Push(ctx, []Number[int]{fifoItem(1)})
	switch {
	case !errors.Is(err, sentinel):
		t.Errorf("TestBackupPushError: Push got err == %v, want %v", err, sentinel)
	case ok:
		t.Errorf("TestBackupPushError: Push got ok == true, want false")
	}
	if q.Len() != 0 {
		t.Errorf("TestBackupPushError: Len after failed Push got %d, want 0", q.Len())
	}
	if err := q.Close(ctx); err != nil {
		t.Errorf("TestBackupPushError: Close got err == %s, want err == nil", err)
	}
}

// TestBackupRestoreOrder verifies Restore re-inserts items at the front in vs order, so a
// rolled-back head removal (Pop) leaves the backup byte-for-byte as it was.
func TestBackupRestoreOrder(t *testing.T) {
	ctx := t.Context()
	fb := &fakeBackup{}
	if err := fb.Push(ctx, []Number[int]{{V: 1}, {V: 2}, {V: 3}, {V: 4}}); err != nil {
		t.Fatalf("TestBackupRestoreOrder: Push got err == %s, want err == nil", err)
	}
	// Simulate a failed Pop(2): the head two were Del'd from the backup, then restored.
	head := []Number[int]{{V: 1}, {V: 2}}
	if err := fb.Del(ctx, head); err != nil {
		t.Fatalf("TestBackupRestoreOrder: Del got err == %s, want err == nil", err)
	}
	if got := sortedVals(fb.items); pretty.Compare([]int{3, 4}, got) != "" {
		t.Fatalf("TestBackupRestoreOrder: after Del got %v, want [3 4]", got)
	}
	if err := fb.Restore(ctx, head); err != nil {
		t.Fatalf("TestBackupRestoreOrder: Restore got err == %s, want err == nil", err)
	}
	want := []int{1, 2, 3, 4}
	var got []int
	for _, it := range fb.items {
		got = append(got, it.V)
	}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestBackupRestoreOrder: order after Restore -want +got:\n%s", diff)
	}
}

// withBboltFault runs f with bk's per-instance fault seam armed to return injected
// after the backup mirror, restoring the seam afterward.
func withBboltFault(bk Backing[Number[int]], injected error, f func()) {
	b := bk.(*bbolt.Backing[Number[int]])
	b.Hooks.FaultAfterBackup = func() error { return injected }
	defer func() { b.Hooks.FaultAfterBackup = nil }()
	f()
}

// TestBackupRestoreOnPopFailure forces the bbolt delete in Pop to fail after the backup
// was mirrored and verifies Restore runs: the queue is unchanged and the backup is put
// back to a true mirror (in order, since these were head items).
func TestBackupRestoreOnPopFailure(t *testing.T) {
	ctx := t.Context()
	injected := errors.New("injected bbolt fault")

	bk, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("TestBackupRestoreOnPopFailure: NewBboltFIFO got err == %s, want nil", err)
	}
	fb := &fakeBackup{}
	q, err := New[Number[int]](ctx, "test", bk, 0, WithBackup(fb))
	if err != nil {
		t.Fatalf("TestBackupRestoreOnPopFailure: New got err == %s, want nil", err)
	}
	for i := 0; i < 5; i++ {
		if ok, err := q.Push(ctx, []Number[int]{fifoItem(i)}); err != nil || !ok {
			t.Fatalf("TestBackupRestoreOnPopFailure: Push(%d) got (ok=%v err=%v)", i, ok, err)
		}
	}

	var items []Number[int]
	var perr error
	withBboltFault(bk, injected, func() { items, perr = q.Pop(ctx, 2) })
	switch {
	case !errors.Is(perr, injected):
		t.Errorf("TestBackupRestoreOnPopFailure: Pop got err == %v, want injected", perr)
	case items != nil:
		t.Errorf("TestBackupRestoreOnPopFailure: Pop got items == %v, want nil", items)
	}
	if q.Len() != 5 {
		t.Errorf("TestBackupRestoreOnPopFailure: queue Len got %d, want 5 (unchanged)", q.Len())
	}
	var fbVals []int
	for _, it := range fb.items {
		fbVals = append(fbVals, it.V)
	}
	if diff := pretty.Compare([]int{0, 1, 2, 3, 4}, fbVals); diff != "" {
		t.Errorf("TestBackupRestoreOnPopFailure: backup after Restore -want +got:\n%s", diff)
	}

	got := pop(t, ctx, "restore-popn", q, 5)
	if diff := pretty.Compare([]int{0, 1, 2, 3, 4}, got); diff != "" {
		t.Errorf("TestBackupRestoreOnPopFailure: drain after recovery -want +got:\n%s", diff)
	}
	if fb.Len() != 0 {
		t.Errorf("TestBackupRestoreOnPopFailure: backup Len after drain got %d, want 0", fb.Len())
	}
	if err := q.Close(ctx); err != nil {
		t.Errorf("TestBackupRestoreOnPopFailure: Close got err == %s, want nil", err)
	}
}

// TestBackupRestoreOnDelFailure forces the bbolt delete in Del to fail after the backup
// was mirrored and verifies Restore re-adds the items so the backup stays a true mirror.
func TestBackupRestoreOnDelFailure(t *testing.T) {
	ctx := t.Context()
	injected := errors.New("injected bbolt fault")

	bk, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
	if err != nil {
		t.Fatalf("TestBackupRestoreOnDelFailure: NewBboltFIFO got err == %s, want nil", err)
	}
	fb := &fakeBackup{}
	q, err := New[Number[int]](ctx, "test", bk, 0, WithBackup(fb))
	if err != nil {
		t.Fatalf("TestBackupRestoreOnDelFailure: New got err == %s, want nil", err)
	}
	for i := 0; i < 5; i++ {
		if ok, err := q.Push(ctx, []Number[int]{fifoItem(i)}); err != nil || !ok {
			t.Fatalf("TestBackupRestoreOnDelFailure: Push(%d) got (ok=%v err=%v)", i, ok, err)
		}
	}

	var derr error
	var dn int
	withBboltFault(bk, injected, func() { dn, derr = q.Del(ctx, []Number[int]{queryItem(2)}) })
	if !errors.Is(derr, injected) {
		t.Errorf("TestBackupRestoreOnDelFailure: Del got err == %v, want injected", derr)
	}
	// Nothing was removed, so nothing may be reported as removed.
	if dn != 0 {
		t.Errorf("TestBackupRestoreOnDelFailure: failed Del removed %d, want 0", dn)
	}
	if q.Len() != 5 {
		t.Errorf("TestBackupRestoreOnDelFailure: queue Len got %d, want 5 (unchanged)", q.Len())
	}
	if diff := pretty.Compare([]int{0, 1, 2, 3, 4}, sortedVals(fb.items)); diff != "" {
		t.Errorf("TestBackupRestoreOnDelFailure: backup multiset after Restore -want +got:\n%s", diff)
	}

	switch n, err := q.Del(ctx, []Number[int]{queryItem(2)}); {
	case err != nil:
		t.Errorf("TestBackupRestoreOnDelFailure: Del after recovery got err == %s, want nil", err)
	case n != 1:
		t.Errorf("TestBackupRestoreOnDelFailure: Del after recovery removed %d, want 1", n)
	}
	if q.Len() != 4 || fb.Len() != 4 {
		t.Errorf("TestBackupRestoreOnDelFailure: after recovery q.Len=%d fb.Len=%d, want both 4", q.Len(), fb.Len())
	}
	if err := q.Close(ctx); err != nil {
		t.Errorf("TestBackupRestoreOnDelFailure: Close got err == %s, want nil", err)
	}
}

// TestBackupDelCountOnFailure pins Del's count on the on-disk delete. On the failure shapes the
// database write did not land, so the items are still on disk however the compensating restore to
// the backup went — reporting anything but 0 tells an operator entries were removed at exactly the
// moment accurate numbers matter most. The success row is what shows 0 means "nothing removed"
// rather than "this test path always returns 0", and each error row varies one input from it.
func TestBackupDelCountOnFailure(t *testing.T) {
	injected := errors.New("injected bbolt fault")
	restoreFailed := errors.New("restore failed")

	tests := []struct {
		name string
		// fault, when set, makes the bbolt delete transaction fail.
		fault error
		// restoreErr, when set, makes the backup's compensating Restore fail too.
		restoreErr error
		wantErr    error
		wantN      int
		// wantBackup is the backup's contents afterwards, ascending.
		wantBackup []int
	}{
		{
			name:       "Success: a delete that lands reports the entries it removed",
			wantN:      1,
			wantBackup: []int{0, 1, 3, 4},
		},
		{
			name:       "Error: the delete fails and the backup restore puts the item back",
			fault:      injected,
			wantErr:    injected,
			wantN:      0,
			wantBackup: []int{0, 1, 2, 3, 4},
		},
		{
			// Varies only restoreErr from the success row: a restore that would fail is never
			// reached when the delete lands, so it changes nothing. That is what makes the
			// compound row below a deliberate third scenario rather than two wrong inputs.
			name:       "Success: a restore failure is unreachable when the delete lands",
			restoreErr: restoreFailed,
			wantN:      1,
			wantBackup: []int{0, 1, 3, 4},
		},
		{
			name:       "Error: the delete fails and the backup restore fails too",
			fault:      injected,
			restoreErr: restoreFailed,
			wantErr:    restoreFailed,
			wantN:      0,
			// The restore did run — fakeBackup.Restore reports restoreErr before it puts
			// anything back — so the backup is left missing the item the mirror already
			// removed. Genuinely out of sync, which is exactly what the joined error says.
			wantBackup: []int{0, 1, 3, 4},
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		bk, err := NewBboltFIFO[Number[int]](ctx, diskRoot(t))
		if err != nil {
			t.Fatalf("TestBackupDelCountOnFailure(%s): NewBboltFIFO got err == %s, want nil", test.name, err)
		}
		fb := &fakeBackup{}
		q, err := New[Number[int]](ctx, "test", bk, 0, WithBackup(fb))
		if err != nil {
			t.Fatalf("TestBackupDelCountOnFailure(%s): New got err == %s, want nil", test.name, err)
		}
		for i := 0; i < 5; i++ {
			if ok, err := q.Push(ctx, []Number[int]{fifoItem(i)}); err != nil || !ok {
				t.Fatalf("TestBackupDelCountOnFailure(%s): Push(%d) got (ok=%v err=%v)", test.name, i, ok, err)
			}
		}

		// Go at the Backing directly. Queue.Del zeroes the count on any error, so it is the only
		// caller that cannot see a wrong one — and Backing is exported, so it is not the only
		// possible caller.
		fb.restoreErr = test.restoreErr
		var n int
		var derr error
		del := func() { n, derr = bk.Del(ctx, []Number[int]{queryItem(2)}) }
		if test.fault != nil {
			withBboltFault(bk, test.fault, del)
		} else {
			del()
		}

		switch {
		case derr == nil && test.wantErr != nil:
			t.Errorf("TestBackupDelCountOnFailure(%s): got err == nil, want err != nil", test.name)
		case derr != nil && test.wantErr == nil:
			t.Errorf("TestBackupDelCountOnFailure(%s): got err == %s, want err == nil", test.name, derr)
		case derr != nil && !errors.Is(derr, test.wantErr):
			t.Errorf("TestBackupDelCountOnFailure(%s): got err == %v, want %v", test.name, derr, test.wantErr)
		}
		if n != test.wantN {
			t.Errorf("TestBackupDelCountOnFailure(%s): Del removed %d, want %d", test.name, n, test.wantN)
		}
		wantLen := int64(5)
		if test.wantErr == nil {
			wantLen = 4
		}
		if q.Len() != wantLen {
			t.Errorf("TestBackupDelCountOnFailure(%s): queue Len got %d, want %d", test.name, q.Len(), wantLen)
		}
		if diff := pretty.Compare(test.wantBackup, sortedVals(fb.items)); diff != "" {
			t.Errorf("TestBackupDelCountOnFailure(%s): backup multiset -want +got:\n%s", test.name, diff)
		}
		fb.restoreErr = nil
		if err := q.Close(ctx); err != nil {
			t.Errorf("TestBackupDelCountOnFailure(%s): Close got err == %s, want nil", test.name, err)
		}
	}
}
