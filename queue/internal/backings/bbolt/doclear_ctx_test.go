package bbolt

import (
	"iter"
	"testing"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// fakeCtxBackup implements Backup. Push records the ctx it is called with and fails
// if that ctx is canceled, so a test can assert which ctx the bbolt commit pipeline
// hands to the backup mirror.
type fakeCtxBackup struct {
	pushCtxs []context.Context
}

func (b *fakeCtxBackup) Push(ctx context.Context, _ []num) error {
	b.pushCtxs = append(b.pushCtxs, ctx)
	return ctx.Err()
}
func (b *fakeCtxBackup) Del(context.Context, []num) error     { return nil }
func (b *fakeCtxBackup) Restore(context.Context, []num) error { return nil }
func (b *fakeCtxBackup) Len() int64                           { return 0 }
func (b *fakeCtxBackup) Close(context.Context) error          { return nil }
func (b *fakeCtxBackup) Clear(context.Context) error          { return nil }
func (b *fakeCtxBackup) OnLoad(context.Context, num) error    { return nil }
func (b *fakeCtxBackup) RangeAll(context.Context) iter.Seq2[num, error] {
	return func(yield func(num, error) bool) {}
}

// TestBboltDoClearDrainUsesFlusherCtx is a regression test: doClear's drain step
// commits items pushed by *other* callers, so it must use the flusher's lifetime
// ctx, not the Clear caller's ctx — otherwise a Clear with a canceled ctx fails
// the in-flight buffered Push's commit (via backup.Push), and the unrelated Push
// call returns that cancellation error.
//
// The test calls doClear directly (bypassing the flusher routing, which only
// affects how doClear is *dispatched*, not its drain-ctx choice) with a manually
// buffered item and a canceled Clear ctx. With the fix, backup.Push for the
// drained item sees a non-canceled ctx and the buffered "Push" (its bufCur) gets
// a nil err.
func TestBboltDoClearDrainUsesFlusherCtx(t *testing.T) {
	ctx := t.Context()
	o, err := core.ApplyBackingOptions(core.CallBboltFIFO, nil)
	if err != nil {
		t.Fatalf("TestBboltDoClearDrainUsesFlusherCtx: core.ApplyBackingOptions got err == %s, want err == nil", err)
	}
	bk, err := newBacking[num](ctx, diskRoot(t), o, fifoKey[num], false)
	if err != nil {
		t.Fatalf("TestBboltDoClearDrainUsesFlusherCtx: newBacking got err == %s, want err == nil", err)
	}
	p := bk.(*Backing[num])
	// SetQueueLock is intentionally not called: the flusher stays offline so doClear
	// is the only goroutine driving commit, and there is no race with a real flush.
	t.Cleanup(func() { _ = p.Close(ctx) })

	backup := &fakeCtxBackup{}
	p.backup = backup

	// Manually stage an item the way a Push would: append to p.buf, bump inflight,
	// remember the current cur so we can read its err after the drain.
	p.lk.Lock()
	p.buf = []num{num{V: 1}}
	p.inflight = 1
	bufCur := p.cur
	p.lk.Unlock()

	cctx, cancel := context.WithCancel(ctx)
	cancel()

	_ = p.doClear(cctx, nil, &codeSubject{}) // doClear's own backup.Clear/db.Update may run under cctx; not asserted here.

	<-bufCur.done
	if bufCur.err != nil {
		t.Errorf("TestBboltDoClearDrainUsesFlusherCtx: buffered Push got err == %v, want err == nil (doClear's drain leaked the Clear caller's canceled ctx into commit)", bufCur.err)
	}
	if len(backup.pushCtxs) != 1 {
		t.Fatalf("TestBboltDoClearDrainUsesFlusherCtx: backup.Push called %d times, want 1", len(backup.pushCtxs))
	}
	if cerr := backup.pushCtxs[0].Err(); cerr != nil {
		t.Errorf("TestBboltDoClearDrainUsesFlusherCtx: backup.Push was called with a canceled ctx (err=%v); the drain must use the flusher's ctx", cerr)
	}
}

// TestBboltDoClearCanceledCtx is a regression test: a Clear caller whose ctx is canceled
// while the command waits in the flusher queue must not have its side effect (or the
// clear) run behind its back — doClear aborts with the cancellation cause before the
// side effect.
func TestBboltDoClearCanceledCtx(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name    string
		cancel  bool
		wantErr bool
	}{
		{name: "Success: live ctx runs the side effect", cancel: false, wantErr: false},
		{name: "Error: canceled ctx aborts before the side effect", cancel: true, wantErr: true},
	}

	for _, test := range tests {
		o, err := core.ApplyBackingOptions(core.CallBboltFIFO, nil)
		if err != nil {
			t.Fatalf("TestBboltDoClearCanceledCtx(%s): core.ApplyBackingOptions got err == %s, want err == nil", test.name, err)
		}
		bk, err := newBacking[num](ctx, diskRoot(t), o, fifoKey[num], false)
		if err != nil {
			t.Fatalf("TestBboltDoClearCanceledCtx(%s): newBacking got err == %s, want err == nil", test.name, err)
		}
		p := bk.(*Backing[num])
		// SetQueueLock is intentionally not called: the flusher stays offline so doClear
		// is the only goroutine driving commit.
		t.Cleanup(func() { _ = p.Close(ctx) })

		cctx := ctx
		if test.cancel {
			var cancel context.CancelFunc
			cctx, cancel = context.WithCancel(ctx)
			cancel()
		}

		ran := false
		err = p.doClear(cctx, func() error { ran = true; return nil }, &codeSubject{})
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestBboltDoClearCanceledCtx(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestBboltDoClearCanceledCtx(%s): got err == %s, want err == nil", test.name, err)
			continue
		}
		if ran == test.cancel {
			t.Errorf("TestBboltDoClearCanceledCtx(%s): side effect ran == %v, want %v", test.name, ran, !test.cancel)
		}
	}
}
