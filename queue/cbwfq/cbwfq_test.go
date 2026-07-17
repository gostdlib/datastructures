package cbwfq

import (
	"errors"
	"runtime"
	"testing"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/datastructures/queue"
	"github.com/kylelemons/godebug/pretty"
)

// newQ builds an unbounded in-memory FIFO queue of queue.String for tests.
func newQ(t *testing.T) *queue.Queue[queue.String] {
	t.Helper()
	b, err := queue.NewFIFO[queue.String]()
	if err != nil {
		t.Fatalf("newQ: NewFIFO: %s", err)
	}
	q, err := queue.New(t.Context(), "", b, queue.Unlimited)
	if err != nil {
		t.Fatalf("newQ: New: %s", err)
	}
	return q
}

// firstRune classifies a queue.String by its leading byte, so "a1" -> "a".
func firstRune(s queue.String) string {
	if s.V == "" {
		return ""
	}
	return s.V[:1]
}

func TestNew(t *testing.T) {
	t.Parallel()

	// baseline returns a fresh, valid class set: weighted "a" (default) + "b", and
	// a strict-priority "p". Each case mutates exactly one thing.
	baseline := func() []Class[queue.String] {
		return []Class[queue.String]{
			{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
			{Name: "b", Weight: 3, Queue: newQ(t)},
			{Name: "p", Queue: newQ(t), Priority: true},
		}
	}

	tests := []struct {
		name       string
		classifier Classifier[queue.String]
		classes    []Class[queue.String]
		wantErr    bool
	}{
		{
			name:       "Success: valid weighted + priority + default set",
			classifier: firstRune,
			classes:    baseline(),
		},
		{
			name:       "Success: single default-only class",
			classifier: firstRune,
			classes:    []Class[queue.String]{{Name: "a", Weight: 1, Queue: newQ(t), Default: true}},
		},
		{
			name:       "Error: nil classifier",
			classifier: nil,
			classes:    baseline(),
			wantErr:    true,
		},
		{
			name:       "Error: no classes",
			classifier: firstRune,
			classes:    nil,
			wantErr:    true,
		},
		{
			name:       "Error: empty class name",
			classifier: firstRune,
			classes:    []Class[queue.String]{{Name: "", Weight: 1, Queue: newQ(t), Default: true}},
			wantErr:    true,
		},
		{
			name:       "Error: duplicate class name",
			classifier: firstRune,
			classes: []Class[queue.String]{
				{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
				{Name: "a", Weight: 1, Queue: newQ(t)},
			},
			wantErr: true,
		},
		{
			name:       "Error: nil queue",
			classifier: firstRune,
			classes:    []Class[queue.String]{{Name: "a", Weight: 1, Queue: nil, Default: true}},
			wantErr:    true,
		},
		{
			name:       "Error: weight below one on a weighted class",
			classifier: firstRune,
			classes:    []Class[queue.String]{{Name: "a", Weight: 0, Queue: newQ(t), Default: true}},
			wantErr:    true,
		},
		{
			name:       "Error: no default class",
			classifier: firstRune,
			classes:    []Class[queue.String]{{Name: "a", Weight: 1, Queue: newQ(t)}},
			wantErr:    true,
		},
		{
			name:       "Error: two default classes",
			classifier: firstRune,
			classes: []Class[queue.String]{
				{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
				{Name: "b", Weight: 1, Queue: newQ(t), Default: true},
			},
			wantErr: true,
		},
		{
			name:       "Error: two priority classes",
			classifier: firstRune,
			classes: []Class[queue.String]{
				{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
				{Name: "p", Queue: newQ(t), Priority: true},
				{Name: "q", Queue: newQ(t), Priority: true},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		_, err := New(t.Context(), "", test.classifier, test.classes)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestNew(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestNew(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

func TestPushClassification(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
		{Name: "b", Weight: 1, Queue: newQ(t)},
	})
	if err != nil {
		t.Fatalf("TestPushClassification: New: %s", err)
	}

	// "a1"->a, "b1"->b, "z1"->default(a). So a gets 2, b gets 1.
	for _, v := range []string{"a1", "b1", "z1"} {
		if err := s.Push(ctx, queue.String{V: v}); err != nil {
			t.Fatalf("TestPushClassification: Push(%s): %s", v, err)
		}
	}

	aLen, _ := s.ClassLen("a")
	bLen, _ := s.ClassLen("b")
	if got, want := [2]int64{aLen, bLen}, [2]int64{2, 1}; got != want {
		t.Errorf("TestPushClassification: got class lens %v, want %v", got, want)
	}
	if got, want := s.Len(), int64(3); got != want {
		t.Errorf("TestPushClassification: got Len() == %d, want %d", got, want)
	}

	if _, err := s.ClassLen("nope"); !errors.Is(err, ErrUnknownClass) {
		t.Errorf("TestPushClassification: ClassLen(nope): got err == %v, want ErrUnknownClass", err)
	}
}

func TestPullWeighting(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// "a" weight 1, "b" weight 3. With both backlogged, single-item pulls should
	// interleave 1:3 -> a,b,b,b,a,b,b,b,...
	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
		{Name: "b", Weight: 3, Queue: newQ(t)},
	})
	if err != nil {
		t.Fatalf("TestPullWeighting: New: %s", err)
	}
	for i := 0; i < 4; i++ {
		if err := s.Push(ctx, queue.String{V: "a"}); err != nil {
			t.Fatalf("TestPullWeighting: Push a: %s", err)
		}
	}
	for i := 0; i < 12; i++ {
		if err := s.Push(ctx, queue.String{V: "b"}); err != nil {
			t.Fatalf("TestPullWeighting: Push b: %s", err)
		}
	}

	got := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		items, class, err := s.Pull(ctx, 1)
		if err != nil {
			t.Fatalf("TestPullWeighting: Pull: %s", err)
		}
		if len(items) != 1 {
			t.Fatalf("TestPullWeighting: got %d items, want 1", len(items))
		}
		got = append(got, class)
	}

	want := []string{"a", "b", "b", "b", "a", "b", "b", "b", "a", "b", "b", "b", "a", "b", "b", "b"}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestPullWeighting: pull-class sequence -want/+got:\n%s", diff)
	}
}

func TestPullBatchCap(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// A weighted class with weight 2 must not hand out more than its quantum in one
	// batch pull, even when more items are available and a larger n is requested.
	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 2, Queue: newQ(t), Default: true},
		{Name: "b", Weight: 2, Queue: newQ(t)},
	})
	if err != nil {
		t.Fatalf("TestPullBatchCap: New: %s", err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Push(ctx, queue.String{V: "a"}); err != nil {
			t.Fatalf("TestPullBatchCap: Push: %s", err)
		}
	}

	items, class, err := s.Pull(ctx, 100)
	if err != nil {
		t.Fatalf("TestPullBatchCap: Pull: %s", err)
	}
	if class != "a" {
		t.Errorf("TestPullBatchCap: got class %q, want a", class)
	}
	if got, want := len(items), 2; got != want {
		t.Errorf("TestPullBatchCap: got batch of %d, want %d (weight quantum)", got, want)
	}
}

func TestPullPriority(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Priority "p" must be served ahead of the weighted classes whenever non-empty.
	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
		{Name: "b", Weight: 1, Queue: newQ(t)},
		{Name: "p", Queue: newQ(t), Priority: true},
	})
	if err != nil {
		t.Fatalf("TestPullPriority: New: %s", err)
	}
	// Backlog the weighted classes, then drop two control items into priority.
	for _, v := range []string{"a1", "a2", "b1", "b2", "p1", "p2"} {
		if err := s.Push(ctx, queue.String{V: v}); err != nil {
			t.Fatalf("TestPullPriority: Push(%s): %s", v, err)
		}
	}

	// First two pulls must drain priority, in FIFO order, before any weighted item.
	for _, want := range []string{"p1", "p2"} {
		items, class, err := s.Pull(ctx, 1)
		if err != nil {
			t.Fatalf("TestPullPriority: Pull: %s", err)
		}
		if class != "p" || items[0].V != want {
			t.Errorf("TestPullPriority: got (%q, %q), want (p, %q)", class, items[0].V, want)
		}
	}

	// Priority now empty: the next pull falls through to a weighted class.
	_, class, err := s.Pull(ctx, 1)
	if err != nil {
		t.Fatalf("TestPullPriority: Pull weighted: %s", err)
	}
	if class == "p" {
		t.Errorf("TestPullPriority: priority empty but pull returned class p")
	}
}

func TestPullBlocksThenWakes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
	})
	if err != nil {
		t.Fatalf("TestPullBlocksThenWakes: New: %s", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Push(ctx, queue.String{V: "a1"}); err != nil {
			t.Errorf("TestPullBlocksThenWakes: Push: %s", err)
		}
	}()

	// Pull on an empty scheduler blocks until the goroutine's Push wakes it.
	items, class, err := s.Pull(ctx, 1)
	if err != nil {
		t.Fatalf("TestPullBlocksThenWakes: Pull: %s", err)
	}
	if class != "a" || items[0].V != "a1" {
		t.Errorf("TestPullBlocksThenWakes: got (%q, %q), want (a, a1)", class, items[0].V)
	}
	<-done
}

func TestConcurrentPushPull(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
		{Name: "b", Weight: 2, Queue: newQ(t)},
		{Name: "p", Queue: newQ(t), Priority: true},
	})
	if err != nil {
		t.Fatalf("TestConcurrentPushPull: New: %s", err)
	}

	const producers, perProducer, consumers = 8, 500, 4
	const total = producers * perProducer
	classes := []string{"a", "b", "p"}

	var pg sync.Group
	for p := 0; p < producers; p++ {
		pg.Go(ctx, func(ctx context.Context) error {
			for i := 0; i < perProducer; i++ {
				if err := s.Push(ctx, queue.String{V: classes[(p+i)%len(classes)]}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Consumers pull until Close makes Pull return ErrClosed. Each writes only its
	// own counts index, so the reads after Wait are race-free.
	counts := make([]int, consumers)
	var cg sync.Group
	for c := 0; c < consumers; c++ {
		cg.Go(ctx, func(ctx context.Context) error {
			for {
				items, _, err := s.Pull(ctx, 16)
				if err != nil {
					return nil
				}
				counts[c] += len(items)
			}
		})
	}

	if err := pg.Wait(ctx); err != nil {
		t.Fatalf("TestConcurrentPushPull: producers: %s", err)
	}
	// Everything is pushed; drain until empty, then Close so consumers exit.
	for s.Len() > 0 {
		runtime.Gosched()
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("TestConcurrentPushPull: Close: %s", err)
	}
	cg.Wait(ctx)

	pulled := 0
	for _, n := range counts {
		pulled += n
	}
	if pulled != total {
		t.Errorf("TestConcurrentPushPull: pulled %d items, want %d", pulled, total)
	}
}

func TestPullCanceled(t *testing.T) {
	t.Parallel()

	s, err := New(t.Context(), "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
	})
	if err != nil {
		t.Fatalf("TestPullCanceled: New: %s", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, _, err := s.Pull(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("TestPullCanceled: got err == %v, want context.Canceled", err)
	}
}

func TestCloseUnblocksPull(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	s, err := New(ctx, "", firstRune, []Class[queue.String]{
		{Name: "a", Weight: 1, Queue: newQ(t), Default: true},
	})
	if err != nil {
		t.Fatalf("TestCloseUnblocksPull: New: %s", err)
	}

	got := make(chan error, 1)
	go func() {
		_, _, perr := s.Pull(ctx, 1)
		got <- perr
	}()

	if err := s.Close(ctx); err != nil {
		t.Fatalf("TestCloseUnblocksPull: Close: %s", err)
	}
	if err := <-got; !errors.Is(err, queue.ErrClosed) {
		t.Errorf("TestCloseUnblocksPull: blocked Pull got err == %v, want ErrClosed", err)
	}
}
