package cbwfq_test

import (
	"fmt"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/datastructures/queue"
	"github.com/gostdlib/datastructures/queue/cbwfq"
)

// mkQ is a tiny helper: an unbounded in-memory FIFO queue of queue.String.
func mkQ() *queue.Queue[queue.String] {
	b, _ := queue.NewFIFO[queue.String]()
	q, _ := queue.New(context.Background(), "", b, queue.Unlimited)
	return q
}

// classByPrefix routes an item to the class named by the text before its first
// ':' (e.g. "bulk:42" -> "bulk"); anything else falls through to the default class.
func classByPrefix(s queue.String) string {
	for i := 0; i < len(s.V); i++ {
		if s.V[i] == ':' {
			return s.V[:i]
		}
	}
	return ""
}

// ExampleCBWFQ shows weighted fair dequeuing: with weights 1 and 3, the "bulk"
// class is served three items for every one from the default "std" class.
func ExampleCBWFQ() {
	ctx := context.Background()

	s, err := cbwfq.New(ctx, "example", classByPrefix, []cbwfq.Class[queue.String]{
		{Name: "std", Weight: 1, Queue: mkQ(), Default: true},
		{Name: "bulk", Weight: 3, Queue: mkQ()},
	})
	if err != nil {
		panic(err)
	}
	defer s.Close(ctx)

	for i := 0; i < 2; i++ {
		s.Push(ctx, queue.String{V: "std:x"})
	}
	for i := 0; i < 6; i++ {
		s.Push(ctx, queue.String{V: "bulk:y"})
	}

	for i := 0; i < 8; i++ {
		_, class, _ := s.Pull(ctx, 1)
		fmt.Print(class, " ")
	}
	fmt.Println()
	// Output: std bulk bulk bulk std bulk bulk bulk
}

// ExampleCBWFQ_priority shows the strict-priority (control) class preempting the
// weighted classes: control traffic is always drained first, ahead of "std".
func ExampleCBWFQ_priority() {
	ctx := context.Background()

	s, err := cbwfq.New(ctx, "example", classByPrefix, []cbwfq.Class[queue.String]{
		{Name: "std", Weight: 1, Queue: mkQ(), Default: true},
		{Name: "ctrl", Queue: mkQ(), Priority: true},
	})
	if err != nil {
		panic(err)
	}
	defer s.Close(ctx)

	s.Push(ctx, queue.String{V: "std:1"})
	s.Push(ctx, queue.String{V: "std:2"})
	s.Push(ctx, queue.String{V: "ctrl:1"}) // arrives last...

	for i := 0; i < 3; i++ {
		items, _, _ := s.Pull(ctx, 1)
		fmt.Println(items[0].V)
	}
	// Output:
	// ctrl:1
	// std:1
	// std:2
}
