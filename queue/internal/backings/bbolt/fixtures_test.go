package bbolt

import (
	"os"
	"testing"

	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// num is a minimal core.Item for the tests in this package. The queue package's own Number is
// not reachable from here — queue imports this package, not the other way round — and these
// tests only need something that satisfies the constraint.
type num struct {
	V int
	P uint64
}

func (n num) Less(o num) bool  { return n.P < o.P }
func (n num) Equal(o num) bool { return n.V == o.V }
func (n num) Priority() uint64 { return n.P }
func (n num) Hash() uint64     { return uint64(n.V) }

var _ core.Item[num] = num{}

// diskRoot is an os.Root over a per-test temporary directory, the same helper the queue package's
// tests use to give each on-disk backing a store of its own.
func diskRoot(t *testing.T) *os.Root {
	t.Helper()

	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatalf("os.OpenRoot: %v", err)
	}
	return root
}
