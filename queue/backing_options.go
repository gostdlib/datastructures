package queue

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/gostdlib/datastructures/queue/internal/backings/core"
)

// WithCodec sets the on-disk serialization for the bbolt backings. enc writes an item
// into a reused *bytes.Buffer; dec reads a record back into a reused *T. The package
// provides JSONEncode/JSONDecode for JSON-serializable types; supply your own for any
// other format. Items whose default JSON encoding fails (notably Value, which has
// function fields) require this — NewBboltFIFO/NewBboltPriority return ErrCodecRequired
// for such an item type when WithCodec is absent. Valid only for the bbolt backings.
func WithCodec[T Item[T]](enc func(dst *bytes.Buffer, v T) error, dec func(src []byte, dst *T) error) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithCodec"); err != nil {
			return o, err
		}
		if enc == nil || dec == nil {
			return o, fmt.Errorf("%w: WithCodec requires a non-nil encoder and decoder", ErrBadOption)
		}
		o.CodecEncode = enc
		o.CodecDecode = dec
		return o, nil
	}
}

// WithIndex keeps an in-memory index of items keyed by Item.Hash so Exists and Del do a
// bucket lookup instead of a full scan. This trades extra memory and a little push/pop
// overhead for much faster Exists/Del on delete-heavy workloads (e.g. scanning RangeAll
// and deleting each matching entry). Valid for NewBTreeFIFO (switches it from the
// positional btype tree to a keyed B-Tree, the positional FIFO having no stable locator
// to index), NewBTreePriority, NewBboltFIFO and NewBboltPriority.
func WithIndex() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		switch o.Call {
		case core.CallBTreeFIFO, core.CallBTreePriority, core.CallBboltFIFO, core.CallBboltPriority:
			o.Index = true
		default:
			return o, fmt.Errorf("%w: WithIndex is not valid for %s", ErrBadOption, o.Call)
		}
		return o, nil
	}
}

// WithBTreeWidth sets the keyed B-Tree node width. A larger width means fewer nodes and
// faster access but more memory; smaller is the reverse. Width must be at least 2
// (default 32). Valid only for the keyed B-Tree backings: NewBTreeFIFO (indexed) and
// NewBTreePriority.
func WithBTreeWidth(width int) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		switch o.Call {
		case core.CallBTreeFIFO, core.CallBTreePriority:
			if width < 2 {
				return o, fmt.Errorf("%w: WithBTreeWidth must be at least 2, got %d", ErrBadOption, width)
			}
			o.Width = width
		default:
			return o, fmt.Errorf("%w: WithBTreeWidth is not valid for %s", ErrBadOption, o.Call)
		}
		return o, nil
	}
}

// WithNoSync skips the fsync after every bbolt commit (bolt.Options.NoSync). This is much
// faster for bulk loading but is unsafe: a crash or OS failure can lose recently committed
// items. Valid only for NewBboltFIFO and NewBboltPriority.
func WithNoSync() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithNoSync"); err != nil {
			return o, err
		}
		o.BoltNoSync = true
		return o, nil
	}
}

// WithNoFreelistSync stops bbolt from syncing its freelist to disk (bolt.Options.
// NoFreelistSync). Faster commits; on the next open the freelist is rebuilt by scanning,
// so it remains crash-safe but reopen is slower. Valid only for NewBboltFIFO and
// NewBboltPriority.
func WithNoFreelistSync() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithNoFreelistSync"); err != nil {
			return o, err
		}
		o.BoltNoFreelistSync = true
		return o, nil
	}
}

// WithNoGrowSync skips the fsync after growing the bbolt file (bolt.Options.NoGrowSync).
// Valid only for NewBboltFIFO and NewBboltPriority.
func WithNoGrowSync() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithNoGrowSync"); err != nil {
			return o, err
		}
		o.BoltNoGrowSync = true
		return o, nil
	}
}

// WithBoltTimeout sets how long bbolt waits to obtain the database file lock at open
// (bolt.Options.Timeout); zero (default) waits indefinitely. Valid only for NewBboltFIFO
// and NewBboltPriority.
func WithBoltTimeout(d time.Duration) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltTimeout"); err != nil {
			return o, err
		}
		o.BoltTimeout = d
		return o, nil
	}
}

// WithBoltPreLoadFreelist loads the free pages when opening the database
// (bolt.Options.PreLoadFreelist): faster first write, slower open. Valid only for
// NewBboltFIFO and NewBboltPriority.
func WithBoltPreLoadFreelist() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltPreLoadFreelist"); err != nil {
			return o, err
		}
		o.BoltPreLoadFreelist = true
		return o, nil
	}
}

// WithBoltFreelistMap selects bbolt's hashmap freelist backend instead of the default
// array (bolt.Options.FreelistType). The hashmap backend is faster in almost all cases,
// especially for large/fragmented databases. Valid only for NewBboltFIFO and
// NewBboltPriority.
func WithBoltFreelistMap() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltFreelistMap"); err != nil {
			return o, err
		}
		o.BoltFreelistMap = true
		return o, nil
	}
}

// WithBoltMlock locks the database memory-map into RAM to prevent page faults
// (bolt.Options.Mlock, UNIX only); the memory cannot be reclaimed while open. Valid only
// for NewBboltFIFO and NewBboltPriority.
func WithBoltMlock() BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltMlock"); err != nil {
			return o, err
		}
		o.BoltMlock = true
		return o, nil
	}
}

// WithBoltMmapFlags sets the flags passed to mmap when memory-mapping the database
// (bolt.Options.MmapFlags), e.g. syscall.MAP_POPULATE on Linux. Valid only for
// NewBboltFIFO and NewBboltPriority.
func WithBoltMmapFlags(flags int) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltMmapFlags"); err != nil {
			return o, err
		}
		o.BoltMmapFlags = flags
		return o, nil
	}
}

// WithBoltInitialMmapSize sets the initial mmap size of the database in bytes
// (bolt.Options.InitialMmapSize). A size large enough to hold the database keeps read
// transactions from blocking write transactions. Valid only for NewBboltFIFO and
// NewBboltPriority.
func WithBoltInitialMmapSize(bytes int) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltInitialMmapSize"); err != nil {
			return o, err
		}
		o.BoltInitialMmapSize = bytes
		return o, nil
	}
}

// WithBoltPageSize overrides the default OS page size for a newly created database
// (bolt.Options.PageSize). It has no effect on an existing database. Valid only for
// NewBboltFIFO and NewBboltPriority.
func WithBoltPageSize(bytes int) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltPageSize"); err != nil {
			return o, err
		}
		o.BoltPageSize = bytes
		return o, nil
	}
}

// WithBoltOpenFile sets the function bbolt uses to open the database file
// (bolt.Options.OpenFile); it defaults to os.OpenFile. Useful for hermetic tests or a
// custom filesystem. Valid only for NewBboltFIFO and NewBboltPriority.
func WithBoltOpenFile(fn func(string, int, os.FileMode) (*os.File, error)) BackingOption {
	return func(o core.BackingOpts) (core.BackingOpts, error) {
		if err := o.BboltOnly("WithBoltOpenFile"); err != nil {
			return o, err
		}
		o.BoltOpenFile = fn
		return o, nil
	}
}
