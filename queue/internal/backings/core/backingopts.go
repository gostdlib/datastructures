package core

import (
	"fmt"
	"os"
	"time"
)

// BackingOption is an optional argument to a backing constructor. A single option type is
// shared across constructors; each option validates against the constructor it is given
// (BackingOpts.Call) and returns an error if it is not valid there.
type BackingOption func(BackingOpts) (BackingOpts, error)

// BackingCall identifies which backing constructor a BackingOption is being applied to,
// so a single shared option type can reject use with a constructor it does not support.
//
//go:generate go tool github.com/johnsiilver/stringer -type=BackingCall -linecomment
type BackingCall uint8

const (
	UnknownBackingCall BackingCall = 0 // Unknown
	CallBTreeFIFO      BackingCall = 1 // NewBTreeFIFO
	CallBTreePriority  BackingCall = 2 // NewBTreePriority
	CallBboltFIFO      BackingCall = 3 // NewBboltFIFO
	CallBboltPriority  BackingCall = 4 // NewBboltPriority
)

// BackingOpts is the construction-time settings shared by the backing constructors. call
// records which constructor is applying the options so each option can validate it.
type BackingOpts struct {
	Call  BackingCall
	Width int
	Index bool

	// bbolt-only settings, applied to bolt.Options at open.
	BoltNoSync          bool
	BoltNoFreelistSync  bool
	BoltNoGrowSync      bool
	BoltPreLoadFreelist bool
	BoltFreelistMap     bool
	BoltMlock           bool
	BoltMmapFlags       int
	BoltInitialMmapSize int
	BoltPageSize        int
	BoltTimeout         time.Duration
	BoltOpenFile        func(string, int, os.FileMode) (*os.File, error)

	// codecEncode and codecDecode hold the WithCodec funcs. They are stored as any
	// because BackingOption is not generic; newBboltBacking type-asserts them back to
	// the item-typed func signatures.
	CodecEncode any
	CodecDecode any
}

// BboltOnly returns an error if o.Call is not a bbolt constructor; used by the
// bbolt-specific options.
func (o BackingOpts) BboltOnly(name string) error {
	switch o.Call {
	case CallBboltFIFO, CallBboltPriority:
		return nil
	default:
		return fmt.Errorf("%w: %s is only valid for the bbolt backings (NewBboltFIFO/NewBboltPriority), got %s", ErrBadOption, name, o.Call)
	}
}

// ApplyBackingOptions seeds BackingOpts for the given call and applies options in order,
// returning the first option error.
func ApplyBackingOptions(call BackingCall, options []BackingOption) (BackingOpts, error) {
	if call == UnknownBackingCall {
		panic("bug: ApplyBackingOptions called without a BackingCall")
	}
	o := BackingOpts{Call: call, Width: 32}
	var err error
	for _, opt := range options {
		if o, err = opt(o); err != nil {
			return o, err
		}
	}
	return o, nil
}
