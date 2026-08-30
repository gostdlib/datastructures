package core

// OpCall identifies which queue operation an OpOption is being applied to, so a single shared
// option type can reject use with an operation it does not support. It mirrors BackingOpts.Call,
// which does the same job for the backing constructors.
//
//go:generate go tool github.com/johnsiilver/stringer -type=OpCall -linecomment
type OpCall uint8

const (
	UnknownOpCall OpCall = 0 // Unknown
	CallPush      OpCall = 1 // Push
	CallPop       OpCall = 2 // Pop
	CallPeek      OpCall = 3 // Peek
	CallExists    OpCall = 4 // Exists
	CallDel       OpCall = 5 // Del
	CallNotEmpty  OpCall = 6 // NotEmpty
	CallNotFull   OpCall = 7 // NotFull
	CallClose     OpCall = 8 // Close
	CallClear     OpCall = 9 // Clear
)

// OpOptions hold per-operation options.
type OpOptions struct {
	// call records which operation the options are being applied to, so an option that is only
	// meaningful on some operations can reject the rest. Set by ResolveOpOptions before any
	// option runs, so every option closure can read it.
	Call OpCall
	// sideEffect runs while the queue's lock is held, once the operation is otherwise
	// guaranteed to succeed; a non-nil return rolls the operation back. nil here means the
	// option was not given — WithSideEffect rejects a nil func with ErrNilSideEffect rather
	// than storing one. See WithSideEffect.
	SideEffect func() error
	// onAdmit runs on Push after capacity is reserved and the lock is released, before the
	// items are inserted; a non-nil return releases the reservation and inserts nothing. nil
	// (the default) means no hook. See WithOnAdmit.
	OnAdmit func() error
}

// OpOption is an optional argument for queue operations. Options are applied in order, so
// later options override earlier ones. All options are optional; zero values ask for defaults.
// Some options are valid only for certain operations and return an error elsewhere; see each
// option. A nil func handed to an option is an error, not a no-op.
type OpOption func(opts OpOptions) (OpOptions, error)

// ResolveOpOptions applies each option in order for the named operation, propagating any error. The
// operation is stamped on the options before the first one runs, so an option that is valid only on
// some operations can reject the rest from inside its own closure.
func ResolveOpOptions(call OpCall, opts []OpOption) (OpOptions, error) {
	if call == UnknownOpCall {
		// Not reachable from a caller: every operation passes its own constant. This catches the
		// one mistake the 46 hand-stamped call sites invite — a new operation added without one.
		panic("bug: ResolveOpOptions called without an OpCall")
	}
	o := OpOptions{Call: call}
	for _, opt := range opts {
		var err error
		o, err = opt(o)
		if err != nil {
			return o, err
		}
	}
	return o, nil
}

// ReleaseReservation signals a parked producer when a Push abandons after RunOnAdmit has already
// given its reservation back — a side effect or backup mirror that failed once the hook succeeded.
// The capacity the reservation held is free again and may be exactly what the parked producer is
// waiting for, and nothing else in that path is going to signal on its behalf. A Push carrying no
// hook never held a reservation and owes nothing. The caller must hold the write lock.
func (o OpOptions) ReleaseReservation(notFull *Signal) {
	if o.OnAdmit == nil {
		return
	}
	if notFull.HasWaiters() {
		notFull.Signal()
	}
}
