package cbwfq

import "errors"

// ErrUnknownClass is returned by ClassLen when no class has the requested name.
var ErrUnknownClass = errors.New("unknown class")
