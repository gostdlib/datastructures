package core

// ValidateKind applies ValidateKindOne to every item in vs. Used by Backings.
func ValidateKind[T Item[T]](priority bool, vs []T) error {
	for _, v := range vs {
		if err := ValidateKindOne(priority, v); err != nil {
			return err
		}
	}
	return nil
}

// ValidateKindOne checks that an item's Priority() matches the backing kind: a priority
// backing requires Priority() > 0, a FIFO backing requires Priority() == 0.
func ValidateKindOne[T Item[T]](priority bool, v T) error {
	switch {
	case priority && v.Priority() == 0:
		return ErrPriorityRequired
	case !priority && v.Priority() != 0:
		return ErrPriorityNotAllowed
	}
	return nil
}

// MatchesAny reports whether stored Equals any element of vs.
func MatchesAny[T Item[T]](stored T, vs []T) bool {
	for i := range vs {
		if stored.Equal(vs[i]) {
			return true
		}
	}
	return false
}
