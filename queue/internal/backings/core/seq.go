package core

// SeqItem pairs an item with a monotonic insert sequence so the btree can hold a stable
// FIFO order independent of T's Less.
type SeqItem[T Item[T]] struct {
	Seq  uint64
	Item T
}

// FifoSeqLess orders SeqItem values by insert sequence only, giving the btree FIFO order.
func FifoSeqLess[T Item[T]](a, b SeqItem[T]) bool {
	return a.Seq < b.Seq
}

// PrioritySeqLess orders SeqItem values by Item.Less with insert sequence as a tiebreak.
func PrioritySeqLess[T Item[T]](a, b SeqItem[T]) bool {
	if a.Item.Less(b.Item) {
		return true
	}
	if b.Item.Less(a.Item) {
		return false
	}
	return a.Seq < b.Seq
}
