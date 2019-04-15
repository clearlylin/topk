package topk

import "container/heap"

type Tuple struct {
	Key   string
	Count uint64
}

func (t *Tuple) Size() int {
	return 8 + len(t.Key)
}

// topk heap
type TupleTopKHeap struct {
	TopK   int
	Tuples []*Tuple
}

func InitTopKHeap(topk int) *TupleTopKHeap {
	tk := new(TupleTopKHeap)
	tk.TopK = topk
	tk.Tuples = make([]*Tuple, 0)

	return tk
}

func (t TupleTopKHeap) Len() int           { return len(t.Tuples) }
func (t TupleTopKHeap) Less(i, j int) bool { return t.Tuples[i].Count < t.Tuples[j].Count }
func (t TupleTopKHeap) Swap(i, j int)      { t.Tuples[i], t.Tuples[j] = t.Tuples[j], t.Tuples[i] }

func (t *TupleTopKHeap) Push(tuple interface{}) {
	tp := tuple.(*Tuple)
	if len(t.Tuples) >= t.TopK {
		min := heap.Pop(t).(*Tuple)
		if min.Count >= tp.Count {
			t.Tuples = append(t.Tuples, min)
			return
		}
	}
	t.Tuples = append(t.Tuples, tuple.(*Tuple))
}

func (t *TupleTopKHeap) Pop() interface{} {
	old := t.Tuples
	n := len(old)
	x := old[n-1]
	t.Tuples = old[0 : n-1]
	return x
}
