package topk

import (
	"container/heap"
	"testing"
)

func Test_topKHeap(t *testing.T) {

	tk := InitTopKHeap(5)

	tuples := [...]*Tuple{
		&Tuple{Key: "VG", Count: 1},
		&Tuple{Key: "IG", Count: 23},
		&Tuple{Key: "KG", Count: 10},
		&Tuple{Key: "EHOEM", Count: 45},
		&Tuple{Key: "LGD", Count: 21},
		&Tuple{Key: "VP", Count: 34},
		&Tuple{Key: "SECRET", Count: 9},
		&Tuple{Key: "XX", Count: 15},
		&Tuple{Key: "XXX", Count: 115},
	}

	heap.Init(tk)
	for _, k := range tuples {
		heap.Push(tk, k)
	}

	got := heap.Pop(tk).(*Tuple)
	if got.Count != 21 {
		t.Errorf("%s, %d. Expect:%d", got.Key, got.Count, 21)
	}

	got = heap.Pop(tk).(*Tuple)
	if got.Count != 23 {
		t.Errorf("%s, %d. Expect:%d", got.Key, got.Count, 23)
	}

	got = heap.Pop(tk).(*Tuple)
	if got.Count != 34 {
		t.Errorf("%s, %d. Expect:%d", got.Key, got.Count, 34)
	}

	got = heap.Pop(tk).(*Tuple)
	if got.Count != 45 {
		t.Errorf("%s, %d. Expect:%d", got.Key, got.Count, 45)
	}

	got = heap.Pop(tk).(*Tuple)
	if got.Count != 115 {
		t.Errorf("%s, %d. Expect:%d", got.Key, got.Count, 115)
	}

	if tk.Len() != 0 {
		t.Errorf("Len:%d, Expect:%d", tk.Len(), 0)
	}
}
