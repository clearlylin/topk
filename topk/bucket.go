package topk

type Bucket interface {
	Close() error
	Write(string) error
	Size() (int64, error)
	Flush() error

	Statistic() ([]*Tuple, int64, error)
	TopK(k int) ([]*Tuple, error)
	TopK2(k int) (*TupleTopKHeap, error)
	WriteSortFile([]*Tuple) error
}
