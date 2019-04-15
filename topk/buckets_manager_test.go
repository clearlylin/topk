package topk

import (
	"bufio"
	"container/heap"
	"hash/fnv"
	"io"
	"os"
	"testing"
)

const MAX_SIZE = 1024 * 1024

func test_init(t *testing.T) (*BucketsManager, *os.File) {
	bm, err := InitFileBucketManager(1024, 5, MAX_SIZE, "./test_result")
	if err != nil {
		t.Fatalf("InitFileBucketManager Failed. %v", err)
	}

	if len(bm.Buckets) != 5 {
		t.Fatal("InitFileBucketManager Failed.", len(bm.Buckets))
	}

	dataFile, err := os.Open("../data/dev_urls")
	if err != nil {
		t.Fatalf("Open Failed. %v", err)
	}
	return bm, dataFile
}

func validate(t *testing.T, bm *BucketsManager) {

	tt := InitTopKHeap(5)
	heap.Init(tt)

	for _, f := range bm.Buckets {
		tuples, _, err := f.Statistic()
		if err != nil {
			t.Fatalf("Statistic Failed. %v", err)
		}

		for _, t := range tuples {
			heap.Push(tt, t)
		}
	}

	result := make([]*Tuple, 0)
	for tt.Len() > 0 {
		result = append(result, heap.Pop(tt).(*Tuple))
	}
	if len(result) != 5 {
		t.Fatalf("Statistic result Failed. expect:5, %d", len(result))
	}

	if result[0].Count != 5 {
		t.Fatalf("Statistic result Failed. expect:980, %d", result[0].Count)
	}

	if result[4].Count != 7 {
		t.Fatalf("Statistic result Failed. expect:980, %d", result[4].Count)
	}

}

func Test_Serial(t *testing.T) {
	bm, dataFile := test_init(t)
	defer bm.Close()
	defer dataFile.Close()

	urls := make([]string, 0)
	buf := bufio.NewReader(dataFile)
	bm.InitSerial(&fnvHash64{Hash64: fnv.New64()})
	for {
		line, err := buf.ReadString('\n')
		if err != nil && err != io.EOF {
			t.Fatalf("ReadString Failed. %v", err)
		} else if err == io.EOF {
			break
		}
		if err = bm.SerialWrite(line); err != nil {
			t.Fatalf("Write Failed. %v", err)
		}
		urls = append(urls, line)
	}
	if err := bm.Flush(); err != nil {
		t.Fatalf("Flush Failed. %v", err)
	}

	validate(t, bm)
}

func Test_Parallel(t *testing.T) {
	bm, dataFile := test_init(t)
	defer bm.Close()
	defer dataFile.Close()

	urls := make([]string, 0)
	buf := bufio.NewReader(dataFile)
	hashs := []Hash{
		&fnvHash64{Hash64: fnv.New64()},
		&fnvHash64{Hash64: fnv.New64()},
		&fnvHash64{Hash64: fnv.New64()},
	}
	bm.InitParallel(100, 3, hashs)
	for {
		line, err := buf.ReadString('\n')
		if err != nil && err != io.EOF {
			t.Fatalf("ReadString Failed. %v", err)
		} else if err == io.EOF {
			break
		}
		if err = bm.ParallelWrite(line); err != nil {
			t.Fatalf("Write Failed. %v", err)
		}
		urls = append(urls, line)
	}
	if err := bm.Flush(); err != nil {
		t.Fatalf("Flush Failed. %v", err)
	}
	validate(t, bm)
}
