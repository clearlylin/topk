package topk

import (
	"bufio"
	"container/heap"
	"hash/fnv"
	"io"
	"time"
)

func ParallelTopK(bm *BucketsManager, topN int, tasks int) ([]*Tuple, error) {

	var topkErr error
	indexChan := make(chan int, 1)
	topkTuplesChans := make(chan []*Tuple, len(bm.Buckets))

	indexChan <- 0
	topkWorker := func(length, topN int) {
		for {
			index, ok := <-indexChan
			if !ok {
				return
			}
			if index == length {
				close(indexChan)
				return
			} else {
				indexChan <- (index + 1)
			}

			tuples, err := bm.Buckets[index].TopK(topN)
			if err != nil {
				topkErr = err
				close(indexChan)
				close(topkTuplesChans)
				return
			}
			topkTuplesChans <- tuples
			if index == length-1 {
				close(topkTuplesChans)
				return
			}
		}
	}

	tk := InitTopKHeap(topN)
	mergeDone := make(chan bool, 1)
	heap.Init(tk)
	mergeWork := func() {
		for {
			tuples, ok := <-topkTuplesChans
			if ok {
				for _, t := range tuples {
					heap.Push(tk, t)
				}
				continue
			}
			mergeDone <- true
			return
		}
	}
	go mergeWork()

	for i := 0; i < tasks; i++ {
		go topkWorker(len(bm.Buckets), topN)
	}
	<-mergeDone

	if topkErr != nil {
		return nil, topkErr
	}

	index := topN - 1
	result := make([]*Tuple, topN)
	for tk.Len() > 0 {
		result[index] = heap.Pop(tk).(*Tuple)
		index--
	}
	return result, nil
}

func SerailTopK(bm *BucketsManager, topN int) ([]*Tuple, error) {
	tk := InitTopKHeap(topN)
	heap.Init(tk)
	for _, fb := range bm.Buckets {
		tuples, err := fb.TopK(topN)
		if err != nil {
			return nil, err
		}
		for _, t := range tuples {
			heap.Push(tk, t)
		}
	}

	index := topN - 1
	result := make([]*Tuple, topN)
	for tk.Len() > 0 {
		result[index] = heap.Pop(tk).(*Tuple)
		index--
	}
	return result, nil
}

const BATCH_SIZE = 700000

func ParallelBatch(bm *BucketsManager, reader *bufio.Reader, tasks int) error {

	hash := make([]Hash, 0)
	working := make(map[int]bool, tasks)
	for i := 0; i < tasks; i++ {
		working[i] = false
		hash = append(hash, &fnvHash64{Hash64: fnv.New64()})
	}

	batchWorker := func(lines []string, index int) {
		working[index] = true
		for _, line := range lines {
			bm.ParallelWrite2(line, hash[index])
		}
		working[index] = false
	}
	bm.InitSerial(&fnvHash64{Hash64: fnv.New64()})

	batchSize := 0
	batch := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if batchSize == BATCH_SIZE {
			running := false
			for key, run := range working {
				if !run {
					go batchWorker(batch, key)
					running = true
					break
				}
			}
			if !running {
				for _, line := range batch {
					if err = bm.SerialWrite(line); err != nil {
						return err
					}
				}
			}
			batchSize = 0
			batch = make([]string, 0)
			continue
		}
		batchSize++
		batch = append(batch, line)
	}

	for _, line := range batch {
		if err := bm.SerialWrite(line); err != nil {
			return err
		}
	}
	for {
		hasRunning := false
		for _, running := range working {
			if running {
				time.Sleep(time.Second * 2)
				hasRunning = true
				break
			}
		}
		if !hasRunning {
			break
		}
	}
	return bm.Flush()
}

func ParallelSplit(bm *BucketsManager, reader *bufio.Reader, chanSize, tasks int) error {
	hash := make([]Hash, 0)
	for i := 0; i < tasks; i++ {
		hash = append(hash, &fnvHash64{Hash64: fnv.New64()})
	}
	var lineSize uint64 = 0
	bm.InitSerial(&fnvHash64{Hash64: fnv.New64()})
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		lineSize++
		if lineSize%2 == 0 {
			//测试
			go bm.ParallelWrite2(line, hash[0])
			continue
		}
		if err = bm.SerialWrite(line); err != nil {
			return err
		}
	}

	time.Sleep(time.Second)

	return bm.Flush()
}

func SerialSplit(bm *BucketsManager, reader *bufio.Reader) error {
	bm.InitSerial(&fnvHash64{Hash64: fnv.New64()})
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err = bm.SerialWrite(line); err != nil {
			return err
		}
	}

	return bm.Flush()
}
