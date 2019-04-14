package topk

import (
	"errors"
	"fmt"
	"log"
	"os"
)

type hash_item struct {
	str   string
	index int
}

// split big file into small file
type BucketsManager struct {
	Number  int
	hash    Hash
	Buckets map[int]Bucket

	maxSize       int64
	hashWorking   int
	waitHashChan  chan string
	writeErr      error
	writeDone     chan bool
	waitWriteChan chan *hash_item

	isParallel bool
}

// multiple goroutines calc hash and write file
// too fierce for chan
func (bm *BucketsManager) InitParallel(chanSize int, hashWorking int, hashs []Hash) {
	bm.writeErr = nil
	bm.isParallel = true
	bm.writeDone = make(chan bool, 1)
	bm.waitHashChan = make(chan string, chanSize)

	bm.hashWorking = hashWorking
	if len(hashs) != hashWorking {
		log.Fatal("hash object != hashWorking")
	}

	hashFunc := func(hash Hash) {
		for {
			str, ok := <-bm.waitHashChan
			if !ok {
				bm.hashWorking--
				if bm.hashWorking == 0 {
					bm.writeDone <- true
				}
				return
			}
			index := hash.HashU64(str) % uint64(bm.Number)
			bm.writeErr = bm.Buckets[int(index)].Write(str)
		}
	}

	for i := 0; i < bm.hashWorking; i++ {
		go hashFunc(hashs[i])
	}
}

func (bm *BucketsManager) InitSerial(hash Hash) {
	bm.hash = hash
	bm.isParallel = false
}

func (bm *BucketsManager) ParallelWrite2(urls string, hash Hash) error {
	index := hash.HashU64(urls) % uint64(bm.Number)
	return bm.Buckets[int(index)].Write(urls)
}

// too fierce for chan
func (bm *BucketsManager) ParallelWrite(urls string) error {
	if bm.writeErr != nil {
		return bm.writeErr
	}
	bm.waitHashChan <- urls
	return nil
}

func (bm *BucketsManager) SerialWrite(urls string) error {
	hash := bm.hash.HashU64(urls)
	index := hash % uint64(bm.Number)
	return bm.Buckets[int(index)].Write(urls)
}

func (bm *BucketsManager) Flush() error {
	if bm.isParallel {
		close(bm.waitHashChan)
		<-bm.writeDone
	}
	for _, f := range bm.Buckets {
		if err := f.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (bm *BucketsManager) Close() {
	for _, b := range bm.Buckets {
		b.Close()
	}
}

func InitFileBucketManager(bufferSize int, bucketSize int,
	maxSize int64, tmp string) (mg *BucketsManager, err error) {

	mg = new(BucketsManager)
	mg.maxSize = maxSize
	mg.Number = bucketSize
	mg.Buckets = make(map[int]Bucket, 0)

	defer func() {
		if err != nil {
			if mg != nil && mg.Buckets != nil {
				for _, f := range mg.Buckets {
					f.Close()
				}
			}
		}
	}()

	if fStat, err := os.Stat(tmp); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err = os.Mkdir(tmp, os.ModePerm); err != nil {
			return nil, err
		}
	} else if !fStat.IsDir() {
		return nil, errors.New("temp path is file.")
	}

	var f *FileBucket
	for i := 0; i < bucketSize; i++ {
		f, err = InitFileBucket(fmt.Sprint(tmp, "/", i), bufferSize)
		if err != nil {
			return
		}
		mg.Buckets[i] = f
	}
	return
}
