package topk

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type FileBucket struct {
	Path string

	mutex  *sync.Mutex
	file   *os.File
	reader *bufio.Reader
	writer *bufio.Writer
}

func (f *FileBucket) Size() (int64, error) {
	info, err := f.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (f *FileBucket) Write(content string) (err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	_, err = f.writer.WriteString(content)
	return
}

func (f *FileBucket) Flush() (err error) {
	if err = f.writer.Flush(); err != nil {
		log.Println("Writer Flush ", f.Path, err)
		return
	}
	return f.file.Sync()
}

func (f *FileBucket) Close() error {
	f.writer.Flush()
	return f.file.Close()
}

func (f *FileBucket) ReadLine() (string, error) {
	return f.reader.ReadString('\n')
}

func (f *FileBucket) TopK2(k int) (*TupleTopKHeap, error) {
	countMap := make(map[string]uint64)
	if _, err := f.file.Seek(0, 0); err != nil {
		return nil, err
	}
	for {
		line, err := f.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		_, ok := countMap[line]
		if ok {
			countMap[line] += 1
			continue
		}
		countMap[line] = 1
	}

	tuples := make([]*Tuple, 0)
	for k, v := range countMap {
		tuples = append(tuples, &Tuple{Key: k, Count: v})
	}

	countMap = nil
	tk := InitTopKHeap(k)
	heap.Init(tk)
	for _, t := range tuples {
		heap.Push(tk, t)
	}

	return tk, nil
}

func (f *FileBucket) TopK(k int) ([]*Tuple, error) {
	countMap := make(map[string]uint64)
	if _, err := f.file.Seek(0, 0); err != nil {
		return nil, err
	}
	for {
		line, err := f.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		_, ok := countMap[line]
		if ok {
			countMap[line] += 1
			continue
		}
		countMap[line] = 1
	}

	tuples := make([]*Tuple, 0)
	for k, v := range countMap {
		tuples = append(tuples, &Tuple{Key: k, Count: v})
	}

	countMap = nil
	tk := InitTopKHeap(k)
	heap.Init(tk)
	for _, t := range tuples {
		heap.Push(tk, t)
	}

	tuples = nil
	//result := make([]*Tuple, 0)
	//for tk.Len() > 0 {
	//	result = append(result, heap.Pop(tk).(*Tuple))
	//}
	return tk.Tuples, nil
}

func (f *FileBucket) Statistic() ([]*Tuple, int64, error) {
	var size int64 = 4
	countMap := make(map[string]uint64)
	if _, err := f.file.Seek(0, 0); err != nil {
		return nil, size, err
	}
	for {
		line, err := f.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, size, err
		} else if err == io.EOF {
			break
		}

		_, ok := countMap[line]
		if ok {
			countMap[line] += 1
			continue
		}
		size += int64(len(line))
		countMap[line] = 1
	}

	tuples := make([]*Tuple, 0)
	for k, v := range countMap {
		tuples = append(tuples, &Tuple{Key: k, Count: v})
	}
	return tuples, size, nil
}

func (f *FileBucket) FileTuplesTopKSort(tt *TupleTopKHeap) ([]*Tuple, error) {
	if _, err := f.file.Seek(0, 0); err != nil {
		return nil, err
	}
	heap.Init(tt)
	for {
		line, err := f.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		items := strings.Split(line, " ")
		count, _ := strconv.ParseInt(items[1], 10, 64)
		heap.Push(tt, &Tuple{Key: items[0], Count: uint64(count)})
	}
	result := make([]*Tuple, 0)
	for tt.Len() > 0 {
		result = append(result, heap.Pop(tt).(*Tuple))
	}
	return result, nil
}

func (f *FileBucket) WriteSortFile(data []*Tuple) (err error) {

	if err = f.file.Truncate(0); err != nil {
		return
	}
	if err = f.file.Sync(); err != nil {
		return
	}
	if _, err = f.file.Seek(0, 0); err != nil {
		return
	}

	for _, t := range data {
		f.writer.WriteString(fmt.Sprintln(t.Key, t.Count))
	}
	f.writer.Flush()
	return nil
}

func InitFileBucket(path string, bufferSize int) (*FileBucket, error) {
	f := new(FileBucket)
	f.Path = path
	f.mutex = new(sync.Mutex)

	var err error
	f.file, err = os.Create(path)
	if err == nil {
		f.writer = bufio.NewWriterSize(f.file, bufferSize)
		f.reader = bufio.NewReaderSize(f.file, bufferSize)
	}
	return f, err
}
