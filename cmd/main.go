package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"topk/topk"
)

//	_ "net/http/pprof"

var (
	topN      int
	buckets   int
	maxMemory int64
	tempPath  string
	filePath  string
	outFile   string
	parallel  bool
)

//buffer size of bufio.Reader or bufio.Writer
const BUFFER_SIZE = 1024 * 1024 * 2

func init() {
	flag.IntVar(&topN, "k", 100, "top k")
	flag.StringVar(&filePath, "f", "", "data file path")
	flag.StringVar(&tempPath, "t", "./", "temp file storage location")
	flag.Int64Var(&maxMemory, "m", 1024, "max used memory. default 1G ")
	flag.StringVar(&outFile, "o", "./out", "output file")
	flag.BoolVar(&parallel, "p", false, "parallel dealing")
}

func writeConsole(tuples []*topk.Tuple) {
	for _, t := range tuples {
		line := fmt.Sprintln(strings.Trim(t.Key, "\n"), t.Count)
		fmt.Println(line)
	}
}

func writeOut(tuples []*topk.Tuple) error {
	file, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, t := range tuples {
		line := fmt.Sprintln(strings.Trim(t.Key, "\n"), t.Count)
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func main() {

	cpuf, err := os.Create("./cpu_profile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpuf)
	defer pprof.StopCPUProfile()

	memf, err := os.Create("./memory_profile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(memf)
	defer memf.Close()

	flag.Parse()
	if filePath == "" {
		flag.Usage()
		log.Fatal("must provide data file.")
	}
	maxMemory *= 1024 * 1024

	dataFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Open ", err)
	}
	defer dataFile.Close()

	fileInfo, err := dataFile.Stat()
	if err != nil {
		log.Fatal("Stat ", err)
	}

	buckets = 1
	fileSize := fileInfo.Size()
	if fileSize > maxMemory {
		buckets = int(fileSize/maxMemory) + 1
	}
	bufferSize := int(maxMemory / int64(buckets+2))
	if bufferSize > BUFFER_SIZE {
		bufferSize = BUFFER_SIZE
	}
	log.Println("buckets:", buckets, "BufferSize:", bufferSize)

	manager, err := topk.InitFileBucketManager(bufferSize, buckets, maxMemory, "./tmp")
	if err != nil {
		log.Fatal("InitFileBucketManager ", err)
	}
	defer manager.Close()

	reader := bufio.NewReaderSize(dataFile, 3*bufferSize)

	var tuples []*topk.Tuple
	cpus := runtime.NumCPU()
	if !parallel || buckets == 1 {
		log.Println("SerialSplit start")
		if err = topk.SerialSplit(manager, reader); err != nil {
			log.Fatal("SerialSplit", err)
		}
		log.Println("SerialSplit Done.")
		reader = nil

		tuples, err = topk.SerailTopK(manager, topN)
		log.Println("SerailTopK Done.")
	} else {
		log.Println("ParallelSplit start")
		err = topk.ParallelBatch(manager, reader, cpus-1)
		if err != nil {
			log.Fatal("ParallelSplit", err)
		}
		log.Println("ParallelSplit  Done.")
		tuples, err = topk.ParallelTopK(manager, topN, cpus)
		log.Println("ParallelTopK Done.")
	}

	if err != nil {
		log.Fatal("SerailTopK", err)
	}
	if err = writeOut(tuples); err != nil {
		writeConsole(tuples)
		log.Fatal("write file failed.", err)
	}
	log.Println("TopK Done.")
}
