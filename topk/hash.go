package topk

import (
	"hash"
	"hash/fnv"
	"io"
)

type Hash interface {
	HashU64(string) uint64
}

type fnvHash64 struct {
	hash.Hash64
}

func (f *fnvHash64) HashU64(str string) uint64 {
	f.Reset()
	f.Write([]byte(str))
	return f.Sum64()
}

//calc the hash value of the last 40 characters
func Suffix40Hash64(content string) uint64 {
	h64 := fnv.New64()
	start := len(content) - 40
	if start < 0 {
		start = 40
	}
	io.WriteString(h64, content[start:len(content)-1])
	return h64.Sum64()
}
