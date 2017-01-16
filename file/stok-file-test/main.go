package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/cydev/stok/file"
)

const (
	maxSize = 1024 * 1024 * 200
)

type testBackend struct {
	size int64
	sync.Mutex
}

func (*testBackend) Close() error {
	return nil
}

func (b *testBackend) WriteAt(p []byte, off int64) (n int, err error) {
	mustCap := int64(len(p)) + off
	curSize := atomic.LoadInt64(&b.size)
	if curSize < mustCap {
		panic("overflow")
	}
	return len(p), nil
}

func (b *testBackend) ReadAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}

func (b *testBackend) Truncate(size int64) error {
	b.Lock()
	if size < atomic.LoadInt64(&b.size) {
		return errors.New("new size is smaller than previous")
	}
	atomic.StoreInt64(&b.size, size)
	b.Unlock()
	go fmt.Println("truncate", size)
	return nil
}

var (
	concurrency = flag.Int("c", 12, "concurrent goroutines")
)

func main() {
	flag.Parse()
	backend := new(testBackend)
	backend.size = 1024
	f := file.NewFile(file.Options{
		Capacity: 1024,
		Size:     0,
		Backend:  backend,
	})
	sizes := make(chan int)
	buf := make([]byte, 0, maxSize)
	for i := 0; i < *concurrency; i++ {
		go func() {
			for s := range sizes {
				if _, err := f.Append(buf[:s]); err != nil {
					panic(err)
				}
			}
		}()
	}
	for {
		sizes <- rand.Intn(maxSize)
	}
}
