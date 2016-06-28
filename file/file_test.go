package file

import (
	"math/rand"
	"sync"
	"testing"

	"os"

	"github.com/cydev/stok/stokutils"
)

func Test_WriteAt(t *testing.T) {
	f, close := stokutils.TempFileClose(t)
	defer close()

	b := make([]byte, 128)
	if _, err := f.WriteAt(b, 1024); err != nil {
		t.Error(err)
	}
}

func TestHeader_Decode(t *testing.T) {
	h := header{Size: 104005}
	buf := h.Append(nil)
	h2 := header{}
	if _, err := h2.Decode(buf); err != nil {
		t.Error(err)
	}
	if h != h2 {
		t.Error("not equal")
	}
	buf[2] = buf[2] + 1
	if _, err := h.Decode(buf); err == nil {
		t.Error(err, "should not be nil")
	}
}

func BenchmarkHeader_Append(b *testing.B) {
	b.ReportAllocs()
	h := header{Size: 1041234}
	buf := h.Append(nil)[:0]
	for i := 0; i < b.N; i++ {
		buf = h.Append(buf)[:0]
	}
}

func BenchmarkFile_WriteHeader(b *testing.B) {
	b.ReportAllocs()
	f := File{
		h: header{1234},
		f: stokutils.Zeroes,
	}
	for i := 0; i < b.N; i++ {
		f.writeHeader()
	}
}

func TestFile_AppendParallel(t *testing.T) {
	rand.Seed(666)
	var (
		max   int64
		sum   int64
		sizes []int64

		workers = 20
		count   = 1024
		cSizes  = make(chan int64, count)
	)
	for i := 0; i < count; i++ {
		size := rand.Int63n(1024 * 1024)
		sum += size
		if size > max {
			max = size
		}
		sizes = append(sizes, size)
	}
	f := &File{
		f:        stokutils.Zeroes,
		capacity: 1024,
	}
	wg := new(sync.WaitGroup)
	w := func(jobs <-chan int64, ff *File) {
		buf := make([]byte, 0, max)
		for j := range jobs {
			if _, err := ff.Append(buf[:j]); err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go w(cSizes, f)
	}
	for _, v := range sizes {
		cSizes <- v
	}
	close(cSizes)
	wg.Wait()

	if f.capacity < int64(sum) {
		t.Errorf("capacity %s is < %d", f.capacity, sum)
	}

	if f.size != int64(sum) {
		t.Errorf("size %d is != %d", f.size, sum)
	}
}

func TestNew(t *testing.T) {
	f := stokutils.TempFile(t)
	n := f.Name()
	ff, err := New(f)
	if err != nil {
		t.Error(err)
	}
	if s := ff.Size(); s != 0 {
		t.Error("size", s)
	}
	ff.Append(make([]byte, 5012))
	if s := ff.Size(); s != 5012 {
		t.Error("size", s)
	}
	buf := make([]byte, 1024)
	if _, err := ff.ReadAt(buf, 2048); err != nil {
		t.Error("read", err)
	}
	if err = ff.Close(); err != nil {
		t.Error(err)
	}
	f, err = os.Open(n)
	if err != nil {
		t.Error(err)
	}
	ff, err = New(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = ff.Close(); err != nil {
		t.Error(err)
	}
}

func TestStartAlloc(t *testing.T) {
	f := &File{
		f: stokutils.Zeroes,
	}
	f.alloc(1025)
	if f.capacity != 2048 {
		t.Error(f.capacity, "bad")
	}
}

func BenchmarkFile_Append(b *testing.B) {
	b.ReportAllocs()
	f := File{
		f:        stokutils.Zeroes,
		capacity: 1024,
	}
	buf := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		f.Append(buf)
		if f.capacity > 6500054353 {
			f.capacity = 1024
			f.h.Size = 0
		}
	}
}
