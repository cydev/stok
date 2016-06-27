package index

import "testing"
import "github.com/cydev/stok/stokutils"

func BenchmarkReaderAtIndex_Get(b *testing.B) {
	b.ReportAllocs()
	var (
		err error
	)
	index := RWAtIndex{
		Backend: stokutils.Zeroes,
		Size:    128,
	}
	buf := make([]byte, index.Size)
	for i := 0; i < b.N; i++ {
		err = index.Get(123, buf)
		if err != nil {
			b.Fatal(err)
		}
		buf = buf[:0]
	}
}

func TestIterator_All(t *testing.T) {
	var (
		count = 16
		size  = 512
		back  = new(stokutils.MemoryBackend)
	)
	index := &RWAtIndex{
		Backend: back,
		Size:    size,
	}
	iterator := &Iterator{
		Size:  size,
		Index: index,
	}
	buf := make([]byte, size)
	for i := StartID; i < int64(count); i++ {
		if err := index.Set(i, buf); err != nil {
			t.Error(err)
		}
	}
	index.Length = 16
	var (
		countRead int
	)
	w := func(k int64, b []byte) error {
		countRead++
		return nil
	}
	if err := iterator.All(w); err != nil {
		t.Error(err)
	}
	if count != countRead {
		t.Error(count, "!=", countRead)
	}
}
