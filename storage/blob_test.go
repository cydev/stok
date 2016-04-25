package storage

import (
	"math/rand"
	"sync"
	"testing"

	. "github.com/cydev/stok/stokutils"
)

func newFileBlob(backend BlobBackend) (*Blob, error) {
	statBackend := backend.(StatBackend)
	info, err := statBackend.Stat()
	if err != nil {
		return nil, err
	}
	return &Blob{
		Size:     0,
		Capacity: info.Size(),
		Backend:  backend,
	}, nil
}

func TestNewFileBlob(t *testing.T) {
	f, fClose := TempFileClose(t)
	defer fClose()
	b, err := newFileBlob(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Truncate(1024); err != nil {
		t.Error(err)
	}
	if b.Size != blobHeaderSize {
		t.Error("size not header")
	}
	if b.Capacity != 1024 {
		t.Error("capacity not 1024")
	}
	if err := b.Sync(); err != nil {
		t.Error(err)
	}
}

func TestNewFileBlobTruncateError(t *testing.T) {
	f, fClose := TempFileClose(t)
	b, err := newFileBlob(f)
	if err != nil {
		t.Fatal(err)
	}
	fClose()
	if err := b.Truncate(1024); err == nil {
		t.Error("Truncate shoud return error")
	}
}

func TestNewFileBlobStatError(t *testing.T) {
	f, fClose := TempFileClose(t)
	fClose()
	if _, err := newFileBlob(f); err == nil {
		t.Error("NewFileBlob should error")
	}
}

func TestOpenBlob(t *testing.T) {
	f := TempFile(t)
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Error(err)
	}
	b, err := OpenBlob(name, nil)
	if err != nil {
		t.Error(err)
	}
	if b.Capacity != DefaultBlobSize {
		t.Error("wrong capacity")
	}
	if b.Size != blobHeaderSize {
		t.Error("wrong size")
	}
	data := []byte("data is good")
	size := int64(len(data))
	offset, err := b.Allocate(size)
	if err != nil {
		t.Error(err)
	}
	if offset != blobHeaderSize {
		t.Error("wrong offset")
	}
	n, err := b.Backend.WriteAt(data, offset)
	if err != nil {
		t.Error(err)
	}
	if n != len(data) {
		t.Error("wrong length")
	}
	if b.Size != offset+int64(len(data)) {
		t.Error("wrong size")
	}
	if err = b.Sync(); err != nil {
		t.Error(err)
	}
	buff := make([]byte, len(data))
	if _, err = b.Backend.ReadAt(buff, offset); err != nil {
		t.Error(err)
	}
	if string(buff) != string(data) {
		t.Error("data corrupted!")
	}
	if err = b.Close(); err != nil {
		t.Error(err)
	}
	if b, err = OpenBlob(name, nil); err != nil {
		t.Error(err)
	}
	defer MustClose(t, b)
	if b.Capacity != DefaultBlobSize {
		t.Error("wrong capacity")
	}
	if b.Size != offset+int64(len(data)) {
		t.Error("wrong size")
	}
}

func TestOpenBlobBad(t *testing.T) {
	f := TempFile(t)
	name := f.Name()
	buf := make([]byte, blobHeaderSize)
	rand.Seed(666)
	if _, err := rand.Read(buf); err != nil {
		t.Error(err)
	}
	if _, err := f.Write(buf); err != nil {
		t.Error(err)
	}
	if err := f.Close(); err != nil {s
		t.Error(err)
	}
	_, err := OpenBlob(name, nil)
	if err != ErrBadHeader {
		t.Error(err, "shoud be", ErrBadHeader)
	}
}

func MustBlob(t testing.TB, path string) *Blob {
	b, err := OpenBlob(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBlob_Allocate(t *testing.T) {
	rand.Seed(666)
	sizes := make([]int64, 128)
	sum := int64(0)
	for i := range sizes {
		sizes[i] = int64(rand.Intn(512) + 256)
		sum += sizes[i]
	}
	f := TempFile(t)
	name := f.Name()
	MustClose(t, f)
	b := MustBlob(t, name)
	defer MustClose(t, b)
	wg := new(sync.WaitGroup)
	worker := func(s chan int64) {
		buf := make([]byte, 1024)
		for size := range s {
			offset, err := b.Allocate(size)
			if err != nil {
				t.Error(err)
			}
			if _, err := b.Backend.WriteAt(buf[:size], offset); err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}
	workers := 24
	sizesChan := make(chan int64)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker(sizesChan)
	}
	for _, size := range sizes {
		sizesChan <- size
	}
	close(sizesChan)
	wg.Wait()
	if err := b.Sync(); err != nil {
		t.Error(err)
	}
	mustSize := sum + blobHeaderSize
	if mustSize != b.Size {
		t.Error("expected size", mustSize, "got", b.Size)
	}
}

func BenchmarkBlob_Write(b *testing.B) {
	f := TempFile(b)
	name := f.Name()
	MustClose(b, f)
	blob := MustBlob(b, name)
	defer MustClose(b, blob)
	b.ReportAllocs()
	size := int64(1024)
	b.SetBytes(size)
	b.RunParallel(func(p *testing.PB) {
		buf := make([]byte, int(size))
		for p.Next() {
			offset, err := blob.Allocate(size)
			if err != nil {
				b.Error(err)
			}
			if _, err := blob.Backend.WriteAt(buf, offset); err != nil {
				b.Error(err)
			}
		}
	})
}

func TestBlobHeader_Read(t *testing.T) {
	buf := make([]byte, blobHeaderSize)
	header := BlobHeader{
		Size:     1024,
		Capacity: 2048,
	}
	header.Put(buf)
	buf[len(blobHeaderMagic)+2]++
	if err := header.Read(buf); err != ErrBadHeaderCRC {
		t.Error("Expected", ErrBadHeaderCRC, "but got", err)
	}
}

func TestBlobHeader_ReadPut(t *testing.T) {
	buf := make([]byte, blobHeaderSize)
	header := BlobHeader{
		Size:     12344,
		Capacity: 51448,
	}
	header.Put(buf)
	newHeader := BlobHeader{}
	if err := newHeader.Read(buf); err != nil {
		t.Error(err)
	}
	if header != newHeader {
		t.Error(header, "!=", newHeader)
	}
}

func TestBlobConfig(t *testing.T) {
	var conf *BlobConfig
	if conf.GetInitialSize() != DefaultBlobSize {
		t.Error("incorrect initial size")
	}
	conf = new(BlobConfig)
	conf.InitialSize = DefaultBlobSize * 2
	if conf.GetInitialSize() != DefaultBlobSize*2 {
		t.Error("incorrect configured initial size")
	}
}
