package storage

import (
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/cydev/stok/stokutils"
)

func TestBulk_Read(t *testing.T) {
	testBulkRead(t, []byte("Data data data data data!"))
}

func TestBulk_Read2xPoolBufferSize(t *testing.T) {
	d := []byte("Data data data data data!")
	buf := make([]byte, defaultByteBufferSize*2)
	copy(buf, d)
	testBulkRead(t, buf)
}

func testBulkRead(t *testing.T, data []byte) {
	backend := TempFile(t)
	defer ClearTempFile(backend, t)
	bulk := Bulk{Backend: backend}
	h := Header{
		Size:      len(data),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        0,
	}
	buf := NewHeaderBuffer()
	h.Put(buf)
	if _, err := backend.Write(buf); err != nil {
		t.Fatal("backend.Write", err)
	}
	if _, err := backend.Write(data); err != nil {
		t.Fatal("data.WriteTo", err)
	}
	if _, err := backend.Seek(0, os.SEEK_SET); err != nil {
		t.Fatal("backend.Seek", err)
	}
	l := Link{
		ID:     h.ID,
		Offset: 0,
	}
	hBuf := make([]byte, 0, h.Size)
	hRead, err := bulk.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	bulkBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bulkBuf)
	if err := bulk.ReadData(hRead, bulkBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	hBuf = bulkBuf.B
	if hRead != h {
		t.Errorf("%v != %v", hRead, h)
	}
	hBuf = hBuf[:hRead.Size]
	if len(hBuf) != hRead.Size {
		t.Errorf("len(hBuf) %d != %d", len(hBuf), hRead.Size)
	}
	if !reflect.DeepEqual(hBuf, data) {
		t.Errorf("%s != %s", string(hBuf), string(data))
	}
}

func testBulkWrite(t *testing.T, data []byte) {
	backend := TempFile(t)
	defer ClearTempFile(backend, t)
	bulk := Bulk{Backend: backend}
	h := Header{
		Size:      len(data),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        0,
	}
	if err := bulk.Write(h, data); err != nil {
		t.Fatal("bulk.Read", err)
	}
	l := Link{
		ID:     h.ID,
		Offset: 0,
	}
	hBuf := make([]byte, 0, h.Size)
	hRead, err := bulk.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	bulkBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bulkBuf)
	if err := bulk.ReadData(hRead, bulkBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	hBuf = bulkBuf.B
	if hRead != h {
		t.Errorf("%v != %v", hRead, h)
	}
	hBuf = hBuf[:hRead.Size]
	if len(hBuf) != hRead.Size {
		t.Errorf("len(hBuf) %d != %d", len(hBuf), hRead.Size)
	}
	if !reflect.DeepEqual(hBuf, data) {
		t.Errorf("%s != %s", string(hBuf), string(data))
	}
}

func TestBulk_Write1b(t *testing.T) {
	testBulkWrite(t, []byte("s"))
}

func TestBulk_Write(t *testing.T) {
	testBulkWrite(t, []byte(("Data data data data data!")))
}

func BenchmarkBulk_Read(b *testing.B) {
	var backend MemoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 0,
	}
	tmpHeader := Header{
		ID:        0,
		Offset:    0,
		Timestamp: time.Now().Unix(),
	}
	data := []byte("Data data data data data!")
	tmpHeader.Size = len(data)
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpHeader.Offset = id * (int64(tmpHeader.Size) + LinkStructureSize)
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		if _, err := backend.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
	}
	bulk := Bulk{Backend: &backend}
	l := Link{
		ID:     3,
		Offset: (int64(tmpHeader.Size) + LinkStructureSize) * 3,
	}
	bulkBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bulkBuf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fRead, err := bulk.ReadHeader(l, bulkBuf.B)
		if err != nil {
			b.Error("bulk.ReadInfo", err)
		}
		if err = bulk.ReadData(fRead, bulkBuf); err != nil {
			b.Error("bulk.Read", err)
		}
		if err != nil {
			b.Error(err)
		}
		bulkBuf.Reset()
	}
}
