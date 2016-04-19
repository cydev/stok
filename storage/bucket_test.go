package storage

import (
	"os"
	"reflect"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	. "github.com/cydev/stok/stokutils"
)

func TestBucket_Allocate(t *testing.T) {
	backend := TempFile(t)
	defer ClearTempFile(backend, t)
	bucket := Bucket{Backend: backend}

	for i := int64(1); i < 10; i++ {
		off, err := bucket.Allocate(128 + i)
		if err != nil {
			t.Error(err)
		}
		log.WithFields(log.Fields{
			"offset": off,
			"end":    128 + i + off,
			"cap":    bucket.Capacity,
		}).Info("allocated")
	}

}

func TestBucket_Read(t *testing.T) {
	testBucketRead(t, []byte("Data data data data data!"))
}

func TestBucket_Read2xPoolBufferSize(t *testing.T) {
	d := []byte("Data data data data data!")
	buf := make([]byte, defaultByteBufferSize*2)
	copy(buf, d)
	testBucketRead(t, buf)
}

func testBucketRead(t *testing.T, data []byte) {
	backend := TempFile(t)
	defer ClearTempFile(backend, t)
	bucket := Bucket{Backend: backend}
	h := Header{
		Length:    len(data),
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
	hBuf := make([]byte, 0, h.Length)
	hRead, err := bucket.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bucket.ReadInfo", err)
	}
	bucketBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bucketBuf)
	if err := bucket.ReadData(hRead, bucketBuf); err != nil {
		t.Error("bucket.Read", err)
	}
	hBuf = bucketBuf.B
	if hRead != h {
		t.Errorf("%v != %v", hRead, h)
	}
	hBuf = hBuf[:hRead.Length]
	if len(hBuf) != hRead.Length {
		t.Errorf("len(hBuf) %d != %d", len(hBuf), hRead.Length)
	}
	if !reflect.DeepEqual(hBuf, data) {
		t.Errorf("%s != %s", string(hBuf), string(data))
	}
}

func testBucketWrite(t *testing.T, data []byte) {
	backend := TempFile(t)
	defer ClearTempFile(backend, t)
	bucket := Bucket{Backend: backend}
	h := Header{
		Length:    len(data),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        0,
	}
	if err := bucket.Write(h, data); err != nil {
		t.Fatal("bucket.Read", err)
	}
	l := Link{
		ID:     h.ID,
		Offset: 0,
	}
	hBuf := make([]byte, 0, h.Length)
	hRead, err := bucket.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bucket.ReadInfo", err)
	}
	bucketBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bucketBuf)
	if err := bucket.ReadData(hRead, bucketBuf); err != nil {
		t.Error("bucket.Read", err)
	}
	hBuf = bucketBuf.B
	if hRead != h {
		t.Errorf("%v != %v", hRead, h)
	}
	hBuf = hBuf[:hRead.Length]
	if len(hBuf) != hRead.Length {
		t.Errorf("len(hBuf) %d != %d", len(hBuf), hRead.Length)
	}
	if !reflect.DeepEqual(hBuf, data) {
		t.Errorf("%s != %s", string(hBuf), string(data))
	}
}

func TestBucket_Write1b(t *testing.T) {
	testBucketWrite(t, []byte("s"))
}

func TestBucket_Write(t *testing.T) {
	testBucketWrite(t, []byte(("Data data data data data!")))
}

func BenchmarkBucket_Read(b *testing.B) {
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
	tmpHeader.Length = len(data)
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpHeader.Offset = id * (int64(tmpHeader.Length) + LinkStructureSize)
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		if _, err := backend.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
	}
	bucket := Bucket{Backend: &backend}
	l := Link{
		ID:     3,
		Offset: (int64(tmpHeader.Length) + LinkStructureSize) * 3,
	}
	bucketBuf := AcquireByteBuffer()
	defer ReleaseByteBuffer(bucketBuf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fRead, err := bucket.ReadHeader(l, bucketBuf.B)
		if err != nil {
			b.Error("bucket.ReadInfo", err)
		}
		if err = bucket.ReadData(fRead, bucketBuf); err != nil {
			b.Error("bucket.Read", err)
		}
		if err != nil {
			b.Error(err)
		}
		bucketBuf.Reset()
	}
}
