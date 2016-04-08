package storage

import (
	"testing"
	"time"
)

func TestHeader(t *testing.T) {
	f := Header{
		ID:        1234,
		Length:    33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := HeaderStructureBuffer{}
	f.Put(buf[:])
	readF := Header{}
	readF.Read(buf[:])
	if f != readF {
		t.Errorf("%v != %v", readF, f)
	}
}

func BenchmarkHeader_Put(b *testing.B) {
	f := Header{
		ID:        1234,
		Length:    33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := HeaderStructureBuffer{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Put(buf[:])
		b.SetBytes(HeaderStructureSize)
	}
}

func BenchmarkHeader_Read(b *testing.B) {
	f := Header{
		ID:        1234,
		Length:    33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := HeaderStructureBuffer{}
	f.Put(buf[:])
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Read(buf[:])
		b.SetBytes(HeaderStructureSize)
	}
}
