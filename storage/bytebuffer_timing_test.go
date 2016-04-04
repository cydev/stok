package storage

import (
	"bytes"
	"testing"
)

func BenchmarkByteBufferWrite(b *testing.B) {
	s := []byte("foobarbaz")
	b.RunParallel(func(pb *testing.PB) {
		var buf ByteBuffer
		for pb.Next() {
			for i := 0; i < 100; i++ {
				if _, err := buf.Write(s); err != nil {
					b.Error(err)
				}
			}
			buf.Reset()
		}
	})
}

func BenchmarkBytesBufferWrite(b *testing.B) {
	s := []byte("foobarbaz")
	b.RunParallel(func(pb *testing.PB) {
		var buf bytes.Buffer
		for pb.Next() {
			for i := 0; i < 100; i++ {
				if _, err := buf.Write(s); err != nil {
					b.Error(err)
				}
			}
			buf.Reset()
		}
	})
}
