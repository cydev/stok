package storage

import (
	"crypto"
	"hash/crc32"
	"io"
	"testing"

	klaus32 "github.com/klauspost/crc32"
)

func benchmarkHash(b *testing.B, size int, writer io.Writer) {
	b.ReportAllocs()
	b.SetBytes(int64(size))
	d := make([]byte, size)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := writer.Write(d); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkSHA1_1KB(b *testing.B) {
	benchmarkHash(b, 1024, crypto.SHA1.New())
}

func BenchmarkSHA512_1KB(b *testing.B) {
	benchmarkHash(b, 1024, crypto.SHA512.New())
}

func BenchmarkCRC32_1KB(b *testing.B) {
	benchmarkHash(b, 1024, crc32.NewIEEE())
}

func BenchmarkKlausCRC32_1KB(b *testing.B) {
	benchmarkHash(b, 1024, klaus32.NewIEEE())
}
