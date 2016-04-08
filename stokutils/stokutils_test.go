package stokutils

import (
	"testing"
)

func TestZeroes_Read(t *testing.T) {
	b := make([]byte, 128)
	n, err := Zeroes.Read(b)
	if err != nil {
		t.Error(err)
	}
	if n != len(b) {
		t.Error(n, "!=", len(b))
	}
}

func BenchmarkZeroes_Read(b *testing.B) {
	d := make([]byte, 128)
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(128)
	for i := 0; i < b.N; i++ {
		Zeroes.Read(d)
	}
}

