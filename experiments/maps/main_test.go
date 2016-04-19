package main

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func resetSlice(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func BenchmarkKeyGet(b *testing.B) {
	total = batchCount * batchSize
	if total == 0 {
		b.Fatal("total:", total)
	}
	db, err := leveldb.OpenFile(dbDir, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		kBuf := make([]byte, 8)
		for pb.Next() {
			resetSlice(kBuf)
			k := rand.Intn(int(total))
			binary.PutVarint(kBuf, int64(k))
			_, err := db.Get(kBuf, nil)
			if err != nil {
				b.Error(err, "key", k)
			}
		}
	})
}

func BenchmarkKeyGetLinear(b *testing.B) {
	total = batchCount * batchSize
	if total == 0 {
		b.Fatal("total:", total)
	}
	db, err := leveldb.OpenFile(dbDir, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	kBuf := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		resetSlice(kBuf)
		k := rand.Intn(int(total))
		binary.PutVarint(kBuf, int64(k))
		_, err := db.Get(kBuf, nil)
		if err != nil {
			b.Error(err, "key", k)
		}
	}
}
