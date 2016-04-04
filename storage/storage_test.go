package storage

import (
	"reflect"
	"testing"
	"time"
)

func TestVolume_ReadFile(t *testing.T) {
	iB := tempFile(t)
	defer clearTempFile(iB, t)
	bB := tempFile(t)
	defer clearTempFile(bB, t)

	v := Volume{
		Index: Index{
			Backend: iB,
		},
		Bulk: Bulk{
			Backend: bB,
		},
	}

	d := []byte("Data")
	h := Header{
		ID:        0,
		Size:      len(d),
		Offset:    0,
		Timestamp: time.Now().Unix(),
	}
	l := h.Link()

	buff := AcquireByteBuffer()
	defer ReleaseByteBuffer(buff)
	if err := v.Index.WriteBuff(l, buff.B[:LinkStructureSize]); err != nil {
		t.Error(err)
	}
	if err := v.Bulk.Write(h, d); err != nil {
		t.Error(err)
	}

	callback := func(rh Header, rd []byte) error {
		if !reflect.DeepEqual(rd, d) {
			t.Error("read data missmatch")
		}
		if rh != h {
			t.Error("header missmatch")
		}
		return nil
	}
	if err := v.ReadFile(h.ID, callback); err != nil {
		t.Error(err)
	}
}

func BenchmarkVolume_ReadFile70b(b *testing.B) {
	iB := tempFile(b)
	defer clearTempFile(iB, b)
	bB := tempFile(b)
	defer clearTempFile(bB, b)

	v := Volume{
		Index: Index{
			Backend: iB,
		},
		Bulk: Bulk{
			Backend: bB,
		},
	}

	d := []byte("Data data data data data data data data data data data data data data!")
	h := Header{
		ID:        0,
		Size:      len(d),
		Offset:    0,
		Timestamp: time.Now().Unix(),
	}
	l := h.Link()

	buff := AcquireByteBuffer()
	defer ReleaseByteBuffer(buff)
	if err := v.Index.WriteBuff(l, buff.B[:LinkStructureSize]); err != nil {
		b.Error(err)
	}
	if err := v.Bulk.Write(h, d); err != nil {
		b.Error(err)
	}

	callback := func(rh Header, rd []byte) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := v.ReadFile(h.ID, callback); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkVolume_ReadFile500kb(b *testing.B) {
	iB := tempFile(b)
	defer clearTempFile(iB, b)
	bB := tempFile(b)
	defer clearTempFile(bB, b)

	v := Volume{
		Index: Index{
			Backend: iB,
		},
		Bulk: Bulk{
			Backend: bB,
		},
	}

	d := make([]byte, defaultByteBufferSize+1024)
	copy(d, []byte("data"))
	h := Header{
		ID:        0,
		Size:      len(d),
		Offset:    0,
		Timestamp: time.Now().Unix(),
	}
	l := h.Link()

	buff := AcquireByteBuffer()
	defer ReleaseByteBuffer(buff)
	if err := v.Index.WriteBuff(l, buff.B[:LinkStructureSize]); err != nil {
		b.Error(err)
	}
	if err := v.Bulk.Write(h, d); err != nil {
		b.Error(err)
	}

	callback := func(rh Header, rd []byte) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := v.ReadFile(h.ID, callback); err != nil {
			b.Error(err)
		}
	}
}