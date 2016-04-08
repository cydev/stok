package storage

import (
	"bytes"
	"testing"

	"io"

	. "github.com/cydev/stok/stokutils"
)

func TestLink(t *testing.T) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	l.Put(buf)
	readL := Link{}
	readL.Read(buf)
	if l != readL {
		t.Errorf("%v != %v", readL, l)
	}
}

func BenchmarkLink_Put(b *testing.B) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Put(buf)
	}
}

func BenchmarkLink_Read(b *testing.B) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	l.Put(buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Read(buf)
	}
}

func TestGetLink(t *testing.T) {
	l := getLinkOffset(10)
	if l != 160 {
		t.Fatalf("%v != %v", l, 160)
	}
}

func TestIndex_ReadBuff(t *testing.T) {
	var backend MemoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			t.Fatal(err)
		}
	}
	backend.Buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	readBuf := make([]byte, LinkStructureSize)
	l, err := index.ReadBuff(3, readBuf)
	if err != nil {
		t.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}

func TestIndex_Read(t *testing.T) {
	var backend MemoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			t.Fatal(err)
		}
	}
	backend.Buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	l, err := index.ReadBuff(3, make([]byte, LinkStructureSize))
	if err != nil {
		t.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}

func BenchmarkIndex_ReadBuff(b *testing.B) {
	var backend MemoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			b.Fatal(err)
		}
	}
	backend.Buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	l, err := index.ReadBuff(3, make([]byte, LinkStructureSize))
	if err != nil {
		b.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		b.Errorf("%v != %v", l, expected)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := index.ReadBuff(3, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexOsFile(t *testing.T) {
	f := TempFile(t)
	defer ClearTempFile(f, t)
	index := Index{Backend: f}
	b := AcquireIndexBuffer()
	expected := Link{
		ID:     0,
		Offset: 1234,
	}
	if err := index.WriteBuff(expected, b.B); err != nil {
		t.Error(err)
	}
	l, err := index.ReadBuff(expected.ID, make([]byte, LinkStructureSize))
	if err != nil {
		t.Error(err)
	}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}

func TestIndex_ReadBuff_Error(t *testing.T) {
	var backend MemoryBackend
	if _, err := backend.WriteAt(make([]byte, LinkStructureSize), 0); err != nil {
		t.Fatal(err)
	}
	expected := BackendError(io.ErrUnexpectedEOF, AtIndex)
	backend.Err = expected.Err
	index := Index{Backend: &backend}
	buf := make([]byte, LinkStructureSize)
	_, err := index.ReadBuff(0, buf)
	bErr, ok := err.(ErrBackendFailed)
	if !ok {
		t.Error(err, "is not backend error")
	}
	if bErr != expected {
		t.Error(bErr, "!=", expected)
	}
}

func TestIndex_Preallocate_Error(t *testing.T) {
	var backend MemoryBackend
	expected := BackendError(io.ErrUnexpectedEOF, AtIndex)
	backend.Err = io.ErrUnexpectedEOF
	index := Index{Backend: &backend}
	err := index.Preallocate(128)
	bErr, ok := err.(ErrBackendFailed)
	if !ok {
		t.Error(err, "is not backend error")
	}
	if bErr != expected {
		t.Error(bErr, "!=", expected)
	}
}

func TestIndex_Preallocate(t *testing.T) {
	f := TempFile(t)
	defer ClearTempFile(f, t)
	index := Index{Backend: f}
	if err := index.Preallocate(128); err != nil {
		t.Fatal(err)
	}
}

func TestIndex_WriteBuff_Error(t *testing.T) {
	var backend MemoryBackend
	expected := BackendError(io.ErrUnexpectedEOF, AtIndex)
	backend.Err = io.ErrUnexpectedEOF
	index := Index{Backend: &backend}
	buf := make([]byte, LinkStructureSize)
	err := index.WriteBuff(Link{ID: 0, Offset: 1234}, buf)
	bErr, ok := err.(ErrBackendFailed)
	if !ok {
		t.Error(err, "is not backend error")
	}
	if bErr != expected {
		t.Error(bErr, "!=", expected)
	}
}

func TestIndex_NextID(t *testing.T) {
	f := TempFile(t)
	defer ClearTempFile(f, t)
	index := Index{Backend: f}
	for i := 0; i < 1000; i++ {
		_, err := index.NextID()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkIndex_NextID(b *testing.B) {
	f := TempFile(b)
	defer ClearTempFile(f, b)
	index := Index{Backend: f}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := index.NextID()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
