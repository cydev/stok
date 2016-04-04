package storage

import (
	"bytes"
	"testing"

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
	b := NewLinkBuffer()
	expected := Link{
		ID:     0,
		Offset: 1234,
	}
	if err := index.WriteBuff(expected, b); err != nil {
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
