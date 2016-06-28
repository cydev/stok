// Package stokutils implements various utilities that
// help testing stok.
package stokutils

import (
	"bytes"
	"io/ioutil"
	"os"
	"time"
	"testing"
	"io"
)

// TempFile returns temporary file and calls t.Fatal if error.
func TempFile(t testing.TB) *os.File {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("tempFile:", err)
	}
	return f
}

func MustClose(t testing.TB, c io.Closer) {
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

// TempFile returns temporary file and calls t.Fatal if error.
func TempFileClose(t testing.TB) (*os.File, func()) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("tempFile:", err)
	}
	callback := func() {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return f, callback
}

// ClearTempFile closes and removes given file and calls t.Error
// on Close error and t.Fatal on Remove error.
func ClearTempFile(f *os.File, t testing.TB) {
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Error(err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
}

// MemoryBackend is in-memory storage.Backend.
type MemoryBackend struct {
	FileName string
	Buff     bytes.Buffer
	Reader   bytes.Reader
	Err      error
}

// Truncate underlying buffer.
func (m MemoryBackend) Truncate(size int64) error {
	m.Buff.Grow(int(size))
	m.Reader = *bytes.NewReader(m.Buff.Bytes())
	return m.Err
}

// ReadAt from reader.
func (m MemoryBackend) ReadAt(b []byte, off int64) (int, error) {
	n, err := m.Reader.ReadAt(b, off)
	if err != nil {
		return n, err
	}
	return n, m.Err
}

// WriteAt to underlying buffer and rewrite Reader.
func (m *MemoryBackend) WriteAt(b []byte, off int64) (int, error) {
	n, err := m.Buff.Write(b)
	if err != nil {
		return n, err
	}
	m.Reader = *bytes.NewReader(m.Buff.Bytes())
	return n, m.Err
}

// Stat returns self (MemoryBackend) and nil error.
func (m MemoryBackend) Stat() (os.FileInfo, error) {
	return m, m.Err
}

// Name returns FileName field value.
func (m MemoryBackend) Name() string {
	return m.FileName
}

// Size returns length of underlying buffer.
func (m MemoryBackend) Size() int64 {
	return int64(m.Buff.Len())
}

// Mode is always 0666.
func (m MemoryBackend) Mode() os.FileMode {
	return os.FileMode(0666)
}

// IsDir is always false.
func (m MemoryBackend) IsDir() bool {
	return false
}

// Sys returns underlying buffer.
func (m MemoryBackend) Sys() interface{} {
	return m.Buff
}

// ModTime is always zero time.
func (m MemoryBackend) ModTime() time.Time {
	return time.Time{}
}

func (m MemoryBackend) Close() error {
	return nil
}

// ZeroReaders implements Reader that returns length and nil error.
type ZeroReader struct {}

func (z ZeroReader) Read(b []byte) (int, error) {
	return len(b), nil
}

func (z ZeroReader) ReadAt(b []byte, off int64) (int, error) {
	return z.Read(b)
}

func (z ZeroReader) Write(b []byte) (int, error) {
	return len(b), nil
}

func (z ZeroReader) WriteAt(b []byte, off int64) (int, error) {
	return z.Write(b)
}

func (z ZeroReader) Close() error {
	return nil
}

func (z ZeroReader) Truncate(int64) error {
	return nil
}

// Zeroes is default ZeroReader
var Zeroes = ZeroReader{}
