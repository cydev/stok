// Package stokutils implements various utilities that
// help testing stok.
package stokutils

import (
	"bytes"
	"io/ioutil"
	"os"
	"time"
)

// IDEAWorkaround is workaround for go-lang-plugin-org/go-lang-idea-plugin#2439.
//
// Usage:
//     func TestMain(m *testing.M) {
//         code := m.Run()
//         IDEAWorkaround()
//         os.Exit(code)
//     }
func IDEAWorkaround() {
	// workaround for go-lang-plugin-org/go-lang-idea-plugin#2439
	if len(os.Getenv("IDEAWAIT")) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

// Fatalist is common interface for testing.T and testing.B.
type Fatalist interface {
	Fatal(args ...interface{})
	Error(args ...interface{})
}

// TempFile returns temporary file and calls t.Fatal if error.
func TempFile(t Fatalist) *os.File {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("tempFile:", err)
	}
	return f
}

// ClearTempFile closes and removes given file and calls t.Error
// on Close error and t.Fatal on Remove error.
func ClearTempFile(f *os.File, t Fatalist) {
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
