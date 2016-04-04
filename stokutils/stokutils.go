package stokutils

import (
	"bytes"
	"io/ioutil"
	"os"
	"time"
)

func IDEAWorkaround() {
	// workaround for go-lang-plugin-org/go-lang-idea-plugin#2439
	if len(os.Getenv("IDEAWAIT")) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

type Fatalist interface {
	Fatal(args ...interface{})
	Error(args ...interface{})
}

func TempFile(t Fatalist) *os.File {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("tempFile:", err)
	}
	return f
}

func ClearTempFile(f *os.File, t Fatalist) {
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Error(err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
}

// MemoryBackend is in-memory storage.Backend
type MemoryBackend struct {
	FileName string
	Buff     bytes.Buffer
	Reader   bytes.Reader
}

func (m MemoryBackend) Truncate(size int64) error {
	m.Buff.Grow(int(size))
	m.Reader = *bytes.NewReader(m.Buff.Bytes())
	return nil
}

func (m MemoryBackend) ReadAt(b []byte, off int64) (int, error) {
	return m.Reader.ReadAt(b, off)
}

func (m *MemoryBackend) WriteAt(b []byte, off int64) (int, error) {
	n, err := m.Buff.Write(b)
	if err != nil {
		return n, err
	}
	m.Reader = *bytes.NewReader(m.Buff.Bytes())
	return n, nil
}

func (m MemoryBackend) Stat() (os.FileInfo, error) {
	return m, nil
}

func (m MemoryBackend) Name() string {
	return m.FileName
}

func (m MemoryBackend) Size() int64 {
	return int64(m.Buff.Len())
}

func (m MemoryBackend) Mode() os.FileMode {
	return os.FileMode(0666)
}

func (m MemoryBackend) IsDir() bool {
	return false
}

func (m MemoryBackend) Sys() interface{} {
	return m.Buff
}

func (m MemoryBackend) ModTime() time.Time {
	return time.Time{}
}
