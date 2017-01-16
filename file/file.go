// Package file implements auto-extending file.
package file

import (
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cydev/stok/binary"
	"github.com/pkg/errors"
)

func New(f *os.File) (*File, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat")
	}
	cap_ := info.Size()
	if cap_ < headerSize {
		return create(f)
	}
	ff := new(File)
	ff.buf = make([]byte, headerSize)
	if _, err = f.ReadAt(ff.buf, 0); err != nil {
		return nil, errors.Wrap(err, "failed to read")
	}
	if _, err := ff.h.Decode(ff.buf); err != nil {
		return nil, errors.Wrap(err, "failed to decode")
	}
	ff.f = f
	ff.capacity = cap_
	ff.size = ff.h.Size
	return ff, nil
}

const (
	initialCap = 1024 // 1kb
)

func create(f *os.File) (*File, error) {
	ff := &File{
		f:        f,
		size:     0,
		capacity: 0,
	}
	if err := ff.alloc(initialCap); err != nil {
		return nil, err
	}
	return ff, nil
}

type header struct {
	Size int64
}

func (h header) Append(buf []byte) []byte {
	buf = binary.AppendMagic(buf, magic)
	return binary.AppendInt64(buf, h.Size)
}

func (h *header) Decode(buf []byte) ([]byte, error) {
	var err error
	if buf, err = binary.DecodeMagic(buf, magic); err != nil {
		return buf, err
	}
	return binary.DecodeInt64(buf, &h.Size), nil
}

var magic = [...]byte{
	0xfa,
	0xaf,
	0x10,
	0x94,
	0x10,
	0x28,
	0x06,
	0x16,
}

// Backend wraps interfaces that defines File backend.
type Backend interface {
	io.Closer
	io.WriterAt
	io.ReaderAt
	Truncate(int64) error
}

// File is auto-truncate Backend abstraction.
type File struct {
	sync.Mutex
	f        Backend
	capacity int64
	size     int64
	h        header
	buf      []byte // buffer for header write
}

type Options struct {
	Backend  Backend
	Capacity int64
	Size     int64
}

func NewFile(o Options) *File {
	f := &File{
		capacity: o.Capacity,
		f:        o.Backend,
		size:     o.Size,
	}
	f.buf = f.h.Append(f.buf)
	return f
}

// Close implements io.Closer.
func (f *File) Close() error {
	if f == nil {
		return errors.New("f is nil")
	}
	if f.f == nil {
		return errors.New("backend is nil")
	}
	return f.f.Close()
}

func (f *File) writeHeader() error {
	f.Lock()
	f.h.Size = atomic.LoadInt64(&f.size)
	f.buf = f.h.Append(f.buf[:0])
	_, err := f.f.WriteAt(f.buf, 0)
	f.Unlock()
	return err
}

const (
	headerSize = 8 + 8 // len(magic) + int64
)

// off returns offset from start of underlying file
// to data offset.
func (f *File) off(off int64) int64 {
	return headerSize + off
}

// Size returns data size in file.
func (f *File) Size() int64 {
	return atomic.LoadInt64(&f.size)
}

// ReadAt implements io.ReaderAt.
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	// checking that len(b) + off is < f.size
	if atomic.LoadInt64(&f.size)-(f.off(off)+int64(len(b))) < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return f.f.ReadAt(b, f.off(off))
}

const (
	s1KB   = 1024 * 1024
	s1MB   = s1KB * 1024
	s64MB  = s1MB * 64
	s128MB = s64MB * 2
	s256MB = s128MB * 2
	s512MB = s256MB * 2
	s1GB   = s1MB * 1024
	s5GB   = s1GB * 5
	s10GB  = s1GB * 10
)

func nearestCap(current, need int64) int64 {
	if (current - need) > (current / 4) {
		return current
	}
	if current >= s10GB {
		return nearestCap(current+s5GB, need)
	}
	if current >= s1GB {
		return nearestCap(current+s512MB, need)
	}
	if current >= s512MB {
		return nearestCap(current+s128MB, need)
	}
	if current >= s64MB {
		return nearestCap(current+s64MB, need)
	}
	next := (need / 2) * 2
	if next == current {
		next *= 2
	}
	return nearestCap(next, need)
}

// alloc truncates file to nearest newCap
func (f *File) alloc(size int64) error {
	oldCap := atomic.LoadInt64(&f.capacity)
	newCap := nearestCap(oldCap, size)
	if newCap == oldCap {
		return nil
	}
	if !atomic.CompareAndSwapInt64(&f.capacity, oldCap, newCap) {
		return f.alloc(size)
	}
	return f.f.Truncate(newCap)
}

// Append writes b and returns it offset, implementing Appender.
func (f *File) Append(b []byte) (int64, error) {
	// [f.size ... f.size+len(b)]
	// soft allocate of len(b) in the end of file
	size := atomic.AddInt64(&f.size, int64(len(b)))
	if err := f.alloc(size); err != nil {
		return 0, err
	}
	offset := size - int64(len(b))
	_, err := f.f.WriteAt(b, offset)
	if err != nil {
		atomic.AddInt64(&f.size, -int64(len(b)))
		return 0, err
	}
	return offset, f.writeHeader()
}

// WriteAt implements io.WriterAt.
func (f *File) WriteAt(b []byte, off int64) (int, error) {
	if err := f.alloc(int64(len(b)) + off); err != nil {
		return 0, err
	}
	return f.f.WriteAt(b, f.off(off))
}
