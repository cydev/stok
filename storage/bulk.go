package storage

import (
	"errors"
)

var (
	// ErrIDMismatch means that Header.ID is not equal to provided Link.ID and
	// is returned by Bulk.ReadHeader.
	//
	// Possible reasons are data corruption or usage of wrong index for bulk.
	ErrIDMismatch = errors.New("BulkBackend Header.ID != Link.ID")
)

// An BulkBackend describes a backend that is used for file store.
type BulkBackend Backend

// Bulk is collection of data slices, prepended with File header. Implements basic operations on files.
type Bulk struct {
	Backend BulkBackend
}

// ReadHeader returns Header and error, if any, reading File by Link from backend.
func (b Bulk) ReadHeader(l Link, buf []byte) (Header, error) {
	// check that provided buffer is enough to store Link
	// to be strict, we can panic (or return error) there.
	if cap(buf) < LinkStructureSize {
		buff := AcquireByteBuffer()
		buff.Append(buf)
		buf = buff.B
		defer ReleaseByteBuffer(buff)
	}
	var h Header
	h.ID = l.ID
	h.Offset = l.Offset
	_, err := b.Backend.ReadAt(buf[:LinkStructureSize], l.Offset)
	if err != nil {
		return h, err
	}
	h.Read(buf[:LinkStructureSize])
	if h.ID != l.ID {
		return h, ErrIDMismatch
	}
	return h, err
}

// ReadData reads h.Size bytes into buffer from f.DataOffset.
func (b Bulk) ReadData(h Header, buf *ByteBuffer) error {
	if cap(buf.B) < h.Size {
		// not enough capacity to use buffer, so allocate more
		buf.B = make([]byte, h.Size)
	}
	buf.B = buf.B[:h.Size]
	_, err := b.Backend.ReadAt(buf.B, h.DataOffset())
	return err
}

// Write returns error if any, writing Header and data to backend.
func (b Bulk) Write(h Header, data []byte) error {
	if cap(data) < HeaderStructureSize {
		// file is smaller than header (corner case)
		buff := AcquireByteBuffer()
		buff.Append(data)
		data = buff.B
		defer ReleaseByteBuffer(buff)
	}
	// saving first HeaderStructureSize bytes to temporary slice on stack
	tmp := make([]byte, HeaderStructureSize)
	copy(tmp, data[:HeaderStructureSize])
	// serializing header to data, preventing heap escape
	h.Put(data[:HeaderStructureSize])
	_, err := b.Backend.WriteAt(data[:HeaderStructureSize], h.Offset)
	// loading back first bytes
	copy(data[:HeaderStructureSize], tmp)
	if err != nil {
		return err
	}
	_, err = b.Backend.WriteAt(data[:h.Size], h.DataOffset())
	return err
}

// Preallocate truncates changes the size of the bulk.
// It is shorthand to Backend.Truncate.
func (b Bulk) Preallocate(size int64) error {
	return b.Backend.Truncate(size)
}
