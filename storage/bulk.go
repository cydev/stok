package storage

import (
	"sync/atomic"
	"sync"
)

// An BulkBackend describes a backend that is used for file store.
type BulkBackend Backend

// Bulk is collection of data slices, prepended with File header. Implements basic operations on files.
type Bulk struct {
	Backend  BulkBackend
	Size     int64
	Capacity int64
	sync.Mutex
}

const (
	// bulkMinFreeRate is minimum free ratio for bulk.
	bulkMinFreeRate = 0.2
	// bulkPreallocateRate is rate of capacity growth.
	bulkPreallocateRate = 2
)

// nearCapacity returns true if bulk will be close to capacity.
func (b Bulk) nearCapacity(size int64) bool {
	if b.Capacity == 0 {
		return true
	}
	return (1 - float64(size)/float64(b.Capacity)) < bulkMinFreeRate
}

// Allocate new returns offset.
func (b *Bulk) Allocate(size int64) (int64, error) {
	newSize := atomic.AddInt64(&b.Size, size)
	off := newSize - size
	if !b.nearCapacity(newSize) {
		return off, nil
	}
	if err := b.Preallocate(newSize * bulkPreallocateRate); err != nil {
		atomic.AddInt64(&b.Size, -size)
		return 0, err
	}
	return off, nil
}


// ReadHeader returns Header and error, if any, reading File by Link from backend.
func (b Bulk) ReadHeader(l Link, buf []byte) (Header, error) {
	// check that provided buffer is enough to store Bulk
	// to be strict, we can panic (or return error) there.
	if cap(buf) < HeaderStructureSize {
		buff := AcquireByteBuffer()
		buff.Append(buf)
		buf = buff.B
		defer ReleaseByteBuffer(buff)
	}
	var h Header
	h.ID = l.ID
	h.Offset = l.Offset
	_, err := b.Backend.ReadAt(buf[:HeaderStructureSize], l.Offset)
	if err != nil {
		return h, BackendError(err, AtBulk)
	}
	h.Read(buf[:HeaderStructureSize])
	if h.ID != l.ID {
		return h, IDMismatchError(h.ID, l.ID, AtBulk)
	}
	return h, err
}

// ReadData reads h.Length bytes into buffer from f.DataOffset.
func (b Bulk) ReadData(h Header, buf *ByteBuffer) error {
	if cap(buf.B) < h.Length {
		// not enough capacity to use buffer, so allocate more
		buf.B = make([]byte, h.Length)
	}
	buf.B = buf.B[:h.Length]
	_, err := b.Backend.ReadAt(buf.B, h.DataOffset())
	if err != nil {
		return BackendError(err, AtBulk)
	}
	return nil
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
		return BackendError(err, AtBulk)
	}
	_, err = b.Backend.WriteAt(data[:h.Length], h.DataOffset())
	if err != nil {
		return BackendError(err, AtBulk)
	}
	return nil
}

// Preallocate changes the size of the bulk to provided value and returns error if any.
// It is shorthand to Backend.Truncate.
func (b *Bulk) Preallocate(size int64) error {
	b.Lock()
	defer b.Unlock()
	if err := b.Backend.Truncate(size); err != nil {
		return BackendError(err, AtBulk)
	}
	b.Capacity = size
	return nil
}
