// Package storage defines a way to save, load, list and delete files for local storage system
// and is optimized for small files, that are often read, rarely written and very rarely deleted.
// It is O(1) for read/write/delete. Package is low-level and subject to the strict limitations.
//
// On file delete it is only marked as deleted causing fragmentation.
// File system space will only become available for usage only after
// scheduled optimization commonly named "vacuum". During "vacuum"
// files are reorganized in way that minimize space consumption.
// Efficiency of vacuum fully depends on underlying algorithm and may vary.
//
// Files are stored in bulks, links in indexes. Bulk and index togather is Volume.
package storage

// Volume consists of Bulk and Index on it, and implements abstraction layer.
//
// Bulk <-> Index
// Bulk is list of files stored in backend with headers, supplied by Index, which links
// ID to Offset and makes possible O(1) read operations.
// Index can be recovered from Bulk, because Bulk contains redundant information (Offset, ID).
type Volume struct {
	Index Index
	Bulk  Bulk
}

// ReadCallback is called on successful read from Volume. Buffer is valid until return.
//
// Do not use b []byte, it is reused in buffer pool.
type ReadCallback func(h Header, b []byte) error

// ReadFile reads file by id and calls ReadCallback if succeed, returns error if any.
// If callback returns error, ReadFile returns it unchanged.
func (v Volume) ReadFile(id int64, f ReadCallback) error {
	b := AcquireByteBuffer()
	defer ReleaseByteBuffer(b)
	l, err := v.Index.ReadBuff(id, b.B)
	if err != nil {
		return err
	}
	if l.ID != id {
		return IDMismatchError(l.ID, id, AtIndex)
	}
	h, err := v.Bulk.ReadHeader(l, b.B)
	if err != nil {
		return err
	}
	if err := v.Bulk.ReadData(h, b); err != nil {
		return err
	}
	return f(h, b.B)
}
