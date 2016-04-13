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
// Files are stored in buckets, links in indexes. Bucket and index togather is Volume.
package storage

import "time"

// Volume consists of Bucket and Index on it, and implements abstraction layer.
//
// Bucket <-> Index
// Bucket is list of files stored in backend with headers, supplied by Index, which links
// ID to Offset and makes possible O(1) read operations.
// Index can be recovered from Bucket, because Bucket contains redundant information (Offset, ID).
type Volume struct {
	Index  Index
	Bucket Bucket
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
	h, err := v.Bucket.ReadHeader(l, b.B)
	if err != nil {
		return err
	}
	if err := v.Bucket.ReadData(h, b); err != nil {
		return err
	}
	return f(h, b.B)
}

// WriteFile writes byte slice by link and returns Header for file and error if any.
func (v Volume) WriteFile(l Link, b []byte) (Header, error) {
	h := Header{
		ID:        l.ID,
		Offset:    l.Offset,
		Length:    len(b),
		Timestamp: time.Now().Unix(),
	}
	buf := AcquireIndexBuffer()
	defer ReleaseIndexBuffer(buf)
	if err := v.Index.WriteBuff(l, buf.B); err != nil {
		return h, err
	}
	return h, v.Bucket.Write(h, b)
}
