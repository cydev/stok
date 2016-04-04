// Package storage defines a way to save, load, list and delete files for local storage system
// and is optimized for small files, that are often read, rarely written and very rarely deleted.
// It is O(1) for read/write/delete. Package is low-level and subject to the strict limitations.
//
// On file delete it is only marked as deleted causing fragmentation
// file system space will only become available for usage only after
// scheduled optimization commonly named "vacuum". During vacuum
// files are reorganized in way that minimize space consumption.
// Efficiency of vacuum fully depends on underlying algorithm and may vary.
//
// Files are stored in bulks, links in indexes.
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

// ReadBuf reads data with provided id to buffer, returning error if any.
func (v Volume) ReadBuf(id int64, b []byte) error {
	l, err := v.Index.ReadBuff(id, b)
	if err != nil {
		return err
	}
	h, err := v.Bulk.ReadHeader(l, b)
	if err != nil {
		return err
	}
	return v.Bulk.ReadData(h, b)
}
