package storage

import (
	"io"
)

// BlobBackend is the interface that wraps ReadAt, WriteAt methods.
type BlobBackend interface {
	io.ReaderAt
	io.WriterAt
}

// Blob represents set of data slices.
type Blob struct {
	Backend BlobBackend
}
