package storage

import (
	"os"
)

// Backend for IO operations used in Index and Bulk.
type Backend interface {
	ReadAt(b []byte, off int64) (int, error)
	WriteAt(b []byte, off int64) (int, error)
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}
