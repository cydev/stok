package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	// ErrIsDirectory means that directory is found where file assumed.
	ErrIsDirectory = errors.New("Got directory, need file")
	// ErrBadHeader means that blob is unable to initialize due header corruption or wrong file format.
	ErrBadHeader = errors.New("Bad header magic bytes, file corrupted or in wrong format")
	// ErrBadHeaderCapacity means that decoded capacity from header is less than actual file size.
	ErrBadHeaderCapacity = errors.New("Capacity in header is less than actual file size, file can be corrupted")
	// ErrBadHeaderCRC means that header crc check failed.
	ErrBadHeaderCRC = errors.New("Header CRC missmatch")
)

// Allocator wraps Allocate method for allocating slices. Should be goroutine-safe.
type Allocator interface {
	Allocate(size int64) (offset int64, err error)
}

// StatBackend wraps Stat method for retrieving file stats.
type StatBackend interface {
	Stat() (os.FileInfo, error)
}

// TruncateSyncer is the interface that groups Sync and Truncate methods.
type TruncateSyncer interface {
	// Sync commits the current contents of the file to stable storage.
	Sync() error
	// Truncate changes the size of the storage.
	Truncate(size int64) error
}

// BlobBackend is the interface that groups basic methods for consistent storage.
type BlobBackend interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	TruncateSyncer
}

// Blob represents set of data slices on top of BlobBackend.
type Blob struct {
	sync.RWMutex
	Backend    BlobBackend
	Size       int64
	Capacity   int64
	headerBuff [blobHeaderSize]byte
}

// Sync commits the current state of blob.
func (b *Blob) Sync() (err error) {
	b.Lock()
	if err = b.writeHeader(); err == nil {
		err = b.Backend.Sync()
	}
	b.Unlock()
	return err
}

// Truncate changes the capacity of the blob.
func (b *Blob) Truncate(size int64) (err error) {
	b.Lock()
	if err = b.Backend.Truncate(size); err == nil {
		b.Capacity = size
		// rendering capacity changes to header
		err = b.writeHeader()
	}
	b.Unlock()
	return err
}

// Allocate returns offset to atomically allocated slice of provided size and error if any.
// After allocation it is safe to call WriteAt(b, offset) with len(b) = size.
//
// Example:
//     data := make([]byte, size)
//     offset, _ := b.Allocate(size)
//     b.WriteAt(data, offset)
func (b *Blob) Allocate(size int64) (int64, error) {
	newSize := atomic.AddInt64(&b.Size, size)
	return newSize - size, nil
}

// readHeader encodes header to start of backend and returns error if any.
// It is not goroutine-safe. Can return errors from backend.
// Uses b.headerBuff as write buffer.
func (b *Blob) writeHeader() error {
	header := BlobHeader{
		Size:     b.Size,
		Capacity: b.Capacity,
	}
	header.Put(b.headerBuff[:])
	_, err := b.Backend.WriteAt(b.headerBuff[:], 0)
	if b.Size == 0 {
		b.Size = blobHeaderSize
	}
	return err
}

// readHeader decodes header from start of backend and returns error if any.
// It is not goroutine-safe.
// Can return ErrBadHeaderCapacity, ErrBadHeader and errors from backend.
// Uses b.headerBuff as read buffer.
func (b *Blob) readHeader() error {
	if _, err := b.Backend.ReadAt(b.headerBuff[:], 0); err != nil {
		return err
	}
	h := BlobHeader{}
	if err := h.Read(b.headerBuff[:]); err != nil {
		return err
	}
	if b.Capacity < h.Capacity {
		return ErrBadHeaderCapacity
	}
	b.Size = h.Size
	return nil
}

const (
	// DefaultBlobSize is initial capacity for newly created blob.
	DefaultBlobSize = 1024
)

// BlobConfig is configuration for blob processing.
type BlobConfig struct {
	InitialSize int64
}

// GetInitialSize returns initial size for the blob used upon creation.
func (i *BlobConfig) GetInitialSize() int64 {
	if i == nil || i.InitialSize == 0 {
		return DefaultBlobSize
	}
	return i.InitialSize
}

// OpenBlob opens or creates a Blob for the given path.
//
// The returned Blob instance is goroutine-safe.
// The Blob must be closed after use, by calling Close method.
func OpenBlob(path string, cfg *BlobConfig) (*Blob, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		return nil, ErrIsDirectory
	}
	b := &Blob{
		Capacity: stat.Size(),
		Size:     0,
		Backend:  f,
	}
	if b.Capacity == 0 {
		err = b.Truncate(cfg.GetInitialSize())
	} else {
		err = b.readHeader()
	}
	runtime.SetFinalizer(b, (*Blob).Close)
	return b, err
}

// Close closes the Blob, rendering it unusable for changes.
// It returns an error, if any.
func (b *Blob) Close() error {
	if err := b.Sync(); err != nil {
		return err
	}
	return b.Backend.Close()
}

// BlobHeader contains info about Blob size and capacity.
type BlobHeader struct {
	Size     int64
	Capacity int64
}

// blobHeaderSize = magic + size + capacity + crc
const blobHeaderSize = 8 + 8 + 8 + 8

// blobHeaderMagic are magic bytes at start of blob header.
var blobHeaderMagic = [...]byte{
	0xbb,
	0xba,
	0xbd,
	0xbb,
	0x13,
	0x37,
	0x20,
	0x16,
}

// Put encodes BlobHeader into buf and returns the number of bytes written.
// If the buffer is too small, Put will panic.
func (h BlobHeader) Put(buf []byte) int {
	var offset = 8
	copy(buf[:offset], blobHeaderMagic[:])
	binary.PutVarint(buf[offset:], h.Size)
	offset += 8
	binary.PutVarint(buf[offset:offset+8], h.Capacity)
	offset += 8
	crc := crc32.ChecksumIEEE(buf[:offset])
	binary.PutUvarint(buf[offset:offset+8], uint64(crc))
	offset += 8
	return offset
}

// Read decodes BlobHeader from buf and returns ErrBadHeader if it fails.
func (h *BlobHeader) Read(buf []byte) error {
	// checking header magic
	for i, v := range blobHeaderMagic {
		if v != buf[i] {
			return ErrBadHeader
		}
	}
	offset := 8
	h.Size, _ = binary.Varint(buf[offset:])
	offset += 8
	h.Capacity, _ = binary.Varint(buf[offset:])
	offset += 8
	// calculating crc
	expectedCRC := crc32.ChecksumIEEE(buf[:offset])
	// checking crc
	if crc, _ := binary.Uvarint(buf[offset:]); crc != uint64(expectedCRC) {
		return ErrBadHeaderCRC
	}
	return nil
}
