package storage

import (
	"encoding/binary"
)

// Header represents a stored data, obtainable with ReadAt(data, Header.Offset+HeaderStructureSize),
// where len(data) >= Length.
//
// ReadAt(b, Header.Offset) with len(b) = HeaderStructureSize will read serialized info, and Header.Read(b)
// will read it into structure fields.
//
// Bucket element structure:
//    |-----------------------------------------| -1
//    |------------ Header.Offset --------------| 0
//    |       HeaderStructureSize bytes         |
//    |              of Header                  |
//    |-- Header.Offset + HeaderStructureSize --| 16 // or Header.DataOffset()
//    |                                         |
//    |         Header.Length bytes             |
//    |                                         |
//    |-----------------------------------------| length + 16
type Header struct {
	ID        int64 // -> Link.ID
	Offset    int64 // -> Link.Offset
	Length    int   // len(data)
	Timestamp int64 // Time.Unix()
}

// Length64 returns int64 version of Length.
func (h Header) Length64() int64 {
	return int64(h.Length)
}

// Link returns link to Header via ID and Offset copy.
func (h Header) Link() Link {
	return Link{
		ID:     h.ID,
		Offset: h.Offset,
	}
}

// DataOffset returns offset for data, associated with Header
func (h Header) DataOffset() int64 {
	return h.Offset + HeaderStructureSize
}

// HeaderStructureSize is minimum buf length required in Header.{Read,Put} and is 256 bit or 32 byte.
const HeaderStructureSize = 8 * 4

// HeaderStructureBuffer is byte array of File structure size
type HeaderStructureBuffer [HeaderStructureSize]byte

// NewHeaderBuffer is shorthand for new []byte slice with length HeaderStructureSize
// that is safe to pass as buffer to all Link-related Read/Write methods.
func NewHeaderBuffer() []byte {
	return make([]byte, HeaderStructureSize)
}

// Read header from byte slice using binary.PutVariant for all fields, returns read size in bytes.
func (h *Header) Read(b []byte) int {
	var offset, read int
	h.ID, read = binary.Varint(b[offset:])
	offset += read
	var tSize int64
	tSize, read = binary.Varint(b[offset:])
	h.Length = int(tSize)
	offset += read
	h.Offset, read = binary.Varint(b[offset:])
	offset += read
	h.Timestamp, read = binary.Varint(b[offset:])
	return offset + read
}

// Put header to byte slice using binary.PutVariant for all fields, returns write size in bytes.
func (h Header) Put(b []byte) int {
	var offset int
	offset += binary.PutVarint(b[offset:], h.ID)
	offset += binary.PutVarint(b[offset:], h.Length64())
	offset += binary.PutVarint(b[offset:], h.Offset)
	offset += binary.PutVarint(b[offset:], h.Timestamp)
	return offset
}
