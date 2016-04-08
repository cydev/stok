package storage

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// Link is index entry that links file id to offset, ID is key, Offset is value.
//
// Collection L = {L1, L2, ..., Ln} defines f(ID) -> Offset on id in L, so
// L is an associative array (ID, Offset).
type Link struct {
	ID     int64 // -> Header.ID
	Offset int64 // -> Header.Offset
}

// LinkStructureSize is minimum buf length required in Link.{Read,Put} and is 128 bit or 16 byte.
const LinkStructureSize = 8 * 2

// An IndexBackend describes a backend that is used for index store.
type IndexBackend Backend

// Index uses IndexBackend to store and retrieve Links
type Index struct {
	Backend     IndexBackend
	LastID      int64
	Capacity    int64
	BackendLock sync.Mutex
}

const (
	// indexMinFreeCapacity is minimum of (Index.Capacity - Index.LastID).
	indexMinFreeCapacity = 10
	// indexPreallocateRate is rate of capacity growth.
	indexPreallocateRate = 2
)

// nearCapacity returns true if newID is close to index capacity.
func (i *Index) nearCapacity(newID int64) bool {
	return newID+ indexMinFreeCapacity > i.Capacity
}

// NextID claims new id from index, allocating more
// space for index if needed and returns error if any.
//
// It is safe to call NextID concurrently, because it uses atomic operations
// and preallocate method is guarded by mutex.
//
// Warning: not calling file sync.
func (i *Index) NextID() (int64, error) {
	newID := atomic.AddInt64(&i.LastID, 1)
	if !i.nearCapacity(newID) {
		return newID, nil
	}
	if err := i.Preallocate(newID * indexPreallocateRate); err != nil {
		atomic.AddInt64(&i.LastID, -1)
		return 0, err
	}
	return newID, nil
}

// Preallocate truncates changes the size of the index file so index
// can fit provided count if links.
func (i *Index) Preallocate(count int64) error {
	i.BackendLock.Lock()
	defer i.BackendLock.Unlock()
	if err := i.Backend.Truncate(count * LinkStructureSize); err != nil {
		return BackendError(err, AtIndex)
	}
	i.Capacity = count
	return nil
}

// ReadBuff returns Link with provided id using provided buffer during serialization
func (i Index) ReadBuff(id int64, b []byte) (Link, error) {
	l := Link{}
	n, err := i.Backend.ReadAt(b, getLinkOffset(id))
	if err != nil {
		return l, BackendError(err, AtIndex)
	}
	l.Read(b[:n])
	return l, nil
}

// WriteBuff writes Link using provided buffer during deserialization
func (i Index) WriteBuff(l Link, b []byte) error {
	l.Put(b[:LinkStructureSize])
	_, err := i.Backend.WriteAt(b[:LinkStructureSize], getLinkOffset(l.ID))
	if err != nil {
		return BackendError(err, AtIndex)
	}
	return nil
}

// getLinkOffset returns offset in index for link with provided file id.
// Link.ID starts from 0, so getLinkOffset(0) == 0, getLinkOffset(1) == LinkStructureSize.
func getLinkOffset(id int64) int64 {
	return id * LinkStructureSize
}

// Put link to byte slice using binary.PutVariant for all fields, returns write size in bytes.
func (l Link) Put(b []byte) int {
	var offset int
	offset += binary.PutVarint(b[offset:], l.ID)
	offset += binary.PutVarint(b[offset:], l.Offset)
	return offset
}

// Read file from byte slice using binary.PutVariant for all fields, returns read size in bytes.
func (l *Link) Read(b []byte) int {
	var offset, read int
	l.ID, read = binary.Varint(b[offset:])
	offset += read
	l.Offset, read = binary.Varint(b[offset:])
	return offset + read
}
