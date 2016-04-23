// Copyright (c) 2016 Aliaksandr Valialkin
// fasthttp/bytebuffer.go

package storage

import (
	"sync"
)

const (
	defaultByteBufferSize = 1024 * 300 // changed to 300 kb because it is mostly used to read files
)

// ByteBuffer provides byte buffer, which can be used with storage API
// in order to minimize memory allocations.
//
// ByteBuffer may be used with functions appending data to the given []byte
// slice. See example code for details.
//
// Use AcquireByteBuffer for obtaining an empty byte buffer.
type ByteBuffer struct {

	// B is a byte buffer to use in append-like workloads.
	// See example code for details.
	B []byte
}

// Write implements io.Writer - it appends p to ByteBuffer.B
func (b *ByteBuffer) Write(p []byte) (int, error) {
	return b.Append(p), nil
}

// Append appends p to ByteBuffer.B and returns length of p
func (b *ByteBuffer) Append(p []byte) int {
	b.B = append(b.B, p...)
	return len(p)
}

// Reset makes ByteBuffer.B empty.
func (b *ByteBuffer) Reset() {
	b.B = b.B[:0]
}

// AcquireByteBuffer returns an empty byte buffer from the pool.
//
// Acquired byte buffer may be returned to the pool via ReleaseByteBuffer call.
// This reduces the number of memory allocations required for byte buffer
// management.
func AcquireByteBuffer() *ByteBuffer {
	v := byteBufferPool.Get()
	if v == nil {
		return &ByteBuffer{
			B: make([]byte, 0, defaultByteBufferSize),
		}
	}
	return v.(*ByteBuffer)
}

// ReleaseByteBuffer returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races occur.
func ReleaseByteBuffer(b *ByteBuffer) {
	b.B = b.B[:0]
	byteBufferPool.Put(b)
}

// AcquireIndexBuffer returns an empty byte buffer from the pool.
//
// Acquired byte buffer may be returned to the pool via ReleaseByteBuffer call.
// This reduces the number of memory allocations required for byte buffer
// management.
func AcquireIndexBuffer() *ByteBuffer {
	v := indexBufferPool.Get()
	if v == nil {
		return &ByteBuffer{
			B: make([]byte, 0, 128),
		}
	}
	return v.(*ByteBuffer)
}

// ReleaseIndexBuffer returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races occur.
func ReleaseIndexBuffer(b *ByteBuffer) {
	b.B = b.B[:0]
	indexBufferPool.Put(b)
}

var (
	byteBufferPool  sync.Pool
	indexBufferPool sync.Pool
)
