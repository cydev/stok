// Copyright (c) 2016 Aliaksandr Valialkin
// fasthttp/bytebuffer.go

package storage

import (
	"github.com/valyala/bytebufferpool"
)

// AcquireByteBuffer returns an empty byte buffer from the pool.
//
// Acquired byte buffer may be returned to the pool via ReleaseByteBuffer call.
// This reduces the number of memory allocations required for byte buffer
// management.
func AcquireByteBuffer() *bytebufferpool.ByteBuffer {
	return byteBufferPool.Get()
}

// ReleaseByteBuffer returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races occur.
func ReleaseByteBuffer(b *bytebufferpool.ByteBuffer) {
	b.B = b.B[:0]
	byteBufferPool.Put(b)
}

// AcquireIndexBuffer returns an empty byte buffer from the pool.
//
// Acquired byte buffer may be returned to the pool via ReleaseByteBuffer call.
// This reduces the number of memory allocations required for byte buffer
// management.
func AcquireIndexBuffer() *bytebufferpool.ByteBuffer {
	return indexBufferPool.Get()
}

// ReleaseIndexBuffer returns byte buffer to the pool.
//
// ByteBuffer.B mustn't be touched after returning it to the pool.
// Otherwise data races occur.
func ReleaseIndexBuffer(b *bytebufferpool.ByteBuffer) {
	b.B = b.B[:0]
	indexBufferPool.Put(b)
}

var (
	byteBufferPool  bytebufferpool.Pool
	indexBufferPool bytebufferpool.Pool
)
