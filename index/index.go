// Package index implements index operations.
//
// Index is linear list of pairs (k, v), where len(v) = const,
// and ki = i, that defines Get and Set operations that has complexity
// of O(1).
//
// Index is represented by:
//
//   N    - max(i)
//   Vlen - len(v) = const
//   Blob - {v0, v1, ..., vi, ... vN}
//
// Vi is ReadAt(buf[:Vlen], Vlen*i) on Blob.
package index

import (
	"io"

	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

const StartID int64 = 0

// Walker is function that is used as callback while iterating over index.
type Walker func(k int64, v []byte) error

// Index is k-v database, where len(v) = const and k is {1, 2, 3, ..., n},
// which is mapped to file.
type Index interface {
	io.Closer
	Get(k int64, b []byte) error
	Set(k int64, v []byte) error
	Len() (int64, error)
}

// Ranger is Index that supports iteration.
type Ranger interface {
	All(w Walker) error
	Range(start, end int64, w Walker) error
}

type Backend interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type RWAtIndex struct {
	Backend Backend
	Size    int
	Length  int
}

func (i RWAtIndex) Len() (int64, error) {
	return int64(i.Length), nil
}

func (i RWAtIndex) Close() error {
	return i.Backend.Close()
}

func (i RWAtIndex) offset(k int64) int64 {
	return int64(i.Size) * k
}

func (i RWAtIndex) Get(k int64, b []byte) error {
	var (
		err error
	)
	_, err = i.Backend.ReadAt(b, i.offset(k))
	return errors.Wrap(err, "failed to read")
}

func (i RWAtIndex) Set(k int64, b []byte) error {
	var (
		err error
	)
	_, err = i.Backend.WriteAt(b, i.offset(k))
	return errors.Wrap(err, "failed to write")
}

type Iterator struct {
	Index Index
	Size  int
}

func extend(b *bytebufferpool.ByteBuffer, n int) *bytebufferpool.ByteBuffer {
	b.Reset()
	if cap(b.B) >= n {
		b.B = b.B[:n]
		return b
	}
	buf := make([]byte, n)
	b.B = append(b.B, buf...)
	return b
}

func (i Iterator) All(w Walker) error {
	n, err := i.Index.Len()
	if err != nil {
		return errors.Wrap(err, "failed get Len")
	}
	b := extend(pool.Get(), i.Size)
	defer pool.Put(b)
	for id := StartID; id < n; id++ {
		if err = i.Index.Get(id, b.B); err != nil {
			return errors.Wrap(err, "failed to Get")
		}
		if err := w(id, b.B); err != nil {
			return errors.Wrap(err, "callback error")
		}
		b.Reset()
	}
	return nil
}

var (
	pool = bytebufferpool.Pool{}
)
