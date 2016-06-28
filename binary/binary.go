package binary

import (
	"github.com/cydev/stok"
	bb "github.com/valyala/bytebufferpool"

	b "encoding/binary"
)

type Appender interface {
	Append(b []byte) []byte
}

type Decoder interface {
	Decode(b []byte) ([]byte, error)
}

func ToBuffer(a Appender, buf *bb.ByteBuffer) {
	buf.B = a.Append(buf.B)
}

var (
	e = b.BigEndian
)

const (
	ErrBadMagic stok.Error = "Bad magic header"
)

func AppendInt64(buf []byte, i int64) []byte {
	inn := make([]byte, 8)
	e.PutUint64(inn, uint64(i))
	return append(buf, inn...)
}

func AppendMagic(buf []byte, magic [8]byte) []byte {
	return append(buf, magic[:]...)
}

func CheckMagic(buf []byte, magic [8]byte) error {
	if len(buf) < len(magic) {
		return ErrBadMagic
	}
	for i, c := range magic {
		if buf[i] != c {
			return ErrBadMagic
		}
	}
	return nil
}

func DecodeInt64(buf []byte, d *int64) []byte {
	*d = int64(e.Uint64(buf[:8]))
	return buf[8:]
}

func DecodeMagic(buf []byte, magic [8]byte) ([]byte, error) {
	return buf[8:], CheckMagic(buf, magic)
}
