package storage

type sError string

func (s sError) Error() string {
	return string(s)
}

const (
	// ErrIsDirectory means that directory is found where file assumed.
	ErrIsDirectory sError = "Got directory, need file"
	// ErrBadHeader means that blob is unable to initialize due header corruption or wrong file format.
	ErrBadHeader sError = "Bad header magic bytes, file corrupted or in wrong format"
	// ErrBadHeaderCapacity means that decoded capacity from header is less than actual file size.
	ErrBadHeaderCapacity sError = "Capacity in header is less than actual file size, file can be corrupted"
	// ErrBadHeaderCRC means that header crc check failed.
	ErrBadHeaderCRC sError = "Header CRC missmatch"
)
