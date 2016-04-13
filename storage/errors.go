package storage

import "fmt"

// VolumeError means that error is internal for volume.
type VolumeError interface {
	VolumeError() string
}

// ErrorKind indicates the place where error occurred.
type ErrorKind int

// Error may occur at Index, Bucket level, or on higher Volume level.
const (
	AtVolume ErrorKind = iota
	AtIndex
	AtBucket
)

func (e ErrorKind) String() string {
	switch e {
	case AtVolume:
		return "Volume"
	case AtIndex:
		return "Index"
	case AtBucket:
		return "Bucket"
	default:
		panic(fmt.Sprint("Unknown error kind:", int(e)))
	}
}

// ErrIDMismatch means that Header.ID is not equal to provided Link.ID and
// is returned by Bucket.ReadHeader.
//
// Possible reasons are data corruption or usage of wrong index for bucket.
type ErrIDMismatch struct {
	Got      int64
	Expected int64
	Kind     ErrorKind
}

func (e ErrIDMismatch) Error() string {
	return fmt.Sprintf("IDMissmatch at %s: Got %d, expected %d", e.Kind, e.Got, e.Expected)
}

// VolumeError implements VolumeError.
func (e ErrIDMismatch) VolumeError() string {
	return e.Error()
}

// IDMismatchError wraps ID mismatch in ErrIDMismatch and returns it.
func IDMismatchError(got, expected int64, k ErrorKind) ErrIDMismatch {
	return ErrIDMismatch{
		Got:      got,
		Expected: expected,
		Kind:     k,
	}
}

// ErrBackendFailed means that underlying backend returned error.
type ErrBackendFailed struct {
	Err  error
	Kind ErrorKind
}

func (e ErrBackendFailed) Error() string {
	if e.Err == nil {
		return "nil"
	}
	return fmt.Sprintf("Backend at %s: %s", e.Kind, e.Err)
}

// VolumeError implements VolumeError.
func (e ErrBackendFailed) VolumeError() string {
	return e.Error()
}

// BackendError wraps error with ErrorKind and returns new ErrBackendFailed.
func BackendError(err error, k ErrorKind) ErrBackendFailed {
	return ErrBackendFailed{
		Err:  err,
		Kind: k,
	}
}
