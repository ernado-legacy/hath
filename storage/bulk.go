package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
)

var (
	// ErrIDMismatch returned when read File.ID is not equal to provided Link.ID and is possible data corruption.
	ErrIDMismatch = errors.New("BulkBackend File.ID != Link.ID")
)

// An BulkBackend describes a backend that is used for file store.
type BulkBackend interface {
	ReadAt(b []byte, off int64) (int, error)
	WriteAt(b []byte, off int64) (int, error)
	Stat() (os.FileInfo, error)
}

type readerAtWrapper struct {
	r   io.ReaderAt
	off int64
}

func (r readerAtWrapper) Read(b []byte) (int, error) {
	return r.r.ReadAt(b, r.off)
}

// WrapReaderAt returns io.Reader which performs ReadAt(b, off) on provided io.ReaderAt
func WrapReaderAt(r io.ReaderAt, off int64) io.Reader {
	return readerAtWrapper{r: r, off: off}
}

// Bulk is collection of data slices, prepended with File header. Implements basic operations on files.
type Bulk struct {
	Backend BulkBackend
}

// Read File from BulkBackend by Link, using provided buffer.
// Returns File and error, if any. Stores data in buffer.
func (b Bulk) Read(l Link, buf *bytes.Buffer) (File, error) {
	buf.Reset()
	var (
		f       File
		fBuffer FileStructureBuffer
	)
	f.ID = l.ID
	f.Offset = l.Offset
	_, err := b.Backend.ReadAt(fBuffer[:], l.Offset)
	if err != nil {
		return f, err
	}
	f.Read(fBuffer[:])
	if f.ID != l.ID {
		return f, ErrIDMismatch
	}
	buf.Grow(int(f.Size))
	_, err = buf.ReadFrom(WrapReaderAt(b.Backend, l.Offset+FileStructureSize))
	return f, err
}
