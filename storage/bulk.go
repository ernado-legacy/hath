package storage

import (
	"errors"
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

// Bulk is collection of data slices, prepended with File header. Implements basic operations on files.
type Bulk struct {
	Backend BulkBackend
}

// ReadFile returns File and error, if any, reading File by Link from backend.
func (b Bulk) ReadFile(l Link, buf []byte) (Header, error) {
	var (
		f Header
	)
	f.ID = l.ID
	f.Offset = l.Offset
	_, err := b.Backend.ReadAt(buf[:LinkStructureSize], l.Offset)
	if err != nil {
		return f, err
	}
	f.Read(buf[:LinkStructureSize])
	if f.ID != l.ID {
		return f, ErrIDMismatch
	}
	return f, err
}

// ReadData reads f.Size bytes into buffer from f.DataOffset.
func (b Bulk) ReadData(f Header, buf []byte) error {
	buf = buf[:f.Size]
	_, err := b.Backend.ReadAt(buf, f.DataOffset())
	return err
}

func (b Bulk) Write(f Header, data []byte) error {
	f.Put(data[:HeaderStructureSize])
	if _, err := b.Backend.WriteAt(data[:HeaderStructureSize], f.Offset); err != nil {
		return err
	}
	_, err := b.Backend.WriteAt(data, f.DataOffset())
	return err
}
