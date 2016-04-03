package storage

import (
	"errors"
	"os"
)

var (
	// ErrIDMismatch returned when read Header.ID is not equal to provided Link.ID and is possible data corruption.
	ErrIDMismatch = errors.New("BulkBackend Header.ID != Link.ID")
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

// ReadHeader returns Header and error, if any, reading File by Link from backend.
func (b Bulk) ReadHeader(l Link, buf []byte) (Header, error) {
	var h Header
	h.ID = l.ID
	h.Offset = l.Offset
	_, err := b.Backend.ReadAt(buf[:LinkStructureSize], l.Offset)
	if err != nil {
		return h, err
	}
	h.Read(buf[:LinkStructureSize])
	if h.ID != l.ID {
		return h, ErrIDMismatch
	}
	return h, err
}

// ReadData reads h.Size bytes into buffer from f.DataOffset.
func (b Bulk) ReadData(h Header, buf []byte) error {
	buf = buf[:h.Size]
	_, err := b.Backend.ReadAt(buf, h.DataOffset())
	return err
}

// Write returns error if any, writing Header and data to backend.
func (b Bulk) Write(h Header, data []byte) error {
	// saving first HeaderStructureSize bytes to temporary slice on stack
	tmp := make([]byte, HeaderStructureSize)
	copy(tmp, data[:HeaderStructureSize])
	// serializing header to data, preventing heap escape
	h.Put(data[:HeaderStructureSize])
	_, err := b.Backend.WriteAt(data[:HeaderStructureSize], h.Offset)
	// loading back first bytes
	copy(data[:HeaderStructureSize], tmp)
	if err != nil {
		return err
	}
	_, err = b.Backend.WriteAt(data, h.DataOffset())
	return err
}
