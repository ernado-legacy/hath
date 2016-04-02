// Package storage defines a way to save, load, list and delete files for local storage system
// and is optimized for small files, that are often read, rarely written and very rarely deleted.
// It is O(1) for read/write/delete. Package is low-level and subject to the strict limitations.
//
// On file delete it is only marked as deleted causing fragmentation
// file system space will only become available for usage only after
// scheduled optimization commonly named "vacuum". During vacuum
// files are reorganized in way that minimize space consumption.
// Efficiency of vacuum fully depends on underlying algorithm and may vary.
//
// Files are stored in bulks, links in indexes.
package storage

import (
	"encoding/binary"
)

// File represents a stored data, obtainable with ReadAt(data, File.Offset+FileStructureSize),
// where len(data) >= Size.
//
// ReadAt(b, File.Offset) with len(b) = FileStructureSize will read serialized info, and File.Read(b)
// will read it into structure fields.
//
// Bulk element structure:
//    |---------------------------------| -1
//    |---------- File.Offset ----------| 0
//    |     FileStructureSize bytes     |
//    |           of File               |
//    |-File.Offset + FileStructureSize-| 16
//    |                                 |
//    |         File.Size bytes         |
//    |                                 |
//    |---------------------------------| size + 16
type File struct {
	ID        int64 // -> Link.ID
	Offset    int64 // -> Link.Offset
	Size      int64 // len(data)
	Timestamp int64 // Time.Unix()
}

// FileStructureSize is minimum buf length required in File.{Read,Put} and is 256 bit or 32 byte.
const FileStructureSize = 8 * 4

// FileStructureBuffer is byte array of File structure size
type FileStructureBuffer [FileStructureSize]byte

// NewFileBuffer is shorthand for new []byte slice with length NewFileBuffer
// that is safe to pass as buffer to all Link-related Read/Write methods.
func NewFileBuffer() []byte {
	return make([]byte, FileStructureSize)
}

// Read file from byte slice using binary.PutVariant for all fields, returns read size in bytes.
func (f *File) Read(b []byte) int {
	var offset, read int
	f.ID, read = binary.Varint(b[offset:])
	offset += read
	f.Size, read = binary.Varint(b[offset:])
	offset += read
	f.Offset, read = binary.Varint(b[offset:])
	offset += read
	f.Timestamp, read = binary.Varint(b[offset:])
	return offset + read
}

// Put file to byte slice using binary.PutVariant for all fields, returns write size in bytes.
func (f File) Put(b []byte) int {
	var offset int
	offset += binary.PutVarint(b[offset:], f.ID)
	offset += binary.PutVarint(b[offset:], f.Size)
	offset += binary.PutVarint(b[offset:], f.Offset)
	offset += binary.PutVarint(b[offset:], f.Timestamp)
	return offset
}
