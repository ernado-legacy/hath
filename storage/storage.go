// Package storage defines a way to save, load, list and delete files for local storage system
// and is optimized for small files
package storage

import "encoding/binary"

// ID is File locally unique identity
type ID uint64

// File represents a stored data, obtainable with ReadAt(data, offset)
// where len(b) = Size and offset=Offset
type File struct {
	ID        uint64
	Size      int64
	Offset    int64
	Timestamp int64
}

// FileStructureSize is minimum buf length required in File.{Read,Put} and is 256 bit or 32 byte
const FileStructureSize = 8 * 4

// Link is index entry that links file id with offset
type Link struct {
	ID     uint64
	Offset int64
}

// Read file from byte slice using binary.Put(U)Variant for all fields, returns read size in bytes
func (f *File) Read(buf []byte) int {
	var offset, read int
	f.ID, read = binary.Uvarint(buf[offset:])
	offset += read
	f.Size, read = binary.Varint(buf[offset:])
	offset += read
	f.Offset, read = binary.Varint(buf[offset:])
	offset += read
	f.Timestamp, read = binary.Varint(buf[offset:])
	return offset + read
}

// Put file to byte slice using binary.Put(U)Variant for all fields, returns write size in bytes
func (f File) Put(buf []byte) int {
	var offset int
	offset += binary.PutUvarint(buf[offset:], f.ID)
	offset += binary.PutVarint(buf[offset:], f.Size)
	offset += binary.PutVarint(buf[offset:], f.Offset)
	offset += binary.PutVarint(buf[offset:], f.Timestamp)
	return offset
}
