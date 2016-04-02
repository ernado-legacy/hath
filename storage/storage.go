// package storage defines a way to save, load, list and delete files for local storage system
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

// Link is index entry that links file id with offset
type Link struct {
	ID     uint64
	Offset int64
}

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

func (f File) Put(buf []byte) int {
	var offset int
	offset += binary.PutUvarint(buf[offset:], f.ID)
	offset += binary.PutVarint(buf[offset:], f.Size)
	offset += binary.PutVarint(buf[offset:], f.Offset)
	offset += binary.PutVarint(buf[offset:], f.Timestamp)
	return offset
}
