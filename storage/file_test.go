package storage

import (
	"testing"
	"time"
)

func TestFile(t *testing.T) {
	f := File{
		ID:        1234,
		Size:      33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := make([]byte, FileStructureSize)
	f.Put(buf)
	readF := File{}
	readF.Read(buf)
	if f != readF {
		t.Errorf("%v != %v", readF, f)
	}
}

func BenchmarkFile_Put(b *testing.B) {
	f := File{
		ID:        1234,
		Size:      33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := make([]byte, FileStructureSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Put(buf)
	}
}

func BenchmarkFile_Read(b *testing.B) {
	f := File{
		ID:        1234,
		Size:      33455,
		Timestamp: time.Now().Unix(),
		Offset:    66234,
	}
	buf := make([]byte, FileStructureSize)
	f.Put(buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Read(buf)
	}
}
