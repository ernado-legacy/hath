package storage

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestBulk_Read(t *testing.T) {
	backend := tempFile(t)
	defer clearTempFile(backend, t)
	bulk := Bulk{Backend: backend}
	s := "Data data data data data!"
	data := bytes.NewBufferString(s)
	f := Header{
		Size:      int64(data.Len()),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        144,
	}
	buf := NewHeaderBuffer()
	f.Put(buf)
	if _, err := backend.Write(buf); err != nil {
		t.Fatal("backend.Write", err)
	}
	if _, err := data.WriteTo(backend); err != nil {
		t.Fatal("data.WriteTo", err)
	}
	if _, err := backend.Seek(0, os.SEEK_SET); err != nil {
		t.Fatal("backend.Seek", err)
	}
	l := Link{
		ID:     f.ID,
		Offset: 0,
	}
	fBuf := make([]byte, 0, f.Size)
	fRead, err := bulk.ReadFile(l, fBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	if err := bulk.ReadData(fRead, fBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	if fRead != f {
		t.Errorf("%v != %v", fRead, f)
	}
	fBuf = fBuf[:fRead.Size]
	if int64(len(fBuf)) != f.Size {
		t.Errorf("data.Len() %d != %d", data.Len(), f.Size)
	}
	if string(fBuf) != s {
		t.Errorf("%s != %s", string(fBuf), s)
	}
}

func TestBulk_Write(t *testing.T) {
	backend := tempFile(t)
	defer clearTempFile(backend, t)
	bulk := Bulk{Backend: backend}
	s := "Data data data data data!"
	data := bytes.NewBufferString(s)
	f := Header{
		Size:      int64(data.Len()),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        144,
	}
	if err := bulk.Write(f, data.Bytes()); err != nil {
		t.Fatal("bulk.Read", err)
	}
	l := Link{
		ID:     f.ID,
		Offset: 0,
	}
	fBuf := make([]byte, 0, f.Size)
	fRead, err := bulk.ReadFile(l, fBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	if err := bulk.ReadData(fRead, fBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	if fRead != f {
		t.Errorf("%v != %v", fRead, f)
	}
	fBuf = fBuf[:f.Size]
	if int64(len(fBuf)) != f.Size {
		t.Errorf("data.Len() %d != %d", data.Len(), f.Size)
	}
	if string(fBuf) != s {
		t.Errorf("%s != %s", string(fBuf), s)
	}
}

func BenchmarkBulk_Read(b *testing.B) {
	var backend memoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 0,
	}
	tmpHeader := Header{
		ID:        0,
		Offset:    0,
		Timestamp: time.Now().Unix(),
	}
	data := []byte("Data data data data data!")
	tmpHeader.Size = int64(len(data))
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpHeader.Offset = id * (tmpHeader.Size + LinkStructureSize)
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		if _, err := backend.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
	}
	bulk := Bulk{Backend: &backend}
	l := Link{
		ID:     3,
		Offset: (tmpHeader.Size + LinkStructureSize) * 3,
	}
	fBuf := make([]byte, 0, tmpHeader.Size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fBuf = fBuf[:0]
		fRead, err := bulk.ReadFile(l, fBuf)
		if err != nil {
			b.Error("bulk.ReadInfo", err)
		}
		if err = bulk.ReadData(fRead, fBuf); err != nil {
			b.Error("bulk.Read", err)
		}
		if err != nil {
			b.Error(err)
		}
	}
}
