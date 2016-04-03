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
	hBuf := make([]byte, 0, f.Size)
	hRead, err := bulk.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	if err := bulk.ReadData(hRead, hBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	if hRead != f {
		t.Errorf("%v != %v", hRead, f)
	}
	hBuf = hBuf[:hRead.Size]
	if int64(len(hBuf)) != f.Size {
		t.Errorf("data.Len() %d != %d", data.Len(), f.Size)
	}
	if string(hBuf) != s {
		t.Errorf("%s != %s", string(hBuf), s)
	}
}

func TestBulk_Write(t *testing.T) {
	backend := tempFile(t)
	defer clearTempFile(backend, t)
	bulk := Bulk{Backend: backend}
	s := "Data data data data data!"
	data := bytes.NewBufferString(s)
	h := Header{
		Size:      int64(data.Len()),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        0,
	}
	if err := bulk.Write(h, data.Bytes()); err != nil {
		t.Fatal("bulk.Read", err)
	}
	l := Link{
		ID:     h.ID,
		Offset: 0,
	}
	hBuf := make([]byte, 0, h.Size)
	hRead, err := bulk.ReadHeader(l, hBuf)
	if err != nil {
		t.Error("bulk.ReadInfo", err)
	}
	if err := bulk.ReadData(hRead, hBuf); err != nil {
		t.Error("bulk.Read", err)
	}
	if hRead != h {
		t.Errorf("%v != %v", hRead, h)
	}
	hBuf = hBuf[:hRead.Size]
	if int64(len(hBuf)) != hRead.Size {
		t.Errorf("len(hBuf) %d != %d", len(hBuf), hRead.Size)
	}
	if string(hBuf) != s {
		t.Errorf("%s != %s", string(hBuf), s)
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
	hBuf := make([]byte, 0, tmpHeader.Size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hBuf = hBuf[:0]
		fRead, err := bulk.ReadHeader(l, hBuf)
		if err != nil {
			b.Error("bulk.ReadInfo", err)
		}
		if err = bulk.ReadData(fRead, hBuf); err != nil {
			b.Error("bulk.Read", err)
		}
		if err != nil {
			b.Error(err)
		}
	}
}
