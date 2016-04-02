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
	f := File{
		Size:      int64(data.Len()),
		Offset:    0,
		Timestamp: time.Now().Unix(),
		ID:        144,
	}
	buf := NewFileBuffer()
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
	fRead, err := bulk.Read(l, data)
	if err != nil {
		t.Error("bulk.Read", err)
	}
	if fRead != f {
		t.Errorf("%v != %v", fRead, f)
	}
	if int64(data.Len()) != f.Size {
		t.Errorf("data.Len()")
	}
}
