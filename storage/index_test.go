package storage

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func tempFile(t *testing.T) *os.File {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("tempFile:", err)
	}
	return f
}

func clearTempFile(f *os.File, t *testing.T) {
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Error(err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
}

// memoryBackend is in-memory file backend used in tests
type memoryBackend struct {
	name   string
	buff   bytes.Buffer
	reader bytes.Reader
}

func (m memoryBackend) ReadAt(b []byte, off int64) (int, error) {
	return m.reader.ReadAt(b, off)
}

func (m *memoryBackend) WriteAt(b []byte, off int64) (int, error) {
	n, err := m.buff.Write(b)
	if err != nil {
		return n, err
	}
	m.reader = *bytes.NewReader(m.buff.Bytes())
	return n, nil
}

func (m memoryBackend) Stat() (os.FileInfo, error) {
	return m, nil
}

func (m memoryBackend) Name() string {
	return m.name
}

func (m memoryBackend) Size() int64 {
	return int64(m.buff.Len())
}

func (m memoryBackend) Mode() os.FileMode {
	return os.FileMode(0666)
}

func (m memoryBackend) IsDir() bool {
	return false
}

func (m memoryBackend) Sys() interface{} {
	return m.buff
}

func (m memoryBackend) ModTime() time.Time {
	return time.Time{}
}

func TestLink(t *testing.T) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	l.Put(buf)
	readL := Link{}
	readL.Read(buf)
	if l != readL {
		t.Errorf("%v != %v", readL, l)
	}
}

func BenchmarkLink_Put(b *testing.B) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Put(buf)
	}
}

func BenchmarkLink_Read(b *testing.B) {
	l := Link{
		ID:     1234,
		Offset: 66234,
	}
	buf := make([]byte, LinkStructureSize)
	l.Put(buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Read(buf)
	}
}

func TestGetLink(t *testing.T) {
	l := getLinkOffset(10)
	if l != 160 {
		t.Fatalf("%v != %v", l, 160)
	}
}

func TestIndex_ReadBuff(t *testing.T) {
	var backend memoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			t.Fatal(err)
		}
	}
	backend.buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	readBuf := make([]byte, LinkStructureSize)
	l, err := index.ReadBuff(3, readBuf)
	if err != nil {
		t.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}

func TestIndex_Read(t *testing.T) {
	var backend memoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			t.Fatal(err)
		}
	}
	backend.buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	l, err := index.ReadBuff(3, make([]byte, LinkStructureSize))
	if err != nil {
		t.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}

func BenchmarkIndex_ReadBuff(b *testing.B) {
	var backend memoryBackend
	buf := make([]byte, LinkStructureSize)
	var id int64
	tmpLink := Link{
		ID:     0,
		Offset: 125,
	}
	for id = 0; id < 10; id++ {
		tmpLink.ID = id
		tmpLink.Put(buf)
		if _, err := backend.WriteAt(buf, getLinkOffset(id)); err != nil {
			b.Fatal(err)
		}
	}
	backend.buff = *bytes.NewBuffer(buf)
	index := Index{Backend: &backend}
	l, err := index.ReadBuff(3, make([]byte, LinkStructureSize))
	if err != nil {
		b.Fatal(err)
	}
	expected := Link{ID: 3, Offset: 125}
	if l != expected {
		b.Errorf("%v != %v", l, expected)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := index.ReadBuff(3, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexOsFile(t *testing.T) {
	f := tempFile(t)
	defer clearTempFile(f, t)
	index := Index{Backend: f}
	b := NewLinkBuffer()
	expected := Link{
		ID:     0,
		Offset: 1234,
	}
	if err := index.WriteBuff(expected, b); err != nil {
		t.Error(err)
	}
	l, err := index.ReadBuff(expected.ID, make([]byte, LinkStructureSize))
	if err != nil {
		t.Error(err)
	}
	if l != expected {
		t.Errorf("%v != %v", l, expected)
	}
}
