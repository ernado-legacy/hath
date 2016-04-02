package storage

import (
	"bytes"
	"os"
	"time"
)

// memoryIndexBackend is in-memory backend for Index used in tests
type memoryIndexBackend struct {
	name   string
	buff   bytes.Buffer
	reader bytes.Reader
}

func (m memoryIndexBackend) ReadAt(b []byte, off int64) (int, error) {
	return m.reader.ReadAt(b, off)
}

func (m *memoryIndexBackend) WriteAt(b []byte, off int64) (int, error) {
	n, err := m.buff.Write(b)
	if err != nil {
		return n, err
	}
	m.reader = *bytes.NewReader(m.buff.Bytes())
	return n, nil
}

func (m memoryIndexBackend) Stat() (os.FileInfo, error) {
	return m, nil
}

func (m memoryIndexBackend) Name() string {
	return m.name
}

func (m memoryIndexBackend) Size() int64 {
	return int64(m.buff.Len())
}

func (m memoryIndexBackend) Mode() os.FileMode {
	return os.FileMode(0666)
}

func (m memoryIndexBackend) IsDir() bool {
	return false
}

func (m memoryIndexBackend) Sys() interface{} {
	return m.buff
}

func (m memoryIndexBackend) ModTime() time.Time {
	return time.Time{}
}
