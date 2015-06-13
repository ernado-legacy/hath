package hath // import "cydev.ru/hath"

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"path"
	"time"
)

const (
	avgFileSize uint64 = 493 * 1024 // 493kb
)

var (
	// ErrFileNotFound should be returned when file does not exist in frontend
	ErrFileNotFound = errors.New("File not found in cache")
	// ErrFileBadLength means that file size does not equals read size
	ErrFileBadLength = errors.New("Bad lenght in file")

	// ErrFileInconsistent should be returned if file failed to check sha1 hash
	ErrFileInconsistent = errors.New("File has bad hash")
)

var (
	// FileTypes list for allowerd images
	FileTypes = []string{"jpg", "png", "gif"}
	// FileTypesN count of FileTypes
	FileTypesN = len(FileTypes)
)

// Frontend is cache backend that should processes
// requests to specidif files, returning them
// with correct headers and processing IO errors
type Frontend interface {
	Handle(file File, w http.ResponseWriter) error
	DirectCache
}

// DirectFrontend is frontend that uses DirectCache
type DirectFrontend struct {
	cache DirectCache
}

// Handle request for file
// returns ErrFileNotFound, ErrFileBadLength
// can return unexpected errors
func (d *DirectFrontend) Handle(file File, w http.ResponseWriter) error {
	f, err := d.cache.Get(file)
	if err == ErrFileNotFound {
		w.WriteHeader(http.StatusNotFound)
		return err
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	defer f.Close()
	n, err := io.Copy(w, f)
	if n != file.Size {
		return ErrFileBadLength
	}
	return err
}

// NewDirectFrontend create direct frontend
func NewDirectFrontend(cache DirectCache) Frontend {
	return &DirectFrontend{cache}
}

func NewFrontend(dir string) Frontend {
	cache := &FileCache{dir}
	return &DirectFrontend{cache}
}

// Some boilerplate code to make DirectFrontend implement DirectCache
// can be example for implementing other Frontend's

// Add file to frontend
func (d *DirectFrontend) Add(file File, r io.Reader) error {
	return d.cache.Add(file, r)
}

// Remove file
func (d *DirectFrontend) Remove(file File) error {
	return d.cache.Remove(file)
}

// RemoveBatch removes files
func (d *DirectFrontend) RemoveBatch(files []File) error {
	return d.cache.RemoveBatch(files)
}

// Get returns file from fontend
func (d *DirectFrontend) Get(file File) (io.ReadCloser, error) {
	return d.cache.Get(file)
}

// Check check da file
func (d *DirectFrontend) Check(file File) error {
	return d.cache.Check(file)
}

// DirectCache is engine for serving files in hath directly from block devices
// i.e. not using any redirects
type DirectCache interface {
	Get(file File) (io.ReadCloser, error)
	Remove(f File) error
	RemoveBatch(f []File) error
	Add(file File, r io.Reader) error
	Check(file File) error
}

// FileCache serves files from disk
// no internal buffering, caching or rate limiting is done
// and should be implement separetaly
type FileCache struct {
	dir string
}

// Get returns readcloser for file
// if file does not exist, it will return ErrFileNotFound
func (c *FileCache) Get(file File) (io.ReadCloser, error) {
	f, err := os.Open(c.path(file))
	if os.IsNotExist(err) {
		return nil, ErrFileNotFound
	}
	return f, err
}

// Remove removes file from storage
func (c *FileCache) Remove(file File) error {
	return os.Remove(c.path(file))
}

// RemoveBatch removes files from storage
func (c *FileCache) RemoveBatch(files []File) error {
	for _, f := range files {
		if err := c.Remove(f); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// path returns absolute(or relative) path
// that starts with cache directory
// used only internaly
func (c *FileCache) path(file File) string {
	return path.Join(c.dir, file.Path())
}

// Add saves file to storage
func (c *FileCache) Add(file File, r io.Reader) error {
	// creating directory if not exists
	err := os.Mkdir(path.Join(c.dir, file.Dir()), 0777)
	if err != nil && !os.IsExist(err) {
		return err
	}
	f, err := os.Create(c.path(file))
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}
	// checking real and provided size
	if n != file.Size {
		return ErrFileBadLength
	}
	return nil
}

// Check performs sha1 hash checking on file
// returns nil if all ok
func (c *FileCache) Check(file File) error {
	f, err := os.Open(c.path(file))
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	hasher := sha1.New()
	n, err := io.Copy(hasher, f)
	if err != nil {
		return err
	}
	// checking real and provided size
	if n != file.Size {
		return ErrFileBadLength
	}
	// checking hashes
	if !bytes.Equal(file.ByteID(), hasher.Sum(nil)) {
		return ErrFileInconsistent
	}
	return nil
}

// FileGenerator is factory for random files
type FileGenerator struct {
	SizeMax       int64
	SizeMin       int64
	ResolutionMax int
	ResolutionMin int
	Dir           string
	TimeDelta     int64
}

// NewFake generates random file without writing it on disk
func (g FileGenerator) NewFake() (f File) {
	f.Size = mrand.Int63n(g.SizeMax-g.SizeMin) + g.SizeMin
	f.Type = FileType(mrand.Intn(int(UnknownImage)))
	f.Width = mrand.Intn(g.ResolutionMax-g.ResolutionMin) + g.ResolutionMin
	f.Height = mrand.Intn(g.ResolutionMax-g.ResolutionMin) + g.ResolutionMin
	if g.TimeDelta > 0 {
		f.LastUsage = time.Now().Unix() + mrand.Int63n(g.TimeDelta*2) - g.TimeDelta
	} else {
		f.LastUsage = time.Now().Unix()
	}
	b := make([]byte, 20)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	copy(f.Hash[:], b)
	t := mrand.Intn(10)
	if t > 5 {
		f.Static = true
	}

	return f
}

// New generates random file and returns it, writing on disk
func (g FileGenerator) New() (f File, err error) {
	// initializing fields with random data
	f = g.NewFake()

	// generating new random file to memory
	hasher := sha1.New()
	buffer := new(bytes.Buffer)
	w := io.MultiWriter(hasher, buffer)
	_, err = io.CopyN(w, rand.Reader, f.Size)
	copy(f.Hash[:], hasher.Sum(nil))

	// making directory if not exists
	err = os.Mkdir(path.Join(g.Dir, f.Dir()), 0777)
	if err != nil && !os.IsExist(err) {
		return f, err
	}
	file, err := os.Create(path.Join(g.Dir, f.Path()))
	defer file.Close()
	if err != nil {
		return f, err
	}
	_, err = io.CopyN(file, buffer, f.Size)
	return f, err
}

// GetRoughCacheSize is cache size estimation based on average file size
func GetRoughCacheSize(count int64) uint64 {
	return avgFileSize * uint64(count)
}
