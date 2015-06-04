package hath // import "cydev.ru/hath"

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"path"
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
	FileTypes  = []string{"jpg", "png", "gif"}
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
	if n != file.size {
		return ErrFileBadLength
	}
	return err
}

func NewDirectFrontend(cache DirectCache) Frontend {
	return &DirectFrontend{cache}
}

// Some boilerplate code to make DirectFrontend implement DirectCache
// can be example for implementing other Frontend's

// Add file to frontend
func (d *DirectFrontend) Add(file File, r io.Reader) error {
	return d.Add(file, r)
}

// Delete file
func (d *DirectFrontend) Delete(file File) error {
	return d.Delete(file)
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
	Delete(file File) error
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

// Delete removes file from storage
func (c *FileCache) Delete(file File) error {
	return os.Remove(c.path(file))
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
	err := os.Mkdir(path.Join(c.dir, file.hash[0:2]), 0777)
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
	if n != file.size {
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
	if n != file.size {
		return ErrFileBadLength
	}
	// checking hashes
	hash := fmt.Sprintf("%x", hasher.Sum(nil))
	if file.hash != hash {
		return ErrFileInconsistent
	}
	return nil
}

type FileGenerator struct {
	SizeMax       int64
	SizeMin       int64
	ResolutionMax int
	ResolutionMin int
	Dir           string
}

// SumHasher is hasher that can sum
type SumHasher interface {
	Sum([]byte) []byte
}

// GetHexHash extracts hexademical representation
// from hasher
func GetHexHash(hasher SumHasher) string {
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// New generates random file and returns it
func (g FileGenerator) New() (f File, err error) {
	// initializing fields with random data
	f.size = mrand.Int63n(g.SizeMax-g.SizeMin) + g.SizeMin
	f.filetype = FileTypes[mrand.Intn(FileTypesN)]
	f.width = mrand.Intn(g.ResolutionMax-g.ResolutionMin) + g.ResolutionMin
	f.height = mrand.Intn(g.ResolutionMax-g.ResolutionMin) + g.ResolutionMin

	// generating new random file to memory
	hasher := sha1.New()
	buffer := new(bytes.Buffer)
	w := io.MultiWriter(hasher, buffer)
	_, err = io.CopyN(w, rand.Reader, f.size)
	f.hash = GetHexHash(hasher)

	// making directory if not exists
	err = os.Mkdir(path.Join(g.Dir, f.hash[0:2]), 0777)
	if err != nil && !os.IsExist(err) {
		return f, err
	}
	file, err := os.Create(path.Join(g.Dir, f.Path()))
	defer file.Close()
	if err != nil {
		return f, err
	}
	_, err = io.CopyN(file, buffer, f.size)
	return f, err
}
