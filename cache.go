package main

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path"
)

var (
	// ErrFileNotFound should be returned when file does not exist in frontend
	ErrFileNotFound = errors.New("File not found in cache")
	// ErrFileBadLength means that file size does not equals read size
	ErrFileBadLength = errors.New("Bad lenght in file")
)

// Frontend is cache backend that should processes
// requests to specidif files, returning them
// with correct headers and processing IO errors
type Frontend interface {
	Handle(file File, w *http.ResponseWriter) error
}

// DirectFrontend is frontend that uses DirectCache
type DirectFrontend struct {
	cache DirectCache
}

// Handle request for file
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
	w.WriteHeader(http.StatusInternalServerError)
	return err
}

// DirectCache is engine for serving files in hath
type DirectCache interface {
	Get(file File) (io.ReadCloser, error)
	Delete(file File) error
	Add(file File, r io.Reader) error
}

// FileCache serves files from disk
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

func (c *FileCache) path(file File) string {
	return path.Join(c.dir, file.Path())
}

// Add saves file to storage
func (c *FileCache) Add(file File, r io.Reader) error {
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
