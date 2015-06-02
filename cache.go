package main

import (
	"io"
	"os"
	"path"
)

// Cache is engine for serving files in hath
type Cache interface {
	Get(file File) (io.ReadCloser, error)
	Delete(file File) error
	Add(file File, r io.Reader) error
}

// FileCache serves files from disk
type FileCache struct {
	dir string
}

// Get returns readcloser for file
// if file does not exist, it will return error
func (c *FileCache) Get(file File) (io.ReadCloser, error) {
	return os.Open(c.path(file))
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
		return io.ErrUnexpectedEOF
	}
	return nil
}
