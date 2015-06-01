package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

// File is hath file representation
type File struct {
	hash     string
	size     int64
	width    int
	height   int
	filetype string
}

// Path returns relative path to file
func (f File) Path() string {
	return path.Join(f.hash[0:2], f.String())
}

// FileFromID generates new File from provided ID
func FileFromID(fileid string) (f File, err error) {
	elems := strings.Split(fileid, keyStampDelimiter)
	if len(elems) != 5 {
		return f, io.ErrUnexpectedEOF
	}
	f.hash = elems[0]
	f.size, err = strconv.ParseInt(elems[1], 10, 64)
	if err != nil {
		return
	}
	f.width, err = strconv.Atoi(elems[2])
	if err != nil {
		return
	}
	f.height, err = strconv.Atoi(elems[3])
	if err != nil {
		return
	}
	f.filetype = elems[4]
	return f, err
}

func (f File) String() string {
	elems := []string{
		f.hash,
		sInt64(f.size),
		strconv.Itoa(f.width),
		strconv.Itoa(f.height),
		f.filetype,
	}
	return strings.Join(elems, keyStampDelimiter)
}

func getFileSHA1(name string) (hash string, err error) {
	f, err := os.Open(name)
	if err != nil {
		return
	}
	hasher := sha1.New()
	if _, err = io.Copy(hasher, f); err != nil {
		return
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
