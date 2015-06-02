package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/dineshappavoo/basex"
)

const (
	keyStampEnd = "hotlinkthis"
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

func (f File) KeyStamp(key string, timestamp int64) string {
	elems := []string{
		sInt64(timestamp),
		f.String(),
		key,
		keyStampEnd,
	}
	toHash := strings.Join(elems, keyStampDelimiter)
	hash := sha1.Sum([]byte(toHash))
	return fmt.Sprintf("%x", hash)[:10]
}

// Basex returns basex representation of hash
func (f File) Basex() string {
	d, _ := hex.DecodeString(f.hash)
	n := big.NewInt(0)
	n.SetBytes(d)
	return basex.Encode(n.String())
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
