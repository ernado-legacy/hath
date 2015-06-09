package hath // import "cydev.ru/hath"

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
	"time"

	"github.com/dineshappavoo/basex"
)

const (
	keyStampEnd  = "hotlinkthis"
	prefixLenght = 2
)

// File is hath file representation
type File struct {
	Hash      string    `json:"hash"`
	Size      int64     `json:"size"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	Type      string    `json:"type"`
	LastUsage time.Time `json:"last_usage"`
}

// Dir is first prefixLenght chars of file hash
func (f File) Dir() string {
	return f.Hash[:prefixLenght]
}

// Path returns relative path to file
func (f File) Path() string {
	return path.Join(f.Dir(), f.String())
}

// Use sets LastUsage to current time
func (f *File) Use() {
	f.LastUsage = time.Now()
}

// FileFromID generates new File from provided ID
func FileFromID(fileid string) (f File, err error) {
	elems := strings.Split(fileid, keyStampDelimiter)
	if len(elems) != 5 {
		return f, io.ErrUnexpectedEOF
	}
	f.Hash = elems[0]
	f.Size, err = strconv.ParseInt(elems[1], 10, 64)
	if err != nil {
		return
	}
	f.Width, err = strconv.Atoi(elems[2])
	if err != nil {
		return
	}
	f.Height, err = strconv.Atoi(elems[3])
	if err != nil {
		return
	}
	f.Type = elems[4]
	return f, err
}

func (f File) String() string {
	elems := []string{
		f.Hash,
		sInt64(f.Size),
		strconv.Itoa(f.Width),
		strconv.Itoa(f.Height),
		f.Type,
	}
	return strings.Join(elems, keyStampDelimiter)
}

// KeyStamp generates file key for provided timestamp
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
	d := f.ByteID()
	n := big.NewInt(0)
	n.SetBytes(d)
	return basex.Encode(n.String())
}

// ByteID returns []byte for file hash
func (f File) ByteID() []byte {
	d, err := hex.DecodeString(f.Hash)
	if err != nil {
		panic("hath => bad id")
	}
	return d
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
