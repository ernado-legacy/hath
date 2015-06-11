package hath // import "cydev.ru/hath"

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/dineshappavoo/basex"
	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	keyStampEnd  = "hotlinkthis"
	prefixLenght = 2
	// HashSize is length of sha1 hash in bytes
	HashSize = 20
)

// FileType represents file format of image
type FileType byte

func (f FileType) String() string {
	if f == JPG {
		return "jpg"
	}
	if f == PNG {
		return "png"
	}
	if f == GIF {
		return "gif"
	}
	return "tmp"
}

const (
	// JPG image
	JPG FileType = iota
	// PNG image
	PNG
	// GIF animation
	GIF
	// UnknownImage is not supported format
	UnknownImage
)

var (
	// ErrFileTypeUnknown when FileType is UnknownImage
	ErrFileTypeUnknown = errors.New("hath => file type unknown")
	// ErrHashBadLength when hash size is not HashSize
	ErrHashBadLength = errors.New("hath => hash of image has bad length")
)

// ParseFileType returns FileType from string
func ParseFileType(filetype string) FileType {
	filetype = strings.ToLower(filetype)
	if filetype == "jpg" || filetype == "jpeg" {
		return JPG
	}
	if filetype == "png" {
		return PNG
	}
	if filetype == "gif" {
		return GIF
	}
	return UnknownImage
}

// File is hath file representation
// total 20 + 3 + 2 + 2 + 1 + 8 + 1 = 37 bytes
type File struct {
	Hash [HashSize]byte `json:"hash"` // 20 byte
	Type FileType       `json:"type"` // 1 byte
	// Static files should never be removed
	Static bool  `json:"static"` // 1 byte
	Size   int64 `json:"size"`   // 4 byte (maximum size 4095mb)
	Width  int   `json:"width"`  // 2 byte
	Height int   `json:"height"` // 2 byte
	// LastUsage is Unix timestamp
	LastUsage int64 `json:"last_usage"` // 8 byte (can be optimized)
}

func (f File) indexKey() []byte {
	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, uint64(f.LastUsage))
	elems := [][]byte{
		timeBytes,
		f.Hash[:],
	}
	return bytes.Join(elems, nil)
}

// LastUsageBefore returns true, if last usage occured before deadline t
func (f File) LastUsageBefore(t time.Time) bool {
	return t.Unix() < f.LastUsage
}

// Dir is first prefixLenght chars of file hash
func (f File) Dir() string {
	return f.HexID()[:prefixLenght]
}

// Path returns relative path to file
func (f File) Path() string {
	return path.Join(f.Dir(), f.String())
}

// Use sets LastUsage to current time
func (f *File) Use() {
	f.LastUsage = time.Now().Unix()
}

// HexID returns hex representation of hash
func (f File) HexID() string {
	return fmt.Sprintf("%x", f.Hash)
}

// SetHash sets hash from string
func (f *File) SetHash(s string) error {
	hash, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(hash) != HashSize {
		return ErrHashBadLength
	}
	copy(f.Hash[:], hash[:HashSize])
	return nil
}

// FileFromID generates new File from provided ID
func FileFromID(fileid string) (f File, err error) {
	elems := strings.Split(fileid, keyStampDelimiter)
	if len(elems) != 5 {
		return f, io.ErrUnexpectedEOF
	}
	if err = f.SetHash(elems[0]); err != nil {
		return
	}
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
	f.Type = ParseFileType(elems[4])
	return f, err
}

func (f File) String() string {
	elems := []string{
		f.HexID(),
		sInt64(f.Size),
		strconv.Itoa(f.Width),
		strconv.Itoa(f.Height),
		f.Type.String(),
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

// Marshal serializes file info
func (f File) Marshal() ([]byte, error) {
	var buff bytes.Buffer
	encoder := msgpack.NewEncoder(&buff)
	err := encoder.Encode(f)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), err
}

// UnmarshalFile deserializes file info fron byte array
func UnmarshalFile(data []byte) (f File, err error) {

	buff := bytes.NewBuffer(data)
	decoder := msgpack.NewDecoder(buff)
	return f, decoder.Decode(&f)
}

// UnmarshalFileTo deserializes file info fron byte array by pointer
func UnmarshalFileTo(data []byte, f *File) error {
	buff := bytes.NewBuffer(data)
	decoder := msgpack.NewDecoder(buff)
	return decoder.Decode(f)
}

// ByteID returns []byte for file hash
func (f File) ByteID() []byte {
	return f.Hash[:]
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
