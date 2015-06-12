package hath // import "cydev.ru/hath"

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

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

func TestFileSHA1(t *testing.T) {
	filepath := "test/gopher.jpg"
	Convey("SHA1 of file", t, func() {
		Convey("OK", func() {
			s, err := getFileSHA1(filepath)
			So(err, ShouldBeNil)
			So(s, ShouldEqual, "070b45ae488fb1967aaf618561a7d6ba4d28a1c9")
		})
		Convey("Not found", func() {
			_, err := getFileSHA1(filepath + ".none")
			So(err, ShouldNotBeNil)
		})

	})
}

func TestFile(t *testing.T) {
	Convey("File", t, func() {
		Convey("Use", func() {
			f := new(File)
			f.Use()
			So(f.LastUsage, ShouldEqual, time.Now().Unix())
		})
	})
}

func TestFileIndex(t *testing.T) {
	Convey("Indexes", t, func() {
		Convey("Newer", func() {
			f := defaultGenerator.NewFake()
			t := time.Now()
			f.LastUsage = time.Now().Unix()
			keyOlder := f.indexKey()
			f.LastUsage = time.Now().Unix() + 50
			keyNewer := f.indexKey()
			// keyOlder should be less than keyNewer
			So(bytes.Compare(keyOlder[:], keyNewer[:]), ShouldEqual, -1)
			Convey("Extract ID", func() {
				hash := getIDFromIndexKey(f.indexKey())
				So(bytes.Compare(hash, f.ByteID()), ShouldEqual, 0)
			})
			Convey("Start/end", func() {
				f.LastUsage = t.Unix()
				start := getIndexStart(t)
				end := getIndexEnd(t)
				So(bytes.Compare(start, f.indexKey()), ShouldEqual, -1)
				So(bytes.Compare(f.indexKey(), end), ShouldEqual, -1)
			})
		})
		Convey("Start/end", func() {
			t := time.Now()
			start := getIndexStart(t)
			end := getIndexEnd(t)
			log.Println(start, end)
			So(bytes.Compare(start, end), ShouldEqual, -1)
		})
	})
}

var defaultGenerator = FileGenerator{
	SizeMax:       10 * 1024 * 1024,
	SizeMin:       200 * 1024,
	ResolutionMax: 2500,
	ResolutionMin: 300,
	TimeDelta:     20,
}

func TestFileSerialization(t *testing.T) {
	g := defaultGenerator
	Convey("Serialization", t, func() {
		Convey("Serialize", func() {
			f := g.NewFake()
			b := f.Bytes()
			So(len(b), ShouldEqual, fileBytes)
			Convey("Deserialize", func() {
				resultFile, err := FileFromBytes(b)
				So(err, ShouldBeNil)
				So(f.Static, ShouldEqual, resultFile.Static)
				So(f.HexID(), ShouldEqual, resultFile.HexID())
				So(f.String(), ShouldEqual, resultFile.String())
			})
		})
		Convey("Random data", func() {
			count := 10000
			failures := 0
			for i := 0; i < count; i++ {
				f := g.NewFake()
				b := f.Bytes()
				nf, err := FileFromBytes(b)
				if err != nil {
					failures++
				}
				if nf != f {
					failures++
				}
			}
			So(failures, ShouldEqual, 0)
		})
	})
}

func TestFileMarshalling(t *testing.T) {
	g := defaultGenerator
	Convey("Marshalling", t, func() {
		Convey("Serialize", func() {
			f := g.NewFake()
			b, err := f.Marshal()
			So(err, ShouldBeNil)
			So(len(b), ShouldEqual, fileBytes)
			Convey("Deserialize", func() {
				resultFile, err := FileFromBytes(b)
				So(err, ShouldBeNil)
				So(f.Static, ShouldEqual, resultFile.Static)
				So(f.HexID(), ShouldEqual, resultFile.HexID())
				So(f.String(), ShouldEqual, resultFile.String())
			})
		})
		Convey("Random data", func() {
			count := 10000
			failures := 0
			for i := 0; i < count; i++ {
				f := g.NewFake()
				b := f.Bytes()
				nf, err := FileFromBytes(b)
				if err != nil {
					failures++
				}
				if nf != f {
					failures++
				}
			}
			So(failures, ShouldEqual, 0)
		})
	})
}

func TestFileID(t *testing.T) {
	f := File{}

	f.Size = 12345
	f.Height = 1080
	f.Width = 1920
	f.Type = PNG
	Convey("File ID", t, func() {
		So(f.SetHash("070b45ae488fb1967aaf618561a7d6ba4d28a1c9"), ShouldBeNil)
		Convey("Generation", func() {
			gotFileID := f.String()
			expectedID := "070b45ae488fb1967aaf618561a7d6ba4d28a1c9-12345-1920-1080-png"
			So(gotFileID, ShouldEqual, expectedID)
		})
		Convey("Keystamp", func() {
			gotKeystamp := f.KeyStamp("key", 10666)
			expectedKeystamp := "71cf950fcd"
			So(gotKeystamp, ShouldEqual, expectedKeystamp)
		})
		Convey("BaseX", func() {
			expectedID := "10JUYVz94XadJT1GdvnVp0E6x3p"
			So(f.Basex(), ShouldEqual, expectedID)
		})
		Convey("Path", func() {
			expected := "07/070b45ae488fb1967aaf618561a7d6ba4d28a1c9-12345-1920-1080-png"
			actual := f.Path()
			So(expected, ShouldEqual, actual)
		})
		Convey("Parsing", func() {
			fid := "070b45ae488fb1967aaf618561a7d6ba4d28a1c9-12345-1920-1080-png"
			parsed, err := FileFromID(fid)
			So(err, ShouldBeNil)
			So(parsed.Hash, ShouldEqual, f.Hash)
			So(parsed.Size, ShouldEqual, f.Size)
			So(parsed.Width, ShouldEqual, f.Width)
			So(parsed.Height, ShouldEqual, f.Height)
			So(parsed.Type, ShouldEqual, f.Type)
			Convey("Error handling", func() {
				examples := []string{
					"070b45ae488fb1967aaf618561a7d6ba4d28a1c9-?-1920-1080-png",
					"kek-12345-1920-1080a-png",
					"one-two-three",
					"070b45ae488fb1967aaf618561a7d6ba4d28a1c9-12345-pek-1080-png",
					"070b45ae488fb1967aaf618561a7d6ba4d28a1c9-12345-1223-2f-png",
				}
				for _, example := range examples {
					_, err := FileFromID(example)
					So(err, ShouldNotBeNil)
				}
			})
		})
	})
}
