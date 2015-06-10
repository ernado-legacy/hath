package hath // import "cydev.ru/hath"

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

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
