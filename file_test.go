package main

import (
	"testing"

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

func TestFileID(t *testing.T) {
	f := File{}
	f.hash = "070b45ae488fb1967aaf618561a7d6ba4d28a1c9"
	f.size = 12345
	f.height = 1080
	f.width = 1920
	f.filetype = "png"
	Convey("File ID", t, func() {
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
			So(parsed.hash, ShouldEqual, f.hash)
			So(parsed.size, ShouldEqual, f.size)
			So(parsed.width, ShouldEqual, f.width)
			So(parsed.height, ShouldEqual, f.height)
			So(parsed.filetype, ShouldEqual, f.filetype)
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
