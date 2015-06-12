package hath

import (
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	testRandSHA1Bytes       = 100
	randDirPrefix           = "hath-test"
	randFileSizeMin         = 1024 * 1
	randFileSizeMax         = 1024 * 5
	randFileSizeDelta       = randFileSizeMax - randFileSizeMin
	randFileResolutionMin   = 200
	randFileResolutionMax   = 3000
	randFileResolutionDelta = randFileResolutionMax - randFileResolutionMin
)

func randSHA1() string {
	b := make([]byte, testRandSHA1Bytes)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func TestFileCache(t *testing.T) {
	testDir, err := ioutil.TempDir("", randDirPrefix)
	testFilesCount := 5
	testFiles := make([]File, testFilesCount)
	defer os.RemoveAll(testDir)
	g := FileGenerator{
		SizeMax:       randFileSizeMax,
		SizeMin:       randFileSizeMin,
		ResolutionMax: randFileResolutionMax,
		ResolutionMin: randFileResolutionMin,
		Dir:           testDir,
	}
	Convey("File cache", t, func() {
		So(err, ShouldBeNil)
		So(testDir, ShouldNotEqual, "")
		for i := 0; i < testFilesCount; i++ {
			f, err := g.New()
			So(err, ShouldBeNil)
			So(f.Size, ShouldNotEqual, 0)
			testFiles[i] = f
		}
		Convey("Init", func() {
			// cache init
			c := new(FileCache)
			c.dir = testDir

			// selecting random file
			f := testFiles[mrand.Intn(testFilesCount)]

			// checking that all ok
			So(c.Check(f), ShouldBeNil)

			// testing get
			r, err := c.Get(f)
			So(err, ShouldBeNil)
			So(r.Close(), ShouldBeNil)
			Convey("Frontend", func() {
				Convey("OK", func() {
					f, err := g.New()
					So(err, ShouldBeNil)
					frontend := NewDirectFrontend(c)
					rec := httptest.NewRecorder()
					err = frontend.Handle(f, rec)
					So(err, ShouldBeNil)
					So(rec.Code, ShouldEqual, http.StatusOK)
				})
				Convey("Not found", func() {
					f, err := g.New()
					So(err, ShouldBeNil)
					f.SetHash("070b45aebadfb1967bad618bada7d6ba4d28a1c9")
					frontend := NewDirectFrontend(c)
					rec := httptest.NewRecorder()
					err = frontend.Handle(f, rec)
					So(err, ShouldEqual, ErrFileNotFound)
					So(rec.Code, ShouldEqual, http.StatusNotFound)
				})
				Convey("Bad length", func() {
					f, err := g.New()
					So(err, ShouldBeNil)
					newpath := path.Join(testDir, f.Path())
					w, err := os.OpenFile(newpath, os.O_APPEND|os.O_WRONLY, 0600)
					So(err, ShouldBeNil)
					w.Write([]byte("corrupt!"))
					w.Close()
					frontend := NewDirectFrontend(c)
					rec := httptest.NewRecorder()
					err = frontend.Handle(f, rec)
					So(err, ShouldEqual, ErrFileBadLength)
					So(rec.Code, ShouldEqual, http.StatusOK)
				})
			})
			Convey("Delete", func() {
				f, err := g.New()
				So(err, ShouldBeNil)
				So(c.Remove(f), ShouldBeNil)
				_, err = c.Get(f)
				So(err, ShouldEqual, ErrFileNotFound)
			})
			Convey("Add", func() {
				f, err := g.New()
				So(err, ShouldBeNil)
				oldpath := path.Join(testDir, f.Path())
				newpath := oldpath + ".new"
				So(os.Rename(oldpath, newpath), ShouldBeNil)
				_, err = c.Get(f)
				So(err, ShouldEqual, ErrFileNotFound)
				r, err := os.Open(newpath)
				defer r.Close()
				So(err, ShouldBeNil)
				So(c.Add(f, r), ShouldBeNil)
				r.Close()
				Convey("Length inconsistency", func() {
					// So(c.Delete(f), ShouldBeNil)
					w, err := os.OpenFile(newpath, os.O_APPEND|os.O_WRONLY, 0600)
					So(err, ShouldBeNil)
					w.Write([]byte("corrupt!"))
					w.Close()
					r, err := os.Open(newpath)
					So(err, ShouldBeNil)
					defer r.Close()
					So(c.Add(f, r), ShouldEqual, ErrFileBadLength)
				})
				Convey("Rewrite", func() {
					r, err := os.Open(newpath)
					So(err, ShouldBeNil)
					So(c.Add(f, r), ShouldBeNil)
				})
				Convey("Check", func() {
					So(c.Check(f), ShouldBeNil)
					corruptBytes := []byte("corruptsssssss!")
					Convey("Length", func() {
						// corrupting the file by length
						w, err := os.OpenFile(path.Join(testDir, f.Path()), os.O_APPEND|os.O_RDWR, 0666)
						So(err, ShouldBeNil)
						w.Write(corruptBytes)
						w.Close()
						So(c.Check(f), ShouldEqual, ErrFileBadLength)
					})
					Convey("Hash", func() {
						// corrupting the file by hash
						w, err := os.OpenFile(path.Join(testDir, f.Path()), os.O_RDWR, 0666)
						So(err, ShouldBeNil)
						_, err = w.Seek(0, os.SEEK_SET)
						So(err, ShouldBeNil)
						_, err = w.Write(corruptBytes)
						So(err, ShouldBeNil)
						So(w.Truncate(f.Size), ShouldBeNil)
						So(w.Close(), ShouldBeNil)
						So(c.Check(f), ShouldEqual, ErrFileInconsistent)
					})
					Convey("Delete", func() {
						f, err := g.New()
						So(err, ShouldBeNil)
						So(c.Remove(f), ShouldBeNil)
						So(c.Check(f), ShouldEqual, ErrFileNotFound)
					})
				})
			})
		})
	})
}
