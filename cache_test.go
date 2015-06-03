package hath

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	mrand "math/rand"
	"os"
	"path"
	"testing"
	"time"

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

func saveRandomFile(testDir string) (f File, err error) {
	tmpFile, err := ioutil.TempFile("", randDirPrefix)
	if err != nil {
		return
	}
	defer tmpFile.Close()
	// initializing fields with random data
	f.size = mrand.Int63n(randFileSizeDelta) + randFileSizeMin
	f.filetype = []string{"jpg", "png", "gif"}[mrand.Intn(3)]
	f.width = mrand.Intn(randFileResolutionDelta) + randFileResolutionMin
	f.height = mrand.Intn(randFileResolutionDelta) + randFileResolutionMin

	// generating new random file
	hasher := sha1.New()
	multiWriter := io.MultiWriter(hasher, tmpFile)
	n, err := io.CopyN(multiWriter, rand.Reader, f.size)
	tmpFile.Close()
	if n != f.size {
		return f, ErrFileBadLength
	}
	f.hash = fmt.Sprintf("%x", hasher.Sum(nil))
	// making directory if not exists
	err = os.Mkdir(path.Join(testDir, f.hash[0:2]), 0777)
	if err != nil && !os.IsExist(err) {
		return f, err
	}
	newpath := path.Join(testDir, f.Path())
	oldpath := path.Join(tmpFile.Name())
	err = os.Rename(oldpath, newpath)
	return f, err
}

func TestFileCache(t *testing.T) {
	testDir, err := ioutil.TempDir("", randDirPrefix)
	testFilesCount := 5
	testFiles := make([]File, testFilesCount)
	defer os.RemoveAll(testDir)
	Convey("File cache", t, func() {
		So(err, ShouldBeNil)
		So(testDir, ShouldNotEqual, "")
		start := time.Now()
		for i := 0; i < testFilesCount; i++ {
			f, err := saveRandomFile(testDir)
			So(err, ShouldBeNil)
			So(f.size, ShouldNotEqual, 0)
			testFiles[i] = f
		}
		log.Println("Generated", testFilesCount, "files for", time.Now().Sub(start))
		Convey("Init", func() {
			c := new(FileCache)
			c.dir = testDir
			f := testFiles[mrand.Intn(testFilesCount)]
			r, err := c.Get(f)
			defer r.Close()
			So(err, ShouldBeNil)
			Convey("Delete", func() {
				So(c.Delete(f), ShouldBeNil)
				_, err := c.Get(f)
				So(err, ShouldEqual, ErrFileNotFound)
			})
			Convey("Add", func() {
				f, err := saveRandomFile(testDir)
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
				Convey("Length inconsistency", func() {
					So(c.Delete(f), ShouldBeNil)
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
						So(w.Truncate(f.size), ShouldBeNil)
						So(w.Close(), ShouldBeNil)
						So(c.Check(f), ShouldEqual, ErrFileInconsistent)
					})
					Convey("Delete", func() {
						So(c.Delete(f), ShouldBeNil)
						So(c.Check(f), ShouldEqual, ErrFileNotFound)
					})
				})
			})
		})
	})
}
