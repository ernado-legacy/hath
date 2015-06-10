package hath // import "cydev.ru/hath"

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDBInit(t *testing.T) {
	Convey("DB", t, func() {
		g := FileGenerator{
			SizeMax:       randFileSizeMax,
			SizeMin:       randFileSizeMin,
			ResolutionMax: randFileResolutionMax,
			ResolutionMin: randFileResolutionMin,
		}
		tmpDB, err := ioutil.TempFile(os.TempDir(), "db")
		So(err, ShouldBeNil)
		tmpDB.Close()
		defer os.Remove(tmpDB.Name())
		db, err := NewDB(tmpDB.Name())
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		Convey("Insert", func() {
			rec := g.NewFake()
			err := db.add(rec)
			So(err, ShouldBeNil)
			Convey("Get", func() {
				f, err := db.get(rec.ByteID())
				So(err, ShouldBeNil)
				So(f.LastUsage, ShouldEqual, rec.LastUsage)
				So(f.Type, ShouldEqual, rec.Type)
				So(f.HexID(), ShouldEqual, rec.HexID())
				So(f.String(), ShouldEqual, rec.String())
			})
			Convey("Get 404", func() {
				rec := g.NewFake()
				_, err := db.get(rec.ByteID())
				So(err, ShouldEqual, ErrFileNotFound)
			})
		})
	})
}
