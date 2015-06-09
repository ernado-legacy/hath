package hath // import "cydev.ru/hath"

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDBInit(t *testing.T) {
	Convey("DB", t, func() {
		tmpDB, err := ioutil.TempFile(os.TempDir(), "db")
		So(err, ShouldBeNil)
		tmpDB.Close()
		defer os.Remove(tmpDB.Name())
		db, err := NewDB(tmpDB.Name())
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)
		Convey("Insert", func() {
			rec := FileRecord{ID: "070b45ae488fb1967aaf618561a7d6ba4d28a1c9", LastUsage: time.Now(), Size: 16551}
			err := db.add(rec)
			So(err, ShouldBeNil)
			Convey("Get", func() {
				f, err := db.get(rec.ByteID())
				So(err, ShouldBeNil)
				So(f.LastUsage.Unix(), ShouldEqual, rec.LastUsage.Unix())
			})
			Convey("Get 404", func() {
				rec := FileRecord{ID: "070b45ae488fb1967bbf618561a7d6ba4d28a1c9", LastUsage: time.Now(), Size: 16551}
				_, err := db.get(rec.ByteID())
				So(err, ShouldEqual, ErrFileNotFound)
			})
		})
	})
}
