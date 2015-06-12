package hath // import "cydev.ru/hath"

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDBCollect(t *testing.T) {
	Convey("DB collect", t, func() {
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
		var files []File
		count := 50
		deadline := time.Now().Add(-4 * time.Second)
		lastUsage := time.Now().Add(-time.Second)
		for i := 0; i < count; i++ {
			f := g.NewFake()
			f.LastUsage = lastUsage.Unix()
			files = append(files, f)
		}
		lastUsage = time.Now().Add(-5 * time.Second)
		for i := 0; i < count; i++ {
			f := g.NewFake()
			f.LastUsage = lastUsage.Unix()
			files = append(files, f)
		}
		Convey("Insert", func() {
			So(db.AddBatch(files), ShouldBeNil)
			Convey("Count", func() {
				n, err := db.GetOldFilesCount(deadline)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, count)
			})
			files, err := db.GetOldFiles(count*2, deadline)
			So(err, ShouldBeNil)
			So(len(files), ShouldEqual, count)
		})
	})
}

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
		Convey("Implements DataBase", func() {
			dbInterface := reflect.TypeOf((*DataBase)(nil)).Elem()
			So(reflect.TypeOf(db).Implements(dbInterface), ShouldBeTrue)
		})
		Convey("Insert", func() {
			rec := g.NewFake()
			err := db.Add(rec)
			So(err, ShouldBeNil)
			Convey("Get", func() {
				f, err := db.Get(rec.ByteID())
				So(err, ShouldBeNil)
				So(f.LastUsage, ShouldEqual, rec.LastUsage)
				So(f.Type, ShouldEqual, rec.Type)
				So(f.HexID(), ShouldEqual, rec.HexID())
				So(f.String(), ShouldEqual, rec.String())
			})
			Convey("Get 404", func() {
				rec := g.NewFake()
				_, err := db.Get(rec.ByteID())
				So(err, ShouldEqual, ErrFileNotFound)
			})
		})
	})
}
