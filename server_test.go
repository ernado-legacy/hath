package hath // import "cydev.ru/hath"

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/smartystreets/goconvey/convey"
)

func TestServer(t *testing.T) {
	Convey("Server", t, func() {
		var (
			clientID  int64 = 1345
			clientKey       = "12345"
		)
		testDir, err := ioutil.TempDir(os.TempDir(), randDirPrefix)
		So(err, ShouldBeNil)
		cache := new(FileCache)
		cache.dir = testDir
		frontend := NewDirectFrontend(cache)
		credentials := Credentials{ClientID: clientID, Key: clientKey}
		cfg := ServerConfig{}
		cfg.Credentials = credentials
		cfg.Frontend = frontend
		db, err := NewDB(path.Join(testDir, "bolt.db"))
		defer db.Close()
		So(err, ShouldBeNil)
		cfg.DataBase = db

		defer os.RemoveAll(testDir)
		g := FileGenerator{
			SizeMax:       randFileSizeMax,
			SizeMin:       randFileSizeMin,
			ResolutionMax: randFileResolutionMax,
			ResolutionMin: randFileResolutionMin,
			Dir:           testDir,
		}
		server := NewServer(cfg)
		server.Start()
		defer server.Close()
		gin.SetMode(gin.TestMode)
		So(server, ShouldNotBeNil)
		s := httptest.NewServer(server)
		defer s.Close()

		So(s, ShouldNotBeNil)
		// host := s.Config.Addr
		u, err := url.Parse(s.URL)
		So(err, ShouldBeNil)
		Convey("GET", func() {
			f, err := g.New()
			So(err, ShouldBeNil)
			So(frontend.Check(f), ShouldBeNil)
			// saving to db
			So(db.Add(f), ShouldBeNil)

			// generating link
			ts := time.Now().Unix()
			ks := f.KeyStamp(credentials.Key, ts)
			args := make(Args)
			args[argsKeystamp] = fmt.Sprintf("%d-%s", ts, ks)
			uPath := fmt.Sprintf("/h/%s/%s/test.png", f, args)
			link := &url.URL{Host: u.Host, Scheme: "http", Path: uPath}

			// making request
			res, err := http.Get(link.String())
			So(err, ShouldBeNil)
			defer res.Body.Close()
			So(res.StatusCode, ShouldEqual, http.StatusOK)

			hash := sha1.New()
			_, err = io.CopyN(hash, res.Body, f.Size)
			So(err, ShouldBeNil)
			So(bytes.Equal(hash.Sum(nil), f.ByteID()), ShouldBeTrue)
			Convey("Content type should be ok", func() {
				ct := res.Header.Get(headerContentType)
				So(ct, ShouldEqual, f.ContentType())
			})
		})
		Convey("GET with no db consistency", func() {
			f, err := g.New()
			So(err, ShouldBeNil)
			So(frontend.Check(f), ShouldBeNil)

			Convey("DB must be inconsistent", func() {
				_, err := db.Get(f.ByteID())
				So(err, ShouldEqual, ErrFileNotFound)
			})

			// generating link
			ts := time.Now().Unix()
			ks := f.KeyStamp(credentials.Key, ts)
			args := make(Args)
			args[argsKeystamp] = fmt.Sprintf("%d-%s", ts, ks)
			uPath := fmt.Sprintf("/h/%s/%s/test.png", f, args)
			link := &url.URL{Host: u.Host, Scheme: "http", Path: uPath}

			// making request
			res, err := http.Get(link.String())
			So(err, ShouldBeNil)
			defer res.Body.Close()
			So(res.StatusCode, ShouldEqual, http.StatusOK)

			hash := sha1.New()
			_, err = io.CopyN(hash, res.Body, f.Size)
			So(err, ShouldBeNil)
			So(bytes.Equal(hash.Sum(nil), f.ByteID()), ShouldBeTrue)
			// Convey("DB must be consistent now", func() {
			// 	time.Sleep(time.Millisecond * 5)
			// 	f2, err := db.Get(f.ByteID())
			// 	So(err, ShouldBeNil)
			// 	So(f2.String(), ShouldEqual, f.String())
			// })
		})
	})
}

func TestParseArgs(t *testing.T) {
	Convey("Arguments parsing", t, func() {
		input := "arg1=val1;arg2=val2;arg3=val3;arg4=4;arg5=1035567"
		args := ParseArgs(input)
		So(args["arg1"], ShouldEqual, "val1")
		So(args["arg2"], ShouldEqual, "val2")
		So(args["arg3"], ShouldEqual, "val3")
		So(args["arg4"], ShouldEqual, "4")
		So(args["arg5"], ShouldEqual, "1035567")
		Convey("Back parse", func() {
			input := args.String()
			args := ParseArgs(input)
			So(args["arg1"], ShouldEqual, "val1")
			So(args["arg2"], ShouldEqual, "val2")
			So(args["arg3"], ShouldEqual, "val3")
			So(args["arg4"], ShouldEqual, "4")
			So(args["arg5"], ShouldEqual, "1035567")
		})
		Convey("Integer", func() {
			So(args.GetInt("arg4"), ShouldEqual, 4)
		})
		Convey("String", func() {
			So(args.Get("arg1"), ShouldEqual, "val1")
		})
		Convey("Int64", func() {
			So(args.GetInt64("arg5"), ShouldEqual, 1035567)
		})
		Convey("With terminating delimiter", func() {
			input := "arg1=val1;arg2=val2;arg3=val3;arg4=4;"
			args := ParseArgs(input)
			So(args["arg1"], ShouldEqual, "val1")
			So(args["arg2"], ShouldEqual, "val2")
			So(args["arg3"], ShouldEqual, "val3")
			So(args["arg4"], ShouldEqual, "4")
		})
	})
}
