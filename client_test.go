package hath // import "cydev.ru/hath"

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type testClient struct {
	request  *http.Request
	response *http.Response
	err      error
}

func (t testClient) Get(url string) (r *http.Response, err error) {
	t.request, err = http.NewRequest(httpGET, url, nil)
	if err != nil {
		return nil, err
	}
	return t.response, t.err
}

func TestClientURL(t *testing.T) {
	var (
		cid int64 = 666
		key       = "123fsdyfh12344AFc"
	)
	cfg := ClientConfig{Credentials: Credentials{cid, key}}
	c := NewClient(cfg)
	Convey("URL", t, func() {
		Convey("Panic", func() {
			So(func() {
				c.getURL()
			}, ShouldPanic)
		})
		Convey("OK", func() {
			action := "action2"
			arg := "add2"
			u := c.getURL(action, arg)
			So(u, ShouldNotBeNil)
			query := u.Query()
			So(query.Get("cid"), ShouldEqual, "666")
			So(query.Get("add"), ShouldEqual, arg)
			So(query.Get("act"), ShouldEqual, action)
			So(query.Get("clientbuild"), ShouldEqual, strconv.FormatInt(clientBuild, 10))
		})
	})
}

func TestParseVars(t *testing.T) {
	Convey("Parsing vars", t, func() {
		res := APIResponse{}
		res.Data = append(res.Data, "s= pek ")
		res.Data = append(res.Data, "  int=1123")
		res.Data = append(res.Data, "int64=  75565  ")
		res.Data = append(res.Data, "uint64=6675565")
		res.Data = append(res.Data, "  int64-2=75565=????!!11")
		res.Data = append(res.Data, "ranges   = aaaa;bbbb;cccc;ffff;")
		res.Data = append(res.Data, "badranges=aaaa;bbbb;cccc;fockyo;")
		vars := res.ParseVars()
		So(len(vars), ShouldEqual, 6)
		Convey("String", func() {
			So(vars.Get("s"), ShouldEqual, "pek")
		})
		Convey("Int", func() {
			v, err := vars.GetInt("int")
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 1123)
		})
		Convey("Int64", func() {
			v, err := vars.GetInt64("int64")
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 75565)
		})
		Convey("UInt64", func() {
			v, err := vars.GetUint64("uint64")
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 6675565)
		})
		Convey("Bad data", func() {
			So(vars.Get("int64-2"), ShouldEqual, "")
		})
		Convey("Static ranges", func() {
			So(vars.Get("ranges"), ShouldEqual, "aaaa;bbbb;cccc;ffff;")
			ranges, err := vars.GetStaticRange("ranges")
			So(err, ShouldBeNil)
			So(ranges.String(), ShouldEqual, "aaaa;bbbb;cccc;ffff")
			So(ranges[StaticRange{0xaa, 0xaa}], ShouldBeTrue)
			So(ranges[StaticRange{0xbb, 0xbb}], ShouldBeTrue)
			So(ranges[StaticRange{0xcc, 0xcc}], ShouldBeTrue)
			So(ranges[StaticRange{0xff, 0xff}], ShouldBeTrue)
			So(ranges[StaticRange{0xee, 0xee}], ShouldBeFalse)
			Convey("Bad static ranges", func() {
				_, err := vars.GetStaticRange("badranges")
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestClientRequest(t *testing.T) {
	var (
		cid int64 = 666
		key       = "123fsdyfh12344AFc"
		tc  *testClient
	)
	cfg := ClientConfig{Credentials: Credentials{cid, key}}
	c := NewClient(cfg)
	tc = new(testClient)
	c.httpClient = tc
	Convey("Request", t, func() {
		Convey("Simple", func() {
			responce := new(http.Response)
			body := "OK\nTestTest\nTtt"
			responce.StatusCode = http.StatusOK
			responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
			c.httpClient = testClient{nil, responce, nil}
			r, err := c.getResponse("action")
			So(err, ShouldBeNil)
			So(r.Success, ShouldBeTrue)
		})
		Convey("Blank", func() {
			responce := new(http.Response)
			body := ""
			responce.StatusCode = http.StatusOK
			responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
			c.httpClient = testClient{nil, responce, nil}
			r, err := c.getResponse("action")
			So(err, ShouldBeNil)
			So(r.Success, ShouldBeFalse)
		})
		Convey("Http error", func() {
			responce := new(http.Response)
			body := "Not at all"
			responce.StatusCode = http.StatusOK
			responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
			c.httpClient = testClient{nil, responce, errors.New("test")}
			r, err := c.getResponse("action")
			So(err, ShouldNotBeNil)
			So(r.Success, ShouldBeFalse)
		})
		Convey("Actions", func() {
			Convey("Still alive", func() {
				Convey("OK", func() {
					responce := new(http.Response)
					body := "OK\nWelcome, master"
					responce.StatusCode = http.StatusOK
					responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
					c.httpClient = testClient{nil, responce, nil}
					err := c.StillAlive()
					So(err, ShouldBeNil)
				})
				Convey("Errors", func() {
					Convey("HTTP", func() {
						responce := new(http.Response)
						body := "OK\nWelcome, master"
						responce.StatusCode = http.StatusOK
						responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
						c.httpClient = testClient{nil, responce, errors.New("test")}
						err := c.StillAlive()
						So(err, ShouldNotBeNil)
					})
					Convey("Not OK", func() {
						responce := new(http.Response)
						body := "FAILED\nWelcome, master"
						responce.StatusCode = http.StatusOK
						responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
						c.httpClient = testClient{nil, responce, nil}
						err := c.StillAlive()
						So(IsUnexpected(err), ShouldBeTrue)
					})
				})
			})
			Convey("Start", func() {
				Convey("OK", func() {
					responce := new(http.Response)
					body := "OK\nWelcome, master"
					responce.StatusCode = http.StatusOK
					responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
					c.httpClient = testClient{nil, responce, nil}
					err := c.Start()
					So(err, ShouldBeNil)
				})
				Convey("Errors", func() {
					Convey("HTTP", func() {
						responce := new(http.Response)
						body := "OK\nWelcome, master"
						responce.StatusCode = http.StatusOK
						responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
						c.httpClient = testClient{nil, responce, errors.New("test")}
						err := c.Start()
						So(err, ShouldNotBeNil)
					})
					Convey("Not OK", func() {
						responce := new(http.Response)
						body := "FAILED\nWelcome, master"
						responce.StatusCode = http.StatusOK
						responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
						c.httpClient = testClient{nil, responce, nil}
						err := c.Start()
						So(err, ShouldNotBeNil)
					})
					Convey("Other", func() {
						getError := func(body string) error {
							responce := new(http.Response)
							responce.StatusCode = http.StatusOK
							responce.Body = ioutil.NopCloser(bytes.NewBufferString(body))
							c.httpClient = testClient{nil, responce, nil}
							return c.Start()
						}
						So(getError("FAIL_CONNECT_TEST"), ShouldEqual, ErrClientFailedConnectionTest)
						So(getError("FAIL_STARTUP_FLOOD"), ShouldEqual, ErrClientStartupFlood)
						So(getError("FAIL_OTHER_CLIENT_CONNECTED"), ShouldEqual, ErrClientOtherConnected)
						So(IsUnexpected(getError("FAIL_WTF")), ShouldBeTrue)
						So(getError("KEY_EXPIRED"), ShouldEqual, ErrClientKeyExpired)
					})
				})
			})
		})
	})
}
