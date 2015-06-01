package main

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
	c := NewClient(cid, key)
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

func TestClientRequest(t *testing.T) {
	var (
		cid int64 = 666
		key       = "123fsdyfh12344AFc"
		tc  *testClient
	)

	c := NewClient(cid, key)
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
						So(err, ShouldEqual, ErrClientUnexpectedResponse)
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
						So(getError("FAIL_WTF"), ShouldEqual, ErrClientUnexpectedResponse)
						So(getError("KEY_EXPIRED"), ShouldEqual, ErrClientKeyExpired)
					})
				})
			})
		})
	})
}
