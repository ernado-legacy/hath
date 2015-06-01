package main

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	clientVersion = "1.2.25"
	clientBuild   = 96

	clientAPIHost   = "rpc.hentaiathome.net"
	clientAPIScheme = "http"
	clientAPIPath   = "clientapi.php"

	argClientBuild    = "clientbuild"
	argAction         = "act"
	argClientID       = "cid"
	argActionArgument = "add"
	argActionKey      = "actkey"
	argTime           = "acttime"

	actionKeyStart     = "hentai@home"
	actionKeyDelimiter = "-"
	actionStart        = "client_start"
	actionGetBlacklist = "get_blacklist"
	actionStillAlive   = "still_alive"

	responseOK                       = "OK"
	responseKeyExpired               = "KEY_EXPIRED"
	responseFailConnectTest          = "FAIL_CONNECT_TEST"
	responseFailStartupFlood         = "FAIL_STARTUP_FLOOD"
	responseFailOtherClientConnected = "FAIL_OTHER_CLIENT_CONNECTED"

	httpGET           = "GET"
	intBase           = 10
	keyStampDelimiter = "-"
)

var (
	// ErrClientKeyExpired timestampt drift occured
	ErrClientKeyExpired = errors.New("Client key is expired")
	// ErrClientFailedConnectionTest client failed to response on test correctly
	ErrClientFailedConnectionTest = errors.New("Client failed connection test")
	// ErrClientStartupFlood api rpc server flood protection is enabled
	// client should wait
	ErrClientStartupFlood = errors.New("API flood protection enabled")
	// ErrClientOtherConnected other client with same clientID connected
	ErrClientOtherConnected = errors.New("Other client is connected")
	// ErrClientUnexpectedResponse unexpected/unhandler error
	ErrClientUnexpectedResponse = errors.New("Unexpected error")
)

// APIResponse represents response from rpc api
type APIResponse struct {
	Success bool
	Message string
	Data    []string
}

// HTTPClient is underlying http client
type HTTPClient interface {
	Get(url string) (*http.Response, error)
}

// Client is api for hath rpc
type Client struct {
	id         int64
	key        string
	httpClient HTTPClient
}

func sInt64(i int64) string {
	return strconv.FormatInt(i, intBase)
}

func (c Client) getURL(args ...string) *url.URL {
	// preparing time and arguments
	if len(args) == 0 {
		panic("bad arguments lenght in getServerAPIURL")
	}
	var (
		action   = args[0]
		argument = ""
		sID      = sInt64(c.id)
		sTime    = sInt64(time.Now().Unix())
		sBuild   = sInt64(clientBuild)
	)

	if len(args) > 1 {
		argument = args[1]
	}

	// generating sha1 hash for action key
	toHash := strings.Join([]string{actionKeyStart,
		action,
		argument,
		sID,
		sTime,
		c.key},
		actionKeyDelimiter,
	)
	h := sha1.New()
	fmt.Fprint(h, toHash)
	actionKey := fmt.Sprintf("%x", h.Sum(nil))

	// building url
	u := &url.URL{Scheme: clientAPIScheme, Path: clientAPIPath, Host: clientAPIHost}
	values := make(url.Values)
	values.Add(argClientBuild, sBuild)
	values.Add(argAction, action)
	values.Add(argActionArgument, argument)
	values.Add(argActionKey, actionKey)
	values.Add(argTime, sTime)
	values.Add(argClientID, sID)
	u.RawQuery = values.Encode()
	return u
}

// ActionURL - get url for action
func (c Client) ActionURL(args ...string) *url.URL {
	return c.getURL(args...)
}

func (c Client) getResponse(args ...string) (r APIResponse, err error) {
	start := time.Now()
	u := c.ActionURL(args...)

	// log request
	defer func() {
		end := time.Now().Sub(start)
		status := "OK"
		if err != nil {
			status = err.Error()
		}
		if !r.Success {
			status = "ERR"
		}
		log.Println(httpGET, u, end, status)
	}()

	// perform request
	res, err := c.httpClient.Get(u.String())
	if err != nil {
		return r, err
	}
	defer res.Body.Close()

	// read response
	scanner := bufio.NewScanner(res.Body)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err = scanner.Err(); err != nil {
		return r, err
	}
	if len(lines) == 0 {
		return r, err
	}
	r.Message = lines[0]
	if len(lines) > 1 {
		r.Data = lines[1:]
	}
	if r.Message == responseOK {
		r.Success = true
	}
	return r, err
}

// Start starts api client
func (c Client) Start() error {
	r, err := c.getResponse(actionStart)
	if err != nil {
		return err
	}
	if r.Success {
		return nil
	}
	if strings.HasPrefix(r.Message, responseFailConnectTest) {
		return ErrClientFailedConnectionTest
	}
	if strings.HasPrefix(r.Message, responseFailStartupFlood) {
		return ErrClientStartupFlood
	}
	if strings.HasPrefix(r.Message, responseKeyExpired) {
		return ErrClientKeyExpired
	}
	if strings.HasPrefix(r.Message, responseFailOtherClientConnected) {
		return ErrClientOtherConnected
	}
	return ErrClientUnexpectedResponse
}

func (c Client) getBlacklist(d time.Duration) error {
	duration := strconv.FormatInt(int64(d.Seconds()), 10)
	r, err := c.getResponse(actionGetBlacklist, duration)
	log.Println(r)
	return err
}

// StillAlive sends heartbeat
func (c Client) StillAlive() error {
	r, err := c.getResponse(actionStillAlive)
	if err != nil {
		return err
	}
	if r.Success {
		return nil
	}
	return ErrClientUnexpectedResponse
}

// NewClient creates new client for api
func NewClient(id int64, key string) *Client {
	c := new(Client)
	c.id = id
	c.key = key
	c.httpClient = http.DefaultClient
	return c
}
