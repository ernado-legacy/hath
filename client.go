package hath // import "cydev.ru/hath"

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

	statTime     = "server_time"
	statMinBuild = "min_client_build"

	actionKeyStart     = "hentai@home"
	actionKeyDelimiter = "-"
	actionStart        = "client_start"
	actionGetBlacklist = "get_blacklist"
	actionStillAlive   = "still_alive"
	actionStatistics   = "server_stat"

	responseOK                       = "OK"
	responseKeyExpired               = "KEY_EXPIRED"
	responseFailConnectTest          = "FAIL_CONNECT_TEST"
	responseFailStartupFlood         = "FAIL_STARTUP_FLOOD"
	responseFailOtherClientConnected = "FAIL_OTHER_CLIENT_CONNECTED"

	maximumTimeLag = 10

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
	// ErrTimeDesync timestamp delta too bit
	ErrTimeDesync = errors.New("Time on server and on client differ too much")
	// ErrClientVersionOld api outdated
	ErrClientVersionOld = errors.New("Client version is too old")
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

// Credentials of hath client
type Credentials struct {
	ClientID int64
	Key      string
}

// ClientConfig is configuration for client
type ClientConfig struct {
	Credentials
	Host string
}

// Client is api for hath rpc
type Client struct {
	cfg        ClientConfig
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
		sID      = sInt64(c.cfg.ClientID)
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
		c.cfg.Key},
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
	// start := time.Now()
	u := c.ActionURL(args...)

	// log request
	// defer func() {
	// 	end := time.Now().Sub(start)
	// 	status := "OK"
	// 	if err != nil {
	// 		status = err.Error()
	// 	}
	// 	if !r.Success {
	// 		status = "ERR"
	// 	}
	// 	log.Println(httpGET, u, end, status)
	// }()

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

// func (c Client) getBlacklist(d time.Duration) error {
// 	duration := strconv.FormatInt(int64(d.Seconds()), 10)
// 	r, err := c.getResponse(actionGetBlacklist, duration)
// 	log.Println(r)
// 	return err
// }

// StillAlive sends heartbeat
func (c Client) StillAlive() error {
	r, err := c.getResponse(actionStillAlive)
	if err != nil {
		return err
	}
	if !r.Success {
		return ErrClientUnexpectedResponse
	}
	return nil
}

// ParseVars parses k=v map from r.Data
func (r APIResponse) ParseVars() map[string]string {
	data := make(map[string]string)
	for _, d := range r.Data {
		d = strings.TrimSpace(d)
		vars := strings.Split(d, "=")
		if len(vars) != 2 {
			continue
		}
		k := strings.TrimSpace(vars[0])
		v := strings.TrimSpace(vars[1])
		data[k] = v
	}
	return data
}

// CheckStats checks time desync and minumum client build
func (c Client) CheckStats() error {
	r, err := c.getResponse(actionStatistics)
	if err != nil {
		return err
	}
	vars := r.ParseVars()
	serverTime, err := strconv.ParseInt(vars[statTime], intBase, 64)
	if err != nil {
		return ErrClientUnexpectedResponse
	}
	delta := time.Now().Unix() - serverTime
	if delta < 0 {
		delta *= -1
	}
	if delta > maximumTimeLag {
		return ErrTimeDesync
	}
	if delta == 0 {
		log.Println("your time is perfectly synced")
	}
	serverMinBuild, err := strconv.ParseInt(vars[statMinBuild], intBase, 64)
	if err != nil {
		return ErrClientUnexpectedResponse
	}
	if serverMinBuild > clientBuild {
		return ErrClientVersionOld
	}
	return nil
}

// NewClient creates new client for api
func NewClient(cfg ClientConfig) *Client {
	c := new(Client)
	if len(cfg.Host) == 0 {
		cfg.Host = clientAPIHost
	}
	c.cfg = cfg
	c.httpClient = http.DefaultClient
	return c
}
