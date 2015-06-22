package hath // import "cydev.ru/hath"

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
	actionSettings     = "client_settings"
	actionRemove       = "file_uncache"
	actionSuspend      = "client_suspend"
	actionResume       = "client_resume"
	actionStop         = "client_stop"
	actionMoreFiles    = "more_files"
	actionOverload     = "overload"
	actionFileDownload = "download_list"
	actionFileAdd      = "file_register"
	actionFileRemove   = "file_uncache"
	actionLogin        = "client_login"

	settingStaticRanges = "static_ranges"

	responseOK                       = "OK"
	responseKeyExpired               = "KEY_EXPIRED"
	responseFailConnectTest          = "FAIL_CONNECT_TEST"
	responseFailStartupFlood         = "FAIL_STARTUP_FLOOD"
	responseFailOtherClientConnected = "FAIL_OTHER_CLIENT_CONNECTED"

	maximumTimeLag = 600
	maxRemoveCount = 50

	httpGET           = "GET"
	intBase           = 10
	keyStampDelimiter = "-"
	fileIDDelimiter   = ";"
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
	Host  string
	Debug bool
}

// Client is api for hath rpc
type Client struct {
	cfg        ClientConfig
	httpClient HTTPClient
}

// ErrUnexpected error while processing request/response
type ErrUnexpected struct {
	Err      error
	Response APIResponse
}

// IsUnexpected return true if err is ErrUnexpected or ErrClientUnexpectedResponse
func IsUnexpected(err error) bool {
	if err == ErrClientUnexpectedResponse {
		return true
	}
	_, ok := err.(ErrUnexpected)
	return ok
}

func (e ErrUnexpected) Error() string {
	return fmt.Sprintf("Unexpected error %v: %v", e.Err, e.Response)
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
	u := c.ActionURL(args...)

	if c.cfg.Debug {
		// log request
		start := time.Now()
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
			log.Println("response:", r)
		}()
	}
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
	return ErrUnexpected{Response: r}
}

// Login performs sync check and authentication
func (c Client) Login() error {
	log.Println("client:", "checking sync")
	if err := c.CheckStats(); err != nil {
		return err
	}
	log.Println("client:", "performing authentication")
	r, err := c.getResponse(actionLogin)
	if err != nil || !r.Success {
		return ErrUnexpected{Response: r, Err: err}
	}
	log.Println("client:", "authentication succeeded")
	return nil
}

// func (c Client) getBlacklist(d time.Duration) error {
// 	duration := strconv.FormatInt(int64(d.Seconds()), 10)
// 	r, err := c.getResponse(actionGetBlacklist, duration)
// 	log.Println(r)
// 	return err
// }

// StillAlive sends heartbeat
func (c Client) StillAlive() error {
	return c.notify(actionStillAlive)
}

// Suspend server
func (c Client) Suspend() error {
	return c.notify(actionSuspend)
}

// Resume server
func (c Client) Resume() error {
	return c.notify(actionResume)
}

// Close server
func (c Client) Close() error {
	return c.notify(actionStop)
}

// More files
func (c Client) More() error {
	return c.notify(actionMoreFiles)
}

func (c Client) notify(action string) error {
	r, err := c.getResponse(action)
	if err != nil {
		return err
	}
	if !r.Success {
		return ErrUnexpected{Response: r}
	}
	return nil
}

// ParseVars parses k=v map from r.Data
func (r APIResponse) ParseVars() Vars {
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

// Vars represents k-v map from APIResponse.Data
type Vars map[string]string

// Get string
func (v Vars) Get(k string) string {
	return v[k]
}

// GetInt parses int
func (v Vars) GetInt(k string) (int, error) {
	return strconv.Atoi(v.Get(k))
}

// GetInt64 parses int64
func (v Vars) GetInt64(k string) (int64, error) {
	return strconv.ParseInt(v.Get(k), intBase, 64)
}

// GetUint64 parses uint64
func (v Vars) GetUint64(k string) (uint64, error) {
	return strconv.ParseUint(v.Get(k), intBase, 64)
}

// GetStaticRange parses static range list
func (v Vars) GetStaticRange(k string) (s StaticRanges, err error) {
	elems := strings.Split(v.Get(k), staticRangeDelimiter)
	s = make(StaticRanges)
	for _, elem := range elems {
		if len(elem) == 0 {
			continue
		}
		r, err := ParseStaticRange(elem)
		if err != nil {
			return s, err
		}
		s.Add(r)
	}
	return s, err
}

// GetProxyMode parses ProxyMode
func (v Vars) GetProxyMode(k string) (p ProxyMode, err error) {
	modeInt, err := strconv.ParseInt(v.Get(k), intBase, proxyModeBits)
	if err != nil {
		return p, err
	}
	// check overflow of ProxyMode
	if modeInt < proxyModeMin || modeInt > proxyModeMax {
		r := APIResponse{Message: v.Get(k)}
		err = ErrUnexpected{Err: errors.New("ProxyMode overflow"), Response: r}
		return p, err
	}
	return ProxyMode(modeInt), nil
}

// CheckStats checks time desync and minumum client build
// returns nil, of time is synced and client version is up to date
func (c Client) CheckStats() error {
	r, err := c.getResponse(actionStatistics)
	if err != nil {
		return err
	}
	vars := r.ParseVars()
	serverTime, err := vars.GetInt64(statTime)
	if err != nil {
		return ErrUnexpected{Response: r, Err: err}
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
	serverMinBuild, err := vars.GetInt(statMinBuild)
	if err != nil {
		return ErrUnexpected{Response: r, Err: err}
	}
	if serverMinBuild > clientBuild {
		return ErrClientVersionOld
	}
	return nil
}

// Settings of hath client
type Settings struct {
	RPCServers            []net.IP
	ImageServer           string
	RequestServer         string
	LowMemory             bool
	ProxyMode             ProxyMode
	StaticRanges          StaticRanges
	Name                  string
	Host                  net.IP
	Port                  int
	MaximumBytesPerSecond int64
	MaximumCacheSize      int64
	DiskReamainingBytes   int64
}

// IsRPCServer returns true, if request is sent from hath rpc server
func (s Settings) IsRPCServer(r *http.Request) bool {
	remoteIP, err := FromRequest(r)
	if err != nil {
		return false
	}
	for _, ip := range s.RPCServers {
		if ip.Equal(remoteIP) {
			return true
		}
	}
	return false
}

// Settings from server
func (c Client) Settings() (cfg Settings, err error) {
	r, err := c.getResponse(actionSettings)
	if err != nil {
		return cfg, err
	}
	if !r.Success {
		return cfg, ErrUnexpected{Response: r}
	}
	vars := r.ParseVars()
	cfg.StaticRanges, err = vars.GetStaticRange(settingStaticRanges)
	if err != nil {
		return cfg, err
	}
	cfg.Port, err = vars.GetInt("port")
	if err != nil {
		return cfg, err
	}
	cfg.Host = net.ParseIP(vars.Get("host"))

	cfg.MaximumBytesPerSecond, err = vars.GetInt64("throttle_bytes")
	if err != nil {
		return cfg, err
	}

	cfg.MaximumCacheSize, err = vars.GetInt64("disklimit_bytes")
	if err != nil {
		return cfg, err
	}

	cfg.Name = vars.Get("name")
	cfg.ProxyMode, err = vars.GetProxyMode("request_proxy_mode")

	cfg.RequestServer = vars.Get("request_server")
	cfg.ImageServer = vars.Get("image_server")

	// parsing rpc servers
	servers := strings.Split(vars.Get("rpc_server_ip"), ";")
	for _, server := range servers {
		ip := net.ParseIP(server)
		if ip == nil {
			return cfg, io.ErrUnexpectedEOF
		}
		cfg.RPCServers = append(cfg.RPCServers, ip)
	}
	log.Println("got settings:")
	log.Println("\tstatic ranges:", cfg.StaticRanges)
	log.Println("\tproxy mode:", cfg.ProxyMode)
	log.Println("\taddr:", cfg.Host, cfg.Port)

	if c.cfg.Debug {
		fmt.Printf("%+v\n", cfg)
	}
	return cfg, err
}

// GetFile returns io.ReadCloser for given url
func (c Client) GetFile(u *url.URL) (rc io.ReadCloser, err error) {
	res, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, ErrUnexpected{Err: errors.New("Unexpected status")}
	}
	log.Println("client:", "downloading from", res.Request.URL.Host)
	return res.Body, nil
}

// RemoveFiles notifies api server of removed files
func (c Client) RemoveFiles(files []File) error {
	count := len(files)
	if count > maxRemoveCount {
		// removing files in batches of maxRemoveCount
		var index int
		for index = 0; index < count; index += maxRemoveCount {
			if err := c.RemoveFiles(files[index : index+maxRemoveCount]); err != nil {
				return err
			}
		}
		// removing reamaning files
		files = files[index:]
	}
	idList := make([]string, len(files))
	for i, f := range files {
		idList[i] = f.String()
	}
	arg := strings.Join(idList, fileIDDelimiter)
	r, err := c.getResponse(actionRemove, arg)
	if err != nil {
		return err
	}
	if !r.Success {
		return ErrUnexpected{Response: r, Err: errors.New("not succeed")}
	}
	return nil
}

// AddFiles notifies api server of registered files
func (c Client) AddFiles(files []File) error {
	count := len(files)
	if count > maxRemoveCount {
		// adding files in batches of maxRemoveCount
		var index int
		for index = 0; index < count; index += maxRemoveCount {
			if err := c.AddFiles(files[index : index+maxRemoveCount]); err != nil {
				return err
			}
		}
		// adding reamaning files
		files = files[index:]
	}
	idList := make([]string, len(files))
	for i, f := range files {
		idList[i] = f.String()
	}
	arg := strings.Join(idList, fileIDDelimiter)
	r, err := c.getResponse(actionFileAdd, arg)
	if err != nil {
		return err
	}
	if !r.Success {
		return ErrUnexpected{Response: r, Err: errors.New("not succeed")}
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
