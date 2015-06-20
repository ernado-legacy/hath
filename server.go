// Package hath is Hentai@Home client implementation in golang
package hath // import "cydev.ru/hath"

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Server should handle requests from users (and rpc?)
// speedtests
// server commands:
//     /servercmd/<command>/<additional:kwds>/<timestamp:int>/<key>'
// image file request:
//     /h/<fileid>/<additional:kwds>/<filename>
type Server interface {
	http.Handler
}

type speedTest struct{}

func (s speedTest) Read(b []byte) (n int, err error) {
	n = len(b)
	return n, err
}

// DefaultServer uses hard drive to respond
type DefaultServer struct {
	api           *Client
	cfg           ServerConfig
	frontend      Frontend
	db            DataBase
	e             *gin.Engine
	useQuery      chan File
	wg            *sync.WaitGroup
	started       bool
	commands      map[string]commandHandler
	stop          chan bool
	updateLock    sync.Locker
	localNetworks []net.IPNet
}

const (
	argsDelimiter       = ";"
	argsEqual           = "="
	argsKeystamp        = "keystamp"
	timestampMaxDelta   = 60
	useQuerySize        = 100
	tmpFolder           = "tmp"
	cmdKeyStart         = "hentai@home-servercmd"
	cmdKeyDelimiter     = "-"
	size100MB           = 10 * size10MB
	headerContentLength = "Content-Length"
	fileInfoDelimiter   = ":"

	downloadError     = "FAIL"
	downloadSuccess   = "OK"
	downloadInvalid   = "INVALID"
	downloadPath      = "image.php"
	argDownloadFileID = "f"
	argDownloadKey    = "t"
	downloadScheme    = "http"

	cmdSpeedTest = "speed_test"
	cmdDownload  = "cache_files"
	cmdProxyTest = "proxy_test"

	argIP        = "ipaddr"
	argPort      = "port"
	argFileID    = "fileid"
	argKeystamp  = "keystamp"
	argTestSize  = "testsize"
	argTestCount = "testcount"
	argTestTime  = "testtime"
	argTestKey   = "testkey"
)

// ProxyMode sets proxy security politics
type ProxyMode byte

const (
	// ProxyDisabled no requests allowed
	ProxyDisabled ProxyMode = iota + 1 // starts with 1
	// ProxyLocalNetworksProtected allows requests from local network with passkey
	ProxyLocalNetworksProtected // 2
	// ProxyLocalNetworksOpen allows any requests from local network
	ProxyLocalNetworksOpen // 3
	// ProxyAllNetworksProtected allows requests from any network with passkey (not recommended)
	ProxyAllNetworksProtected // 4
	// ProxyAllNetworksOpen allows any requests from any network (very not recommended)
	ProxyAllNetworksOpen // 5
)

const (
	proxyModeMin  = int64(ProxyDisabled)
	proxyModeMax  = int64(ProxyAllNetworksOpen)
	proxyModeBits = 4
)

func (p ProxyMode) String() string {
	if p == ProxyDisabled {
		return "disabled"
	}
	if p == ProxyLocalNetworksOpen {
		return "open for local networks"
	}
	if p == ProxyAllNetworksProtected {
		return "local networks with passkey"
	}
	if p == ProxyAllNetworksProtected {
		return "all networks with passkey"
	}
	if p == ProxyAllNetworksOpen {
		return "open for all networks"
	}
	return "unknown"
}

// Args represents additional arguments in request string
type Args map[string]string

// Get returns string value
func (a Args) Get(k string) string {
	return a[k]
}

// GetInt parses and returns integer value
func (a Args) GetInt(k string) int {
	i, _ := strconv.Atoi(a[k])
	return i
}

// GetInt64 parses and returns 64bit integer value
func (a Args) GetInt64(k string) int64 {
	i, _ := strconv.ParseInt(a[k], 10, 64)
	return i
}

func (a Args) String() string {
	var elems []string
	for k, v := range a {
		elems = append(elems, strings.Join([]string{k, v}, argsEqual))
	}
	return strings.Join(elems, argsDelimiter)
}

// ParseArgs parses arguments from string
//     arg1=val1;arg2=val2
func ParseArgs(s string) (a Args) {
	a = make(Args)
	components := strings.Split(s, argsDelimiter)
	for _, component := range components {
		kw := strings.Split(component, argsEqual)
		if len(kw) != 2 {
			continue
		}
		a[kw[0]] = kw[1]
	}
	return a
}

func hashStrings(args ...string) string {
	hasher := sha1.New()
	for _, s := range args {
		fmt.Fprint(hasher, s)
	}
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (s *DefaultServer) proxyPasskey(f File) string {
	size := 10
	// sha(id + "I think we can put our differences behind us." + sha(key + "For science.")[:10] + "You monster")[:10]
	keyHash := hashStrings(s.cfg.Key, "For science.")[:size]
	return hashStrings(f.String(), "I think we can put our differences behind us.", keyHash, "You monster.")[:size]
}

// FromRequest extracts the user IP address from req, if present.
func FromRequest(req *http.Request) (net.IP, error) {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
	}

	userIP := net.ParseIP(ip)
	if userIP == nil {
		return nil, fmt.Errorf("userip: %q is not IP:port", req.RemoteAddr)
	}
	return userIP, nil
}

// addFile adds to cache and db, checks sha1
func (s *DefaultServer) addFile(f File, r io.Reader) error {
	log.Println("server: adding file", f)
	if s.db.Exists(f) {
		log.Println("server:", f, "already exists")
		return nil
	}
	if err := s.frontend.Add(f, r); err != nil {
		log.Println("server: frontend fail:", f, err)
		return err
	}
	if err := s.frontend.Check(f); err != nil {
		log.Println("server: frontend integrity check failed:", f, err)
		if err2 := s.frontend.Remove(f); err2 != nil {
			log.Println("server: fronted failed to remove:", f, err2)
		}
		return err
	}
	if err := s.db.Add(f); err != nil {
		log.Println("server: db fail:", f, err)
		return err
	}
	return nil
}

// handleProxy /p/fileid=asdf;token=asdf;gid=123;page=321;passkey=asdf/filename
func (s *DefaultServer) handleProxy(c *gin.Context) {
	mode := s.cfg.Settings.ProxyMode
	if mode == ProxyDisabled {
		c.String(http.StatusForbidden, "proxy disabled")
		return
	}
	args := ParseArgs(c.Param("kwds"))
	fileID := args.Get("fileid")
	f, err := FileFromID(fileID)
	if err != nil {
		log.Println("proxy:", "bad file id", fileID, err)
		c.String(http.StatusBadRequest, "400: bad file id")
		return
	}
	galleryID := args.GetInt("gid")
	page := args.GetInt("page")
	passkey := args.Get("passkey")
	token := args.Get("token")
	filename := c.Param("filename")

	if s.db.Exists(f) {
		log.Println("proxy:", "file already exists; serving from cache", f)
		s.frontend.Handle(f, c.Writer)
		return
	}

	if mode == ProxyLocalNetworksOpen || mode == ProxyLocalNetworksProtected {
		ip, err := FromRequest(c.Request)
		if err != nil {
			log.Println("proxy:", "unable to extract ip from", c.Request.RemoteAddr)
			c.String(http.StatusInternalServerError, "unable to parse ip")
			return
		}
		var isLocal bool
		for _, net := range s.localNetworks {
			if net.Contains(ip) {
				isLocal = true
				break
			}
		}
		if !isLocal {
			log.Println("proxy:", "access from non-local network by", ip, "<-", c.Request.RemoteAddr)
			c.String(http.StatusForbidden, "bad ip")
			return
		}
	}

	if mode == ProxyLocalNetworksProtected || mode == ProxyAllNetworksProtected {
		// checking passkey
		if passkey != s.proxyPasskey(f) {
			log.Println("proxy:", "bad passkey provided")
			c.String(http.StatusForbidden, "bad passkey")
			return
		}
	}

	u := new(url.URL)
	u.Scheme = downloadScheme
	u.Host = s.cfg.Settings.RequestServer
	u.Path = fmt.Sprintf("/r/%s/%s/%d-%d/%s", fileID, token, galleryID, page, filename)

	var downloadFromHathNetwork bool
	var downloaded bool
	for attempt := 1; attempt <= s.cfg.MaxDownloadAttemps; attempt++ {
		downloadFromHathNetwork = attempt != s.cfg.MaxDownloadAttemps

		// skip download from hath network
		if !downloadFromHathNetwork {
			log.Println("proxy: falling back to direct download")
			q := make(url.Values)
			q.Add("nl", "1") // so fucking obvious
			u.RawQuery = q.Encode()
		}

		log.Println("proxy:", "downloading", f)

		rc, err := s.api.GetFile(u)
		if err != nil {
			log.Println("proxy: download attempt failed", err)
			continue
		}
		defer rc.Close()

		buff := f.Buffer()
		w := io.MultiWriter(buff, c.Writer)
		// proxying data without buffering for speed-up
		c.Writer.Header().Add(headerContentLength, sInt64(f.Size))
		n, err := io.CopyN(w, rc, f.Size)
		if err != nil || n != f.Size {
			log.Println("proxy: failed", err)
			return
		}
		// async saving file to db/frontend
		go func() {
			defer buff.Reset()
			log.Println("proxy:", "saving file to cache/db")
			if err := s.addFile(f, buff); err != nil {
				log.Println("proxy:", "add failed", err)
				return
			}
			log.Println("proxy:", "cached", f)
		}()
		downloaded = true
		break
	}
	if downloaded {
		log.Println("proxy:", "downloaded", f)
	} else {
		log.Println("proxy:", "failed to download", f)
	}
}

// handleImage /h/<fileid>/<additional:kwds>/<filename>
func (s *DefaultServer) handleImage(c *gin.Context) {
	fileID := c.Param("fileid")
	args := ParseArgs(c.Param("kwds"))
	// parsing timestamp and keystamp
	stamps := strings.Split(args.Get(argsKeystamp), keyStampDelimiter)
	if len(stamps) != 2 {
		c.String(http.StatusBadRequest, "400: Bad stamp format")
		return
	}
	timestamp, err := strconv.ParseInt(stamps[0], intBase, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "400: Bad timestamp")
		return
	}
	if !s.isDeltaValid(stamps[0]) {
		c.String(http.StatusBadRequest, "400: timestamp delta is too big")
		return
	}
	keyStamp := stamps[1]
	f, err := FileFromID(fileID)
	if err != nil {
		c.String(http.StatusBadRequest, "400: bad file id")
		return
	}
	expectedKeyStamp := f.KeyStamp(s.cfg.Key, timestamp)
	if expectedKeyStamp != keyStamp && !s.cfg.DontCheckSHA1 {
		c.String(http.StatusForbidden, "403: bad keystamp")
		return
	}
	s.useQuery <- f
	s.frontend.Handle(f, c.Writer)
}

func getSHA1(sep string, args []string) string {
	hasher := sha1.New()
	toHash := strings.Join(args, sep)
	h := hasher.Sum([]byte(toHash))
	return hex.EncodeToString(h)
}

type commandHandler func(*gin.Context, Args)

func (s *DefaultServer) isDeltaValid(ts string) bool {
	if s.cfg.DontCheckTimestamps {
		return true
	}
	timestamp, err := strconv.ParseInt(ts, intBase, 64)
	if err != nil {
		return false
	}
	delta := time.Now().Unix() - timestamp
	if delta < 0 {
		delta *= -1
	}
	return delta < maximumTimeLag
}

var (
	// ErrNoFilesToRemove is flag that there is 0 files to remove
	ErrNoFilesToRemove = errors.New("No more unused files")
)

func (s *DefaultServer) removeUnused(deadline time.Time) error {

	files, err := s.db.GetOldFiles(maxRemoveCount, deadline)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	if err := s.api.RemoveFiles(files); err != nil {
		return err
	}
	if err := s.db.RemoveBatch(files); err != nil {
		return err
	}
	if err := s.frontend.RemoveBatch(files); err != nil {
		return err
	}
	return nil
}

func (s *DefaultServer) removeAllUnused(deadline time.Time) error {
	log.Println("removing old files")
	defer log.Println("removing old files completed")
	for {
		err := s.removeUnused(deadline)
		// just iterating until error occurs
		if err == nil {
			continue
		}
		// error occured
		// unexpected error check
		if err != ErrNoFilesToRemove {
			return err
		}
		// breaking inner loop
		return nil
	}
}

func (s *DefaultServer) removeLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	defer s.wg.Done()
	for {
		select {
		case t := <-ticker.C:
			deadline := t.Add(-1 * s.cfg.RemoveTimeout)
			count, err := s.db.GetOldFilesCount(deadline)
			if err != nil {
				log.Println("error while getting old files count", err)
			}
			if count == 0 {
				continue
			}
			s.updateLock.Lock()
			defer s.updateLock.Unlock()
			if err := s.removeAllUnused(deadline); err != nil {
				log.Println("error while removing files", err)
			}
		case _ = <-s.stop:
			return
		}
	}
}

// /servercmd/<command>/<additional:kwds>/<timestamp:int>/<key>
func (s *DefaultServer) handleCommand(c *gin.Context) {
	// checking remote ip
	if !s.cfg.Settings.IsRPCServer(c.Request) {
		log.Println("server:", "got request from suspicous origin", c.Request.RemoteAddr)
		c.String(http.StatusUnauthorized, "403: not authorised")
		return
	}

	// checking crypto sign
	// parsing params
	key := c.Param("key")
	kwds := c.Param("kwds")
	args := ParseArgs(kwds)
	timestampS := c.Param("timestamp")
	command := c.Param("command")
	sClientID := strconv.FormatInt(s.cfg.ClientID, intBase)

	// checkign timestamp delta
	if !s.isDeltaValid(timestampS) {
		c.String(http.StatusUnauthorized, "403: bad timestamp")
		return
	}

	hashArgs := []string{
		cmdKeyStart,
		kwds,
		sClientID,
		timestampS,
		s.cfg.Key,
	}
	keyCalculated := getSHA1(cmdKeyDelimiter, hashArgs)
	if key != keyCalculated && !s.cfg.DontCheckSHA1 {
		c.String(http.StatusUnauthorized, "403: bad sign")
		return
	}
	handler, ok := s.commands[command]
	if !ok {
		// handler for command not found
		c.String(http.StatusNotFound, "404: command not found")
		return
	}
	handler(c, args)
}

func (s *DefaultServer) commandSpeedTest(c *gin.Context, args Args) {
	size := args.GetInt64(argTestSize)
	if size <= 0 || size > size100MB {
		size = size100MB
	}
	c.Header(headerContentLength, strconv.FormatInt(size, intBase))
	_, err := io.CopyN(c.Writer, speedTest{}, size)
	if err != nil {
		log.Println("speed test error", err)
	}
}

// commandDownload process request from server to download a list of files
// and store them in database and cache
func (s *DefaultServer) commandDownload(c *gin.Context, args Args) {
	for fileInfo, key := range args {
		elems := strings.Split(fileInfo, fileInfoDelimiter)
		if len(elems) != 2 {
			log.Println("warning:", "got bad file info string from server:", fileInfo)
			continue
		}
		fileID := elems[0]
		host := elems[1]

		writeStatus := func(status string) {
			fmt.Fprintf(c.Writer, "%s:%s\n", fileID, status)
		}

		// parsing and validating fileID
		f, err := FileFromID(fileID)
		if err != nil {
			writeStatus(downloadInvalid)
			continue
		}

		// generating url
		u := new(url.URL)
		u.Host = host
		u.Scheme = downloadScheme
		u.Path = downloadPath
		q := make(url.Values)
		q.Add(argDownloadFileID, fileID)
		q.Add(argDownloadKey, key)
		u.RawQuery = q.Encode()

		// downloading and saving file to db/cache
		if err := s.addFromURL(f, u); err != nil {
			writeStatus(downloadError)
			continue
		}
		writeStatus(downloadSuccess)
	}
}

// commandProxyTest processes speed-test requests for other hath clients
// golang implementation differs from original java in way of
// removing unreliable attemps of reducing RTT impact
// with minimum side-effects and simple as fock
func (s *DefaultServer) commandProxyTest(c *gin.Context, args Args) {
	// parsing arguments
	ip := net.ParseIP(args.Get(argIP))
	port := args.GetInt(argPort)
	fileID := args.Get(argFileID)
	keystamp := args.Get(argKeystamp)

	// writeStatus writes fileID:status-duration to response body
	writeStatus := func(status string, t float64) {
		fmt.Fprintf(c.Writer, "%s:%s-%f\n", fileID, status, t)
	}

	f, err := FileFromID(fileID)
	if err != nil || f.Size > FileMaximumSize {
		writeStatus(downloadInvalid, 0)
		return
	}
	// generating url
	u := new(url.URL)
	u.Scheme = downloadScheme
	u.Host = ip.String()
	u.Path = fmt.Sprintf("/h/%s/keystamp=%s/test.jpg", fileID, keystamp)
	log.Printf("proxy: testing %s:%d", ip, port)

	// creating buffer for file
	buff := f.Buffer()

	// starting request
	rc, err := s.api.GetFile(u)
	if err != nil {
		writeStatus(downloadError, 0)
		return
	}
	// on this stage we already got all headers parsed
	// and ready to read body
	// thats how we remove TTFB impact
	defer rc.Close()

	// connected; tcp handshake succeed, headers parsed;
	start := time.Now()
	// downloading to in-memory buffer
	// because we limit maximum size of the file by fairly low (10mb by default)
	// see FileMaximumSize for exact value
	// thats how we remove HDD impact
	n, err := io.CopyN(buff, rc, f.Size)
	if err != nil || n != f.Size {
		log.Println("proxy: failed to download", err)
		writeStatus(downloadError, 0)
		return
	}
	// Measured pure download duration, that does not include
	// time to first byte, header parsing, sha1 checksum calculation,
	// db interactions, possible frontend speed degradation, etc.
	// There is no sense to calculate possible RTT impact here
	duration := time.Now().Sub(start)
	if err := s.addFile(f, buff); err != nil {
		// possible sha1 checksum fail
		log.Println("proxy: failed to add  file", err)
		writeStatus(downloadError, 0)
		return
	}
	writeStatus(downloadSuccess, duration.Seconds())
}

func (s *DefaultServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.e.ServeHTTP(w, r)
}

// lastUsage update loop
func (s *DefaultServer) useLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.UpdateRate)
	defer ticker.Stop()

	var files []File
	update := func() {
		if files == nil {
			return
		}
		s.updateLock.Lock()
		defer s.updateLock.Unlock()
		if err := s.db.UseBatch(files); err != nil {
			log.Println("error while updating lastUsage", err)
		}
		files = nil
	}
	// we need to update last files before stopping server
	defer update()
	for {
		select {
		case f := <-s.useQuery:
			files = append(files, f)
		case <-ticker.C:
			update()
		case <-s.stop:
			return
		}
	}
}

func (s *DefaultServer) addFromURL(f File, u *url.URL) error {
	rc, err := s.api.GetFile(u)
	if err != nil {
		return err
	}
	defer rc.Close()
	return s.addFile(f, rc)
}

// Start server internal goroutines
func (s *DefaultServer) Start() error {
	go s.stopNotificator()

	// starting lastUsage update loop
	s.wg.Add(1)
	go s.useLoop()

	// starting removal loop
	s.wg.Add(1)
	go s.removeLoop()

	s.started = true
	return nil
}

func (s *DefaultServer) stopNotificator() {
	<-s.stop
	log.Println("stopping server")
}

// Close stops server
func (s *DefaultServer) Close() error {
	close(s.useQuery)
	close(s.stop)
	s.wg.Wait()

	s.db.Close()
	s.started = false
	return nil
}

// ServerConfig cfg for server
type ServerConfig struct {
	Credentials
	Frontend            Frontend
	DataBase            DataBase
	Client              *Client
	DontCheckTimestamps bool
	DontCheckSHA1       bool
	RemoveTimeout       time.Duration
	RemoveRate          time.Duration
	UpdateRate          time.Duration
	MaxDownloadAttemps  int
	Settings            Settings
}

// PopulateDefaults of the config
func (cfg *ServerConfig) PopulateDefaults() {
	if cfg.RemoveTimeout == time.Second*0 {
		cfg.RemoveTimeout = time.Hour * 24 * 30
	}
	if cfg.RemoveRate == time.Second*0 {
		cfg.RemoveRate = time.Hour
	}
	if cfg.Client == nil {
		cfg.Client = NewClient(ClientConfig{Credentials: cfg.Credentials})
	}
	if cfg.UpdateRate == time.Second*0 {
		cfg.UpdateRate = time.Second * 5
	}
	if cfg.MaxDownloadAttemps == 0 {
		cfg.MaxDownloadAttemps = 4
	}
}

// NewServer cleares default server with provided client and frontend
func NewServer(cfg ServerConfig) *DefaultServer {
	cfg.PopulateDefaults()
	s := new(DefaultServer)
	s.cfg = cfg
	s.db = cfg.DataBase
	s.frontend = cfg.Frontend

	// routing init
	e := gin.New()
	e.GET("/h/:fileid/:kwds/:filename", s.handleImage)
	e.GET("/servercmd/:command/:kwds/:timestamp/:key", s.handleCommand)
	e.GET("/p/:kwds/:filename", s.handleProxy)
	e.GET("/favicon.ico", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "http://g.e-hentai.org/favicon.ico")
	})
	e.GET("/robots.txt", func(c *gin.Context) {
		c.Writer.Header().Add(headerContentType, "text/plain")
		fmt.Fprintf(c.Writer, "User-agent: *\nDisallow: /")
	})

	// routing for commands
	s.commands = map[string]commandHandler{
		cmdSpeedTest: s.commandSpeedTest,
		cmdDownload:  s.commandDownload,
		cmdProxyTest: s.commandProxyTest,
	}

	if cfg.DontCheckTimestamps {
		log.Println("warning: not checking timestamps")
	}

	if cfg.DontCheckSHA1 {
		log.Println("warning: not checking sha1")
	}

	// local networks list init
	localNetworks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"fc00::/7", // ipv6
	}
	for _, cidr := range localNetworks {
		_, net, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(err)
		}
		s.localNetworks = append(s.localNetworks, *net)
	}

	s.e = e
	s.useQuery = make(chan File, useQuerySize)
	s.wg = new(sync.WaitGroup)
	s.api = cfg.Client
	s.stop = make(chan bool)
	s.updateLock = new(sync.Mutex)
	return s
}
