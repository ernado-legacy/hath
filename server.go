// Package hath is Hentai@Home client implementation in golang
package hath // import "cydev.ru/hath"

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
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

func (s speedTest) Write(b []byte) (n int, err error) {
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
	registerQuery chan File
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
	stillAliveInterval  = time.Minute * 5

	downloadError     = "FAIL"
	downloadSuccess   = "OK"
	downloadInvalid   = "INVALID"
	downloadPath      = "image.php"
	argDownloadFileID = "f"
	argDownloadKey    = "t"
	downloadScheme    = "http"

	cmdSpeedTest         = "speed_test"
	cmdDownload          = "cache_files"
	cmdProxyTest         = "proxy_test"
	cmdRefreshSettings   = "refresh_settings"
	cmdStillAlive        = "still_alive"
	cmdList              = "cache_list"
	cmdThreadedProxyTest = "threaded_proxy_test"

	argIP        = "ipaddr"
	argPort      = "port"
	argFileID    = "fileid"
	argKeystamp  = "keystamp"
	argTestSize  = "testsize"
	argTestCount = "testcount"
	argTestTime  = "testtime"
	argTestKey   = "testkey"

	onDemandFilename = "ondemand"
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

func (s *DefaultServer) proxy(c *gin.Context, f File, token string, galleryID, page int, filename string) {
	log.Println("proxy:", f)

	u := new(url.URL)
	u.Scheme = downloadScheme
	u.Host = s.cfg.Settings.RequestServer
	u.Path = fmt.Sprintf("/r/%s/%s/%d-%d/%s", f, token, galleryID, page, filename)

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

		rc, err := s.api.RequestFile(f, u)
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
			s.registerQuery <- f
			log.Println("proxy:", "cached", f)
		}()
		downloaded = true
		break
	}
	if downloaded {
		log.Println("proxy:", "downloaded", f)
	} else {
		c.String(http.StatusInternalServerError, "failed to download")
		log.Println("proxy:", "failed to download", f)
	}
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

	s.proxy(c, f, token, galleryID, page, filename)
}

// handleImage /h/<fileid>/<additional:kwds>/<filename>
func (s *DefaultServer) handleImage(c *gin.Context) {
	fileID := c.Param("fileid")
	filename := c.Param("filename")
	log.Println("server:", "serving", fileID)
	args := ParseArgs(c.Param("kwds"))

	ip, err := FromRequest(c.Request)
	if err != nil {
		log.Println("server:", "unable to extract ip from", c.Request.RemoteAddr)
		c.String(http.StatusInternalServerError, "unable to parse ip")
		return
	}

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
	if s.db.Exists(f) {
		s.frontend.Handle(f, c.Writer)
		s.useQuery <- f
		log.Println("server:", "served", f)
		return
	}
	if !s.cfg.Settings.StaticRanges.Contains(f) {
		log.Println("server:", "not found", f)
		c.String(http.StatusNotFound, "404: not found")
		return
	}

	log.Println("server:", "downloading file from static range", f.Range())
	tokens, err := s.api.Tokens([]File{f})
	if err != nil {
		log.Println("server:", "failed to get tokens for file:", err)
	}
	token, ok := tokens[f.String()]
	if !ok || filename == onDemandFilename {
		c.String(http.StatusNotFound, "404: not found")
		return
	}
	// checking for request from local network
	for _, net := range s.localNetworks {
		if net.Contains(ip) {
			c.String(http.StatusNotFound, "404: not found locally")
			return
		}
	}
	s.proxy(c, f, token, 1, 1, onDemandFilename)
}

func getSHA1(sep string, args []string) string {
	hasher := sha1.New()
	toHash := strings.Join(args, sep)
	fmt.Fprint(hasher, toHash)
	hasher.Sum([]byte(toHash))
	// log.Println("checksum of", toHash)
	return hex.EncodeToString(hasher.Sum(nil))
}

type commandHandler func(*gin.Context, Args)

func (s *DefaultServer) isDeltaValid(ts string) bool {
	if s.cfg.DontCheckTimestamps {
		return true
	}
	timestamp, err := strconv.ParseInt(ts, intBase, 64)
	if err != nil {
		log.Println("server:", "timestamp check failed", err)
		return false
	}
	delta := time.Now().Unix() - timestamp
	if delta < 0 {
		delta *= -1
	}
	if delta > maximumTimeLag {
		log.Println("server:", "timestamp delta is too bing", delta, ">", maximumTimeLag)
		return false
	}
	return true
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

func (s *DefaultServer) stillAliveLoop() {
	ticker := time.NewTicker(stillAliveInterval)
	defer ticker.Stop()
	defer s.wg.Done()
	for {
		select {
		case t := <-ticker.C:
			if err := s.api.StillAlive(); err != nil {
				log.Println("server:", "still alive notification failed:", err)
			}
			log.Println("server:", "still sent alive", t)
		case _ = <-s.stop:
			return
		}
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
			log.Println("server:", "files to remove:", count)
			s.updateLock.Lock()
			defer s.updateLock.Unlock()
			if err := s.removeAllUnused(deadline); err != nil {
				log.Println("error while removing files", err)
			} else {
				log.Println("server:", "removed", count)
			}
		case _ = <-s.stop:
			return
		}
	}
}

// /servercmd/<command>/<additional:kwds>/<timestamp:int>/<key>
func (s *DefaultServer) handleCommand(c *gin.Context) {
	// checking remote ip
	log.Println("command:", c.Request.URL.Path)
	if !s.cfg.Settings.IsRPCServer(c.Request) {
		log.Println("server:", "got request from suspicous origin", c.Request.RemoteAddr)
		if !s.cfg.Debug {
			c.String(http.StatusUnauthorized, "401: not authorised")
			return
		}
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
		c.String(http.StatusUnauthorized, "401: bad timestamp")
		return
	}

	hashArgs := []string{
		cmdKeyStart,
		command,
		kwds,
		sClientID,
		timestampS,
		s.cfg.Key,
	}
	keyCalculated := getSHA1(cmdKeyDelimiter, hashArgs)
	if key != keyCalculated && !s.cfg.DontCheckSHA1 {
		log.Println("server:", "checksum missmatch", key, keyCalculated)
		c.String(http.StatusUnauthorized, "401: bad sign")
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

// commandRefreshSettings precesses request to refresh settings
func (s *DefaultServer) commandRefreshSettings(c *gin.Context, args Args) {
	if err := s.refreshSettings(); err != nil {
		c.String(http.StatusOK, "false")
		return
	}
	c.String(http.StatusOK, "true")
}

func (s *DefaultServer) refreshSettings() error {
	log.Println("server:", "refreshing settings")
	settings, err := s.api.Settings()
	if err != nil {
		log.Println("server:", "failed to refresh settings", err)
		return err
	}
	s.cfg.Settings = settings
	log.Println("server:", "refreshed settings")
	return err
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
	log.Println("proxy: tested for", duration)
	writeStatus(downloadSuccess, duration.Seconds())
}

// commandStillAlive is heartbeat response
func (s *DefaultServer) commandStillAlive(c *gin.Context, _ Args) {
	c.String(http.StatusOK, "I feel FANTASTIC and I'm still alive")
}

func (s *DefaultServer) commandThreadedProxyTest(c *gin.Context, args Args) {
	port := args.GetInt("port")
	ip := net.ParseIP(args.Get("ipaddr"))
	size := args.GetInt64("testsize")
	count := args.GetInt("testcount")
	timestamp := args.GetInt64("testtime")
	key := args.Get("testkey")

	type Result struct {
		err      error
		duration time.Duration
	}

	var (
		totalDuration time.Duration
		testsPassed   int
		results       = make(chan Result)
	)

	u := new(url.URL)
	u.Host = fmt.Sprintf("%s:%d", ip, port)
	u.Scheme = downloadScheme

	for attempt := 0; attempt < count; attempt++ {
		u.Path = fmt.Sprintf("/t/%d/%d/%s/%d", size, timestamp, key, rand.Int())
		go func(workerURL url.URL) {
			rc, err := s.api.GetFile(&workerURL)
			if err != nil {
				results <- Result{err: err}
				return
			}
			defer rc.Close()
			start := time.Now()
			n, err := io.CopyN(speedTest{}, rc, size)
			if err != nil || n != size {
				log.Println("proxy: failed to download", err)
				results <- Result{err: errors.New("failed to download")}
				return
			}

			duration := time.Now().Sub(start)
			results <- Result{duration: duration}
		}(*u)
	}

	for attempt := 0; attempt < count; attempt++ {
		result := <-results
		if result.err != nil {
			continue
		}
		testsPassed++
		log.Println("proxy:", "test passed", result.duration)
		totalDuration += result.duration
	}
	close(results)
	totalTimeMilliseconds := totalDuration.Nanoseconds() / int64(time.Millisecond)
	log.Println("proxy:", "test completed for", totalDuration)
	fmt.Fprintf(c.Writer, "OK:%d-%d", testsPassed, totalTimeMilliseconds)
}

// proxyTest
// /t/:size/:timestamp/:key/:n
func (s *DefaultServer) proxyTest(c *gin.Context) {
	sizeS := c.Param("size")
	timestampS := c.Param("timestamp")
	key := c.Param("key")

	if !s.isDeltaValid(timestampS) {
		c.String(http.StatusUnauthorized, "403: bad timestamp")
		return
	}

	// !MiscTools.getSHAString("hentai@home-speedtest-" + testsize + "-" + testtime + "-" + Settings.getClientID() + "-" + Settings.getClientKey()).equals(testkey))
	args := []string{
		"hentai@home-speedtest",
		sizeS,
		timestampS,
		sInt64(s.cfg.Credentials.ClientID),
		s.cfg.Credentials.Key,
	}
	expected := getSHA1("-", args)
	if expected != key && !s.cfg.DontCheckSHA1 {
		c.String(http.StatusUnauthorized, "403: bad key")
		return
	}

	size, err := strconv.ParseInt(sizeS, intBase, 64)
	if size > size100MB {
		log.Println("proxy:", "got test with too big size", size)
		c.String(http.StatusUnauthorized, "403: bad size")
		return
	}

	// sending test
	c.Writer.Header().Add(headerContentLength, sizeS)
	n, err := io.CopyN(c.Writer, speedTest{}, size)
	if err != nil || n != size {
		log.Println("proxy:", "speed test failed", err)
	}
}

// commandList returns list of files in ache
func (s *DefaultServer) commandList(c *gin.Context, args Args) {
	log.Println("server:", "sending file list")
	files := make(chan File)
	max := args.GetInt64("max_filecount")
	// spawning goroutine that reads from database
	// and sends files to channel
	log.Println("server:", "we have", s.db.Count(), "files in db")
	go func() {
		if err := s.db.GetBatch(files, max); err != nil {
			log.Println("server:", "failed to generate file list", err)
		}
		close(files)
	}()
	var count int64
	for file := range files {
		count++
		fmt.Fprintln(c.Writer, file)
	}
	log.Println("server:", "sent", count)
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
			log.Println("error while updating lastUsage:", err)
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

// file register loop
func (s *DefaultServer) registerLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.UpdateRate)
	defer ticker.Stop()

	var files []File
	update := func() {
		if files == nil {
			return
		}
		if err := s.api.AddFiles(files); err != nil {
			log.Println("api: failed to add files:", err)
		} else {
			log.Println("api:", "registered files:", len(files))
		}
		files = nil
	}
	// we need to register last files before stopping server
	defer update()
	for {
		select {
		case f := <-s.registerQuery:
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
	log.Println("server:", "starting")
	if err := s.api.Login(); err != nil {
		return err
	}

	if err := s.refreshSettings(); err != nil {
		return err
	}

	s.stop = make(chan bool)
	go s.stopNotificator()

	// starting lastUsage update loop
	s.wg.Add(1)
	go s.useLoop()

	// starting removal loop
	s.wg.Add(1)
	go s.removeLoop()

	// starting still alive loop
	s.wg.Add(1)
	go s.stillAliveLoop()

	// starting register loop
	s.wg.Add(1)
	go s.registerLoop()

	s.started = true
	log.Println("server:", "started")
	return nil
}

// Listen on confgured port
func (s *DefaultServer) Listen() error {
	addr := fmt.Sprintf(":%d", s.cfg.Settings.Port)
	go func() {
		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			time.Sleep(time.Millisecond * 500)
			log.Println("client:", "sending start notification")
			if err := s.api.Start(); err != nil {
				log.Println("client:", "start notification failed:", err)
				if err == ErrClientStartupFlood {
					wait := time.Second * 60
					log.Println("client:", "will try again in", wait)
					time.Sleep(wait)
					continue
				}
				continue
			}
			log.Println("client:", "registered in hath network")
			return
		}
		log.Fatalln("client:", "failed to register in hath network")
	}()
	log.Println("server:", "listening on", addr)
	return http.ListenAndServe(addr, s)
}

func (s *DefaultServer) stopNotificator() {
	<-s.stop
	log.Println("stopping server")
}

const progressChannelSize = 20
const populateBatchSize = 10000

// PopulateFromFrontend scans frontend and adds all files in it to database
func (s DefaultServer) PopulateFromFrontend() error {
	files := make(chan File)
	progress := make(chan Progress, progressChannelSize)
	go func() {
		defer close(files)
		if err := s.frontend.Scan(files, progress); err != nil {
			log.Println("cache scan failed; unable to add all files:", err)
		} else {
			log.Println("cache scan completed")
		}
	}()
	go func() {
		for p := range progress {
			log.Println("scan progres:", p)
		}
	}()
	batch := make([]File, 0, populateBatchSize)
	for f := range files {
		batch = append(batch, f)
		if len(batch) >= populateBatchSize {
			log.Println("writing", len(batch), "files to db")
			if err := s.db.AddBatch(batch); err != nil {
				log.Println("server:", "failed to add to db", err)
			}
			// resetting batch
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		log.Println("writing", len(batch), "files to db")
		if err := s.db.AddBatch(batch); err != nil {
			log.Println("server:", "failed to add to db", err)
		}
	}

	return nil
}

// Close stops server
func (s *DefaultServer) Close() error {
	if !s.started {
		return nil
	}
	close(s.useQuery)
	close(s.registerQuery)
	close(s.stop)
	s.wg.Wait()

	if err := s.api.Close(); err != nil {
		log.Println("server:", "close notification failed", err)
	}

	s.db.Close()
	s.started = false
	log.Println("server:", "stopped")
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
	Debug               bool
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
		cfg.Client = NewClient(ClientConfig{Credentials: cfg.Credentials, Debug: cfg.Debug})
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

	// config init
	if cfg.DontCheckTimestamps {
		log.Println("warning: not checking timestamps")
	}

	if cfg.DontCheckSHA1 {
		log.Println("warning: not checking sha1")
	}

	if cfg.Debug {
		log.Println("warning:", "running in debug mode")
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	e := gin.New()
	e.Use(gin.Logger())

	// routing init
	e.Use(gin.Recovery())
	e.GET("/h/:fileid/:kwds/:filename", s.handleImage)
	e.GET("/servercmd/:command/:kwds/:timestamp/:key", s.handleCommand)
	e.GET("/p/:kwds/:filename", s.handleProxy)
	e.GET("/t/:size/:timestamp/:key/:n", s.proxyTest)
	e.GET("/favicon.ico", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "http://g.e-hentai.org/favicon.ico")
	})
	e.GET("/robots.txt", func(c *gin.Context) {
		c.Writer.Header().Add(headerContentType, "text/plain")
		fmt.Fprintf(c.Writer, "User-agent: *\nDisallow: /")
	})

	// routing for commands
	s.commands = map[string]commandHandler{
		cmdSpeedTest:         s.commandSpeedTest,
		cmdDownload:          s.commandDownload,
		cmdProxyTest:         s.commandProxyTest,
		cmdRefreshSettings:   s.commandRefreshSettings,
		cmdStillAlive:        s.commandStillAlive,
		cmdList:              s.commandList,
		cmdThreadedProxyTest: s.commandThreadedProxyTest,
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
	s.registerQuery = make(chan File)
	s.wg = new(sync.WaitGroup)
	s.api = cfg.Client
	s.stop = make(chan bool)
	s.updateLock = new(sync.Mutex)
	return s
}
