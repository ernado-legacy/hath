// Package hath is Hentai@Home client implementation in golang
package hath // import "cydev.ru/hath"

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
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
	api      *Client
	cfg      ServerConfig
	frontend Frontend
	db       DataBase
	e        *gin.Engine
	useQuery chan File
	wg       *sync.WaitGroup
	started  bool
	commands map[string]commandHandler
	stop     chan bool
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
	size := args.GetInt64("testsize")
	if size <= 0 || size > size100MB {
		size = size100MB
	}
	c.Header(headerContentLength, strconv.FormatInt(size, intBase))
	_, err := io.CopyN(c.Writer, speedTest{}, size)
	if err != nil {
		log.Println("speed test error", err)
	}
}

func (s *DefaultServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.e.ServeHTTP(w, r)
}

// lastUsage update loop
func (s *DefaultServer) useLoop() {
	defer s.wg.Done()
	for f := range s.useQuery {
		if err := s.db.Use(f); err != nil {
			log.Printf("db miss for %s, writing to database\n", f.HexID())
			if err := s.db.Add(f); err != nil {
				log.Println("db error while adding file", f)
			}
		}
	}
}

func (s *DefaultServer) addFromURL(f File, u *url.URL) error {
	rc, err := s.api.GetFile(u)
	if err != nil {
		return err
	}
	defer rc.Close()
	return s.frontend.Add(f, rc)
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

	// routing for commands
	commands := make(map[string]commandHandler)
	commands["speed_test"] = s.commandSpeedTest
	s.commands = commands

	if cfg.DontCheckTimestamps {
		log.Println("warning: not checking timestamps")
	}

	if cfg.DontCheckSHA1 {
		log.Println("warning: not checking sha1")
	}

	s.e = e
	s.useQuery = make(chan File, useQuerySize)
	s.wg = new(sync.WaitGroup)
	s.api = cfg.Client
	s.stop = make(chan bool)
	return s
}
