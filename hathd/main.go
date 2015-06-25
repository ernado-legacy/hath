package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"

	"cydev.ru/hath"

	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/xlab/closer"
)

const version = "0.5dev"

var (
	clientID        int64
	dir             string
	clientKey       string
	credentialsPath string
	debug           bool
	scan            bool
)

func createDirIfNotExists() error {
	s, err := os.Stat(dir)
	if err == nil && s.IsDir() {
		return nil
	}
	if os.IsNotExist(err) {
		return os.Mkdir(dir, 0777)
	}
	return err
}

func init() {
	flag.Int64Var(&clientID, "client-id", 0, "Hentai@Home client id")
	flag.BoolVar(&debug, "debug", false, "enable debug")
	flag.BoolVar(&scan, "scan", false, "scan files from cache and add them to database")
	flag.StringVar(&clientKey, "client-key", "", "Hentai@Home client key")
	flag.StringVar(&dir, "dir", "hath", "working directory")
	flag.StringVar(&credentialsPath, "cfg", "cfg.toml", "Path to credentials")
}

func main() {
	flag.Parse()
	fmt.Println("Hentai@Home", version)

	if err := createDirIfNotExists(); err != nil {
		log.Fatal("hath: error while checking directory", dir, err)
	}

	frontend := hath.NewFrontend(dir)
	db, err := hath.NewDB(path.Join(dir, "hath.db"))
	if err != nil {
		log.Fatal(err)
	}
	filesInDB := db.Count()
	log.Println("hath:", "files in database:", filesInDB)

	credentials := hath.Credentials{ClientID: clientID, Key: clientKey}
	cfg := hath.ServerConfig{}
	if len(credentialsPath) != 0 {
		f, err := os.Open(credentialsPath)
		if err != nil {
			log.Fatal(err)
		}
		_, err = toml.DecodeReader(f, &credentials)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("hath: credentials loaded from", credentialsPath)
	}
	cfg.Credentials = credentials
	cfg.Frontend = frontend
	cfg.DataBase = db
	if debug {
		cfg.DontCheckTimestamps = true
		cfg.DontCheckSHA1 = true
		cfg.Debug = true
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	log.Println("hath:", "starting")
	s := hath.NewServer(cfg)

	// profiling endpoint
	// if debug {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// }

	// populating database from disk
	if filesInDB == 0 || scan {
		log.Println("server:", "database is empty; trying to scan files in cache")
		if err := s.PopulateFromFrontend(); err != nil {
			log.Fatalln("server:", "failed to scan files and add them to db:", err)
		}
		log.Println("server:", "cache scanned")
	}

	closer.Bind(func() {
		s.Close()
	})

	// starting server
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
	log.Fatal(s.Listen())
	closer.Hold()
}
