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
)

const version = "0.1a"

var (
	clientID        int64
	dir             string
	clientKey       string
	credentialsPath string
	debug           bool
)

func init() {
	flag.Int64Var(&clientID, "client-id", 0, "Hentai@Home client id")
	flag.BoolVar(&debug, "debug", false, "enable debug")
	flag.StringVar(&clientKey, "client-key", "", "Hentai@Home client key")
	flag.StringVar(&dir, "dir", "hath", "working directory")
	flag.StringVar(&credentialsPath, "cfg", "cfg.toml", "Path to credentials")
}

func main() {
	flag.Parse()
	fmt.Println("Hentai@Home", version)
	frontend := hath.NewFrontend(dir)
	db, err := hath.NewDB(path.Join(dir, "hath.db"))
	if err != nil {
		log.Fatal(err)
	}
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
		log.Println("credentials loaded", credentials)
	}
	cfg.Credentials = credentials
	cfg.Frontend = frontend
	cfg.DataBase = db
	if debug {
		cfg.DontCheckTimestamps = true
		cfg.DontCheckSHA1 = true
	}
	s := hath.NewServer(cfg)
	clientCfg := hath.ClientConfig{}
	clientCfg.Credentials = credentials
	c := hath.NewClient(clientCfg)
	if err := c.CheckStats(); err != nil {
		log.Fatal("check stats:", err)
	}
	settings, err := c.Settings()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", settings.Port), s))
}
