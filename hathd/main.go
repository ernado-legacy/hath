package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"path"

	"cydev.ru/hath"
)

const version = "0.1a"

var (
	clientID  int64
	dir       string
	clientKey string
)

func init() {
	flag.Int64Var(&clientID, "client-id", 0, "Hentai@Home client id")
	flag.StringVar(&clientKey, "client-key", "", "Hentai@Home client key")
	flag.StringVar(&dir, "dir", "hath", "working directory")
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
	cfg.Credentials = credentials
	cfg.Frontend = frontend
	cfg.DataBase = db
	s := hath.NewServer(cfg)
	clientCfg := hath.ClientConfig{}
	clientCfg.Credentials = credentials
	c := hath.NewClient(clientCfg)
	if err := c.CheckStats(); err != nil {
		log.Fatal(err)
	}
	if err := c.Settings(); err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	log.Fatal(http.ListenAndServe(":5569", s))
}
