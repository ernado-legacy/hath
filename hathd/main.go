package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"cydev.ru/hath"
)

const version = "0.1a"

var (
	clientID  int64
	clientKey string
)

func init() {
	flag.Int64Var(&clientID, "client-id", 0, "Hentai@Home client id")
	flag.StringVar(&clientKey, "client-key", "", "Hentai@Home client key")
}

func main() {
	flag.Parse()
	f, err := os.Open("testestst")
	fmt.Println(f, err, os.IsNotExist(err))
	fmt.Println("Hentai@Home", version)
	cache := new(hath.FileCache)
	frontend := hath.NewDirectFrontend(cache)
	credentials := hath.Credentials{ClientID: clientID, Key: clientKey}
	cfg := hath.ServerConfig{}
	cfg.Credentials = credentials
	cfg.Frontend = frontend
	s := hath.NewServer(cfg)
	log.Fatal(http.ListenAndServe(":5569", s))
}
