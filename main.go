package main

import (
	"flag"
	"fmt"
	"log"
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
	fmt.Println("Hentai@Home")
	fmt.Println("Version:", version)
	fmt.Println("Client version:", clientVersion)
	fmt.Println("Client build:", clientBuild)
	client := NewClient(clientID, clientKey)
	client.StillAlive()
	log.Println("started")
}
