package main

import (
	"flag"
	"fmt"
	"log"
	"time"
)

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
	fmt.Println("Hello, hath!")
	client := NewClient(clientID, clientKey)
	client.StillAlive()
	err := client.getBlacklist(time.Hour * 24 * 365)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("started")
}
