package main

import (
	"flag"
	"fmt"
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
	client.printRequest(actionStart)
	fmt.Println(client.ActionURL("test"))
}
