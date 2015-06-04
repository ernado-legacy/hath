package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pivotal-golang/bytefmt"
)

var (
	dir     string
	workers int
)

func init() {
	flag.IntVar(&workers, "workers", 1, "concurrent workers")
	flag.StringVar(&dir, "dir", "", "working directory")
}

type walker struct {
	sum   int64
	count int64
}

func (w *walker) Walk(path string, info os.FileInfo, err error) error {
	if err == nil && !info.IsDir() {
		// log.Println(info.Name(), bytefmt.ByteSize(uint64(info.Size())))
		w.sum += info.Size()
		w.count++
	}
	return nil
}

func main() {
	flag.Parse()
	start := time.Now()
	w := new(walker)
	filepath.Walk(dir, w.Walk)
	end := time.Now()
	duration := end.Sub(start)
	log.Println("count", w.count, "size", bytefmt.ByteSize(uint64(w.sum)), "duration", duration)
}
