package main

import (
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"cydev.ru/hath"

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
	log.Print("scanning")
	start := time.Now()
	frontend := hath.NewFrontend(dir)
	files := make(chan hath.File)
	progress := make(chan hath.Progress)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		for p := range progress {
			log.Println("scan progress", p)
		}
	}()
	go func() {
		defer close(files)
		defer wg.Done()
		if err := frontend.Scan(files, progress); err != nil {
			log.Println("error while scanning:", err)
		} else {
			log.Println("scanned")
		}
	}()
	w := new(walker)
	for f := range files {
		// fmt.Print(".")
		w.count++
		w.sum += f.Size
	}
	wg.Wait()
	log.Println("end")
	end := time.Now()
	duration := end.Sub(start)
	log.Println("count", w.count, "size", bytefmt.ByteSize(uint64(w.sum)), "duration", duration)
}
