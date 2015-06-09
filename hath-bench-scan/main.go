package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	w := new(walker)
	d, err := os.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	dirs, err := d.Readdirnames(0)
	d.Close()
	if err != nil {
		log.Fatal(err)
	}
	wg := new(sync.WaitGroup)
	worker := func(subdirs chan string) {
		defer wg.Done()
		for subdir := range subdirs {
			d, err := os.Open(path.Join(dir, subdir))
			if err != nil {
				log.Println(subdir, err)
				continue
			}
			fmt.Print("d")
			files, err := d.Readdir(0)
			// files, err := d.Readdirnames(0)
			if err != nil {
				log.Println(subdir, err)
			}
			for _, f := range files {
				fmt.Print(".")
				atomic.AddInt64(&w.count, 1)
				atomic.AddInt64(&w.sum, f.Size())
			}
		}
	}
	subdirs := make(chan string)
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go worker(subdirs)
	}
	log.Println("dirs:", len(dirs))
	for _, subdir := range dirs {
		subdirs <- subdir
	}
	close(subdirs)
	wg.Wait()
	fmt.Print("\n")
	end := time.Now()
	duration := end.Sub(start)
	log.Println("count", w.count, "size", bytefmt.ByteSize(uint64(w.sum)), "duration", duration)
	start = time.Now()
	pattern := path.Join(dir, "*", "*")
	log.Println("pattern", pattern)
	matches, err := filepath.Glob(pattern)
	duration = time.Now().Sub(start)
	log.Println("count", len(matches), "duration", duration)
	log.Println(len(matches), "is rought", bytefmt.ByteSize(hath.GetRoughCacheSize(int64(len(matches)))))
}
