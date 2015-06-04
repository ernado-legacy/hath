package main

import (
	"cydev.ru/hath"
	"flag"
	"fmt"
	"github.com/pivotal-golang/bytefmt"
	"log"
	"time"
)

var (
	count   int64
	dir     string
	sizeMax int64
	sizeMin int64
	resMax  int
	resMin  int
	workers int
)

func init() {
	flag.Int64Var(&count, "count", 100, "files to generate")
	flag.Int64Var(&sizeMax, "size-max", 1024*100, "maximum file size in bytes")
	flag.Int64Var(&sizeMin, "size-min", 1024*5, "minimum file size in bytes")
	flag.IntVar(&resMax, "res-max", 1980, "maximum ephemeral resolution")
	flag.IntVar(&resMin, "res-min", 500, "minumum ephemeral resolution")
	flag.IntVar(&workers, "workers", 1, "concurrent workers")
	flag.StringVar(&dir, "dir", "", "working directory")
}

func main() {
	flag.Parse()
	g := hath.FileGenerator{
		SizeMax:       sizeMax,
		SizeMin:       sizeMin,
		ResolutionMax: resMax,
		ResolutionMin: resMin,
		Dir:           dir,
	}
	files := make(chan hath.File)
	worker := func(work chan hath.File) {
		log.Println("starting worker")
		for {
			f, err := g.New()
			if err != nil {
				log.Fatal(err)
			}
			work <- f
		}
	}
	for i := 0; i < workers; i++ {
		go worker(files)
	}
	fmt.Printf("%+v\n", g)
	start := time.Now()
	var i int64
	var total int64
	for i = 0; i < count; i++ {
		f := <-files
		total += f.Size()
		fmt.Println(f)
	}
	end := time.Now()
	duration := end.Sub(start)
	totalWrote := bytefmt.ByteSize(uint64(total))
	perSecond := float64(total) / duration.Seconds()
	rate := bytefmt.ByteSize(uint64(perSecond))
	fmt.Printf("OK for %v\n", duration)
	fmt.Printf("%s at rate %s/s\n", totalWrote, rate)
}
