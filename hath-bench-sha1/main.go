package main

import (
	"crypto/sha1"
	"flag"
	"io"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pivotal-golang/bytefmt"
)

var (
	duration  = flag.Duration("duration", time.Second*5, "duration of test")
	workers   = flag.Int("workers", 1, "workers to use")
	cpus      = flag.Int("cpus", 2, "cpus to use")
	processed int64
)

const (
	chunkSize = 1 * 1024
)

type writer struct {
	sum uint64
}

func (w *writer) Write(b []byte) (n int, err error) {
	n = len(b)
	atomic.AddUint64(&w.sum, uint64(n))
	return n, err
}

type reader struct {
}

func (r *reader) Read(b []byte) (n int, err error) {
	var data [chunkSize]byte
	copy(data[:], b)
	return chunkSize, err
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*cpus)
	w := new(writer)
	r := new(reader)
	worker := func(wr *writer) {
		hasher := sha1.New()
		dst := io.MultiWriter(wr, hasher)
		io.Copy(dst, r)
	}
	for index := 0; index < *workers; index++ {
		go worker(w)
	}
	time.Sleep(*duration)
	bytes := bytefmt.ByteSize(w.sum)
	speed := uint64(float64(w.sum) / duration.Seconds())
	log.Println(duration, bytes, "total;", bytefmt.ByteSize(speed), "per second")

}
