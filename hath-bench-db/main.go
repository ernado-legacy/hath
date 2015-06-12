package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"cydev.ru/hath"
	"github.com/pivotal-golang/bytefmt"
)

var (
	count      int64
	dbpath     string
	sizeMax    int64
	sizeMin    int64
	resMax     int
	resMin     int
	workers    int
	generate   bool
	cpus       int
	collect    bool
	bulkSize   int64
	onlyOpen   bool
	onlyMemory bool
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
)

func init() {
	flag.Int64Var(&count, "count", 10000, "files to generate")
	flag.Int64Var(&bulkSize, "bulk", 10000, "bulk size")
	flag.Int64Var(&sizeMax, "size-max", 1024*100, "maximum file size in bytes")
	flag.Int64Var(&sizeMin, "size-min", 1024*5, "minimum file size in bytes")
	flag.IntVar(&resMax, "res-max", 1980, "maximum ephemeral resolution")
	flag.IntVar(&resMin, "res-min", 500, "minumum ephemeral resolution")
	flag.BoolVar(&generate, "generate", false, "generate data")
	flag.BoolVar(&collect, "collect", true, "collect old files")
	flag.BoolVar(&onlyOpen, "only-open", false, "only open db")
	flag.BoolVar(&onlyMemory, "only-memory", false, "only load to memory")
	flag.StringVar(&dbpath, "dbfile", "db.bolt", "working directory")
	flag.IntVar(&cpus, "cpus", runtime.GOMAXPROCS(0), "cpu to use")
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	flag.Parse()
	runtime.GOMAXPROCS(cpus)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	g := hath.FileGenerator{
		SizeMax:       sizeMax,
		SizeMin:       sizeMin,
		ResolutionMax: resMax,
		ResolutionMin: resMin,
		TimeDelta:     20,
	}
	if onlyMemory {
		log.Println("allocating")
		var files = make([]hath.File, count)
		log.Println("generating")
		for i := range files {
			files[i] = g.NewFake()
		}
		log.Println("generated", len(files))
		fmt.Scanln()
		start := time.Now()
		log.Println("iterating...")
		var count int
		for _, f := range files {
			if f.Static {
				count++
			}
		}
		end := time.Now()
		duration := end.Sub(start)
		rate := float64(len(files)) / duration.Seconds()
		fmt.Println("static", count)
		fmt.Printf("OK for %v at rate %f per second\n", duration, rate)
		log.Println("iterated")
		os.Exit(0)
	}
	db, err := hath.NewDB(dbpath)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	d, err := g.NewFake().Marshal()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("average file info is", len(d), "bytes")
	log.Printf("%x", d)
	if onlyOpen {
		log.Println("only open. Waiting for 10s")
		count = int64(db.Count())
		start := time.Now()
		n, err := db.GetOldFilesCount(time.Now().Add(time.Second * -10))
		if err != nil {
			log.Fatal(err)
		}
		log.Println("got", n)
		end := time.Now()
		duration := end.Sub(start)
		rate := float64(count) / duration.Seconds()
		log.Printf("Scanned %d for %v at rate %f per second\n", count, duration, rate)
		time.Sleep(10 * time.Second)
	}
	if generate {
		log.Println("generating", count, "files")

		fmt.Printf("%+v\n", g)
		var i int64
		files := make([]hath.File, count)
		for i = 0; i < count; i++ {
			files[i] = g.NewFake()
		}
		start := time.Now()
		if count < bulkSize {
			log.Println("writing")
			if err := db.AddBatch(files); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Println("writing in bulks")
			for i = 0; i+bulkSize < count; i += bulkSize {
				bulkstart := time.Now()

				if err := db.AddBatch(files[i : i+bulkSize]); err != nil {
					log.Fatal(err)
				}

				log.Println("from", i, "to", i+bulkSize, time.Now().Sub(bulkstart))
			}
			log.Println("from", i+bulkSize, "to", count)
			if err := db.AddBatch(files[i:]); err != nil {
				log.Fatal(err)
			}
		}
		end := time.Now()
		duration := end.Sub(start)
		rate := float64(count) / duration.Seconds()
		fmt.Printf("OK for %v at rate %f per second\n", duration, rate)
	}
	// if collect {
	// 	log.Println("collecting")
	// 	start := time.Now()
	// 	n, err := db.Collect(time.Now().Add(-time.Second))
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	//
	// 	end := time.Now()
	// 	duration := end.Sub(start)
	// 	rate := float64(n) / duration.Seconds()
	// 	fmt.Printf("Removed %d for %v at rate %f per second\n", n, duration, rate)
	// }
	log.Println(count, "is rought", bytefmt.ByteSize(hath.GetRoughCacheSize(count)))
	log.Println("OK")
}
