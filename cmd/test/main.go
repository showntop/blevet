package main

import (
	_ "expvar"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"blevet"
)

var nShards = flag.Int("shards", 1, "shard size for indexing")
var bindAddr = flag.String("addr", ":8094", "http listen address")
var indexPath = flag.String("index", "./indexes", "index path")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

func main() {

	flag.Parse()

	log.Printf("GOMAXPROCS: %d", runtime.GOMAXPROCS(-1))

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	// open the index
	myIndex := indexer.New(*indexPath, *nShards, 0)

	err := myIndex.Open()
	if err != nil {
		log.Fatalln(err)
	}

	count, err := myIndex.Count()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("doc count:", count)
	myIndex.Test()
}
