package main

import (
	_ "expvar"
	"flag"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"blevet"
	bleveHttp "github.com/blevesearch/bleve/http"
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

	// count, err := myIndex.Count()
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// log.Println("doc count:", count)

	// add the API
	bleveHttp.RegisterIndexName("myindex", myIndex.IdexerHub())

	searchHandler := bleveHttp.NewSearchHandler("myindex")
	http.HandleFunc("/api/search", xxx(searchHandler))

	// start the HTTP server
	log.Printf("Listening on %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))

}

func xxx(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		stime := time.Now()
		h.ServeHTTP(w, req)
		log.Println("used ", time.Now().Sub(stime))
	}
}
