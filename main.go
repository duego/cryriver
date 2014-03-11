// Package cryriver is used for indexing mongodb objects into elasticsearch in real time.
package main

import (
	"flag"
	"fmt"
	"github.com/duego/cryriver/elasticsearch"
	"github.com/duego/cryriver/mongodb"
	"labix.org/v2/mgo"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

var (
	mongoServer   = flag.String("mongo", "localhost", "Specific server to tail")
	mongoInitial  = flag.Bool("initial", false, "True if we want to do initial sync from the full collection, otherwise resume reading oplog")
	esServer      = flag.String("es", "http://localhost:9200", "Elasticsearch server to index to")
	esConcurrency = flag.Int("concurrency", 1, "Maximum number of simultaneous ES connections")
	esIndex       = flag.String("index", "testing", "Elasticsearch index to use")
	optimeStore   = flag.String("db", "/tmp/cryriver.db", "What file to save progress on for oplog resumes")
	ns            = flag.String("ns", "api.users", "The namespace to tail on")
	debugAddr     = flag.String("debug", "127.0.0.1:5000", "Which address to listen on for debug, empty for no debug")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// Enable http server for debug endpoint
	go func() {
		if *debugAddr != "" {
			log.Println(http.ListenAndServe(*debugAddr, nil))
		}
	}()

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	mgoSession, err := mgo.Dial(*mongoServer + "?connect=direct")
	if err != nil {
		log.Fatal(err)
	}
	defer mgoSession.Close()
	mongoc := make(chan *mongodb.Operation)
	mongoErr := make(chan error)
	exit := make(chan bool)
	go func() {
		mongoErr <- mongodb.Tail(mgoSession.New(), *ns, *mongoInitial, lastEsSeen, mongoc, exit)
	}()

	esc := make(chan elasticsearch.Transaction)
	esDone := make(chan bool)
	go func() {
		// Boot up our slurpers.
		// The client will have the transport configured to allow the same amount of connections
		// as go routines towards ES, each connection may be re-used between slurpers.
		client := elasticsearch.NewClient(fmt.Sprintf("%s/_bulk", *esServer), *esConcurrency)
		var slurpers sync.WaitGroup
		slurpers.Add(*esConcurrency)
		for n := 0; n < *esConcurrency; n++ {
			go func() {
				elasticsearch.Slurp(client, esc)
				slurpers.Done()
			}()
		}
		slurpers.Wait()
		close(esDone)
	}()

	tailDone := make(chan bool)
	go func() {
		// Map mongo collections to es index
		indexes := map[string]string{
			strings.Split(*ns, ".")[0]: *esIndex,
		}
		for op := range mongoc {
			// Wrap all mongo operations to comply with ES interface, then send them off to the slurper.
			esOp := mongodb.NewEsOperation(mgoSession, indexes, nil, op)
			select {
			case esc <- esOp:
				lastEsSeenC <- &op.Timestamp
			// Abort delivering any pending EsOperations we might block for
			case <-exit:
				break
			}
		}
		// If mongoc closed, tailer has stopped
		close(tailDone)
	}()

	select {
	//  Get more operations from mongo tail
	case <-tailDone:
		log.Println("MongoDB tailer returned")
	// ES client closed
	case <-esDone:
		log.Println("ES slurper returned")
	// An interrupt signal was catched
	case <-interrupt:
		log.Println("Closing down...")
	}
	close(exit)

	// MongoDB tailer shutdown
	if err := <-mongoErr; err != nil {
		log.Println(err)
	} else {
		log.Println("No errors occured in mongo tail")
	}

	// Elasticsearch indexer shutdown
	log.Println("Waiting for EsOperation tail to stop")
	<-tailDone

	log.Println("Waiting for ES to return")
	// We are the producer for this channel, close it down and wait for ES slurpers to return
	close(esc)
	<-esDone
	log.Println("Bye!")
}
