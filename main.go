// Package cryriver is used for indexing mongodb objects into elasticsearch in real time.
package main

import (
	"flag"
	"github.com/duego/cryriver/elasticsearch"
	"github.com/duego/cryriver/mongodb"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	mongoServer = flag.String("mongo", "localhost", "Specific server to tail")
	esServer    = flag.String("es", "localhost", "Elasticsearch server to index to")
	esIndex     = flag.String("index", "testing", "Elasticsearch index to use")
	optimeStore = flag.String(
		"db", "/tmp/cryriver.db", "What file to save progress on for oplog resumes")
	ns        = flag.String("ns", "api.users", "The namespace to tail on")
	debugAddr = flag.String(
		"debug", "127.0.0.1:5000", "Which address to listen on for debug, empty for no debug")
)

func main() {
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

	mongoc := make(chan *mongodb.Operation)
	closingMongo := make(chan chan error)
	go mongodb.Tail(*mongoServer, *ns, lastEsSeen, mongoc, closingMongo)

	esc := make(chan *elasticsearch.Operation)
	esDone := make(chan bool)
	go func() {
		elasticsearch.Slurp(*esServer, esc)
		close(esDone)
	}()

	lastEsSeenTimer := time.NewTicker(time.Second)
	mapper := mongodb.NewEsMapper(*esIndex)
	var (
		// Operations ES has seen
		seenMongoOp *mongodb.Operation
		// Last operation from mongo tailer
		mongoOp *mongodb.Operation
		// Elasticsearch operations mapped from mongo operations
		esOp *elasticsearch.Operation
		// Nil switched channel for enabling ES delivery once a mongo mapped operation is available
		esDelivery chan *elasticsearch.Operation = nil
		// Nil switched channel for enabling more mongo operations once ES operation has been delivered
		mongoDelivery chan *mongodb.Operation = mongoc
		err           error
	)

tail:
	for {
		select {
		//  Get more operations from mongo tail
		case mongoOp = <-mongoDelivery:
			if mongoOp == nil {
				break tail
			}
			if esOp, err = mapper.EsMap(mongoOp); err != nil {
				log.Println(err, esOp)
			} else {
				// Nil switch to block mongo delivery until elasticsearch delivery is done
				mongoDelivery = nil
				esDelivery = esc
			}
		// Deliver mapped operation to ES
		case esDelivery <- esOp:
			// Forward mappers to elasticsearch based on operations from mongodb tail
			// Block the channel until new deliveries are available
			esDelivery = nil
			mongoDelivery = mongoc
			// Keep track of what ES has seen so far
			seenMongoOp = mongoOp
		// Store the latest timestamps of operations sent to ES
		case <-lastEsSeenTimer.C:
			if seenMongoOp == nil {
				continue
			}
			lastEsSeenC <- &seenMongoOp.Timestamp
			seenMongoOp = nil
		// ES client closed
		case <-esDone:
			log.Println("ES slurper returned")
			break tail
		// An interrupt signal was catched
		case <-interrupt:
			log.Println("Closing down...")
			break tail
		}
	}

	// MongoDB tailer shutdown
	errc := make(chan error)
	closingMongo <- errc
	if err := <-errc; err != nil {
		log.Println(err)
	} else {
		log.Println("No errors occured in mongo tail")
	}

	// Elasticsearch indexer shutdown
	close(esc)
	log.Println("Waiting for ES to return")
	<-esDone
	log.Println("Bye!")
}
