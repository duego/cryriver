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
)

var (
	mongoServer = flag.String("mongo", "localhost", "Specific server to tail")
	esServer    = flag.String("es", "localhost", "Elasticsearch server to index to")
	esIndex     = flag.String("index", "testing", "Elasticsearch index to use")
	ns          = flag.String("ns", "api.users", "The namespace to tail on")
	pprofAddr   = flag.String("pprof", "127.0.0.1:5000", "Which address to listen on for profiling")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	go func() {
		log.Println(http.ListenAndServe(*pprofAddr, nil))
	}()

	mongoc := make(chan *mongodb.Operation)
	closingMongo := make(chan chan error)
	go mongodb.Tail(*mongoServer, *ns, mongoc, closingMongo)

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	esc := make(chan *elasticsearch.Operation)
	esDone := make(chan bool)
	go func() {
		elasticsearch.Slurp(*esServer, esc)
		close(esDone)
	}()

	mapper := mongodb.NewEsMapper(*esIndex)
	var (
		esOp          *elasticsearch.Operation
		esDelivery    chan *elasticsearch.Operation = nil
		mongoDelivery chan *mongodb.Operation       = mongoc
		err           error
	)

tail:
	for {
		select {
		case op := <-mongoDelivery:
			if op == nil {
				break tail
			}
			if esOp, err = mapper.EsMap(op); err != nil {
				log.Println(err, esOp)
			} else {
				// Nil switch to block mongo delivery until elasticsearch delivery is done
				mongoDelivery = nil
				esDelivery = esc
			}
		case esDelivery <- esOp:
			// Forward mappers to elasticsearch based on operations from mongodb tail
			// Block the channel until new deliveries are available
			esDelivery = nil
			mongoDelivery = mongoc
		case <-esDone:
			log.Println("ES slurper returned")
			break tail
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
