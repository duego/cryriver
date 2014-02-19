// Package cryriver is used for indexing mongodb objects into elasticsearch in real time.
package main

import (
	"flag"
	"github.com/duego/cryriver/bridge"
	"github.com/duego/cryriver/elasticsearch"
	"github.com/duego/cryriver/mongodb"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	mongoServer = flag.String("mongo", "localhost", "Specific server to tail")
	esServer    = flag.String("es", "localhost", "Elasticsearch server to index to")
	esIndex     = flag.String("index", "testing", "Elasticsearch index to use")
	ns          = flag.String("ns", "api.users", "The namespace to tail on")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	opc := make(chan *mongodb.Operation)
	closingMongo := make(chan chan error)
	go mongodb.Tail(*mongoServer, *ns, opc, closingMongo)

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	transformations := []bridge.Transformer{
		&bridge.NopTransformation{},
	}

	esDone := make(chan bool)
	go func() {
		elasticsearch.Slurp(*esServer, *esIndex, opc, transformations)
		close(esDone)
	}()

tail:
	for {
		select {
		case <-esDone:
			log.Println("ES slurper returned")
			break tail
		case <-interrupt:
			log.Println("Closing down...")
			break tail
		}
	}

	errc := make(chan error)
	closingMongo <- errc
	if err := <-errc; err != nil {
		log.Println(err)
	} else {
		log.Println("No errors occured in mongo tail")
	}
	log.Println("Waiting for ES to return")
	<-esDone
	log.Println("Bye!")
}
