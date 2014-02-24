// Package elasticsearch handles indexing documents to an ES server.
package elasticsearch

import (
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"time"
)

// D for document, convenience type for indexing ES documents.
type D map[string]interface{}

type IndexerOperation int

const (
	Index IndexerOperation = iota
	Update
)

// Mapper decides what elasticsearch operation that applies.
type Mapper interface {
	EsMap() (Operation, error)
}

type Operation struct {
	Id        string
	Timestamp *time.Time
	Index     string
	Type      string
	TTL       string
	Op        IndexerOperation
	Document  D
}

// Slurp attaches batch indexer to specified server and feeds mapped operations into it,
// closing mapc will stop the indexer.
func Slurp(server string, esc chan *Operation) {
	// Set the Elasticsearch Host to Connect to.
	// TODO: Followup on support for multiple servers, for now we can specify different servers
	// for each process on each mongo shard.
	api.Domain = server
	api.Port = "9200"

	// Bulk Indexing, start with settings similar to the current river configuration.
	indexer := core.NewBulkIndexerErrors(200, 60)
	indexer.BulkMaxBuffer = 10485760
	indexer.BulkMaxDocs = 6000
	done := make(chan bool)
	indexer.Run(done)
	// Clean up on return as the indexer doesn't seem to do it by itself.
	defer func() { indexer.ErrorChannel = nil }()
	defer close(indexer.ErrorChannel)

	go func() {
		for errBuf := range indexer.ErrorChannel {
			// just blissfully print errors forever.
			log.Println(errBuf.Err)
		}
		log.Println("Error channel has been closed")
	}()

	// Loop all incoming operations and send them to the bulk indexer.
	for es := range esc {
		switch es.Op {
		case Index:
			indexer.Index(es.Index, es.Type, es.Id, es.TTL, es.Timestamp, es.Document)
		case Update:
			indexer.Update(es.Index, es.Type, es.Id, es.TTL, es.Timestamp, es.Document)
		}
	}
	log.Println("Slurper closing down")
	done <- true
	// Make sure we really flushed all pending things before returning.
	indexer.Flush()
}
