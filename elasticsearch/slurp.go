// Package elasticsearch handles indexing documents to an ES server.
package elasticsearch

import (
	"github.com/duego/cryriver/bridge"
	"github.com/duego/cryriver/mongodb"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
)

// D for document, convenience type for indexing ES documents.
type D map[string]interface{}

// Slurp attaches batch indexer to specified server and feeds transformed mongodb ops into it,
// closing opc will stop the indexer.
func Slurp(server, index string, opc chan *mongodb.Operation, transformations []bridge.Transformer) {
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
		log.Println("Error channel closed")
	}()

	// Loop all incoming mongodb operations and send them to the bulk indexer.
	for op := range opc {
		id, err := op.Id()
		if err != nil {
			log.Println("Could not find an id in", op)
			continue
		}
		switch op.Op {
		// FIXME: Updates might be full objects (not $set) where we want to treat it as with Inserts
		// to make sure removed fields don't stick along.
		case mongodb.Update:
			changes, err := op.Changes()
			if err != nil {
				log.Println(err)
				continue
			}
			indexer.Update(index, "users", id, "", op.Timestamp.Time(), D{
				// The object in the mongo $set which is a subset of the full document.
				"doc": changes,
				// doc_as_upsert makes sure we will create additional fields should they show up.
				"doc_as_upsert": true,
			})
		case mongodb.Insert:
			indexer.Index(index, "users", id, "", op.Timestamp.Time(), op.Object)
		default:
			log.Println("Operation of type", op.Op, "is not supported yet")
		}
	}
	done <- true
	// Make sure we really flushed all pending things before returning.
	indexer.Flush()
}
