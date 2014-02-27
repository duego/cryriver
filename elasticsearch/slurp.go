// Package elasticsearch handles indexing documents to an ES server.
package elasticsearch

import (
	"fmt"
	"log"
	"net/http"
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
	Index     string
	Type      string
	TTL       string
	Timestamp *time.Time
	Op        IndexerOperation
	Document  D
}

type Timestamper interface {
	Time() *time.Time
}

type Identifier interface {
	Index() (string, error)
	Type() (string, error)
	Id() (string, error)
}

type Documenter interface {
	Document() (map[string]interface{}, error)
}

type Operationer interface {
	// What action to perform, index or update
	Action() (string, error)
}

type Transaction interface {
	Operationer
	Documenter
	Identifier
	Timestamper
}

type EsClient struct {
	*http.Client
	url string
}

func NewEsClient(url string, maxConn int) *EsClient {
	tr := &http.Transport{
		MaxIdleConnsPerHost: maxConn,
	}
	return &EsClient{
		&http.Client{Transport: tr},
		url,
	}
}

func (c EsClient) SendBulk(b *BulkBody) error {
	b.Done()
	log.Println("Send that buffer!", string(b.Bytes()))
	resp, err := c.Post(c.url, "application/x-www-form-urlencoded", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b.Reset()
	// TODO: Catch returned ES errors here
	log.Println(resp)
	return nil
}

// Slurp attaches batch indexer to specified server and feeds mapped operations into it,
// closing mapc will stop the indexer.
func Slurp(server string, esc chan Transaction) {
	defer log.Println("Slurper stopped")

	client := NewEsClient(fmt.Sprintf("%s/_bulk", server), 10)
	bulkBuf := NewBulkBody(MB)
	bulkTicker := time.NewTicker(time.Second)

	// Loop all incoming operations and send them to the bulk indexer.
	for {
		select {
		case op := <-esc:
			if op == nil {
				if bulkBuf.Len() > 0 {
					if err := client.SendBulk(bulkBuf); err != nil {
						log.Println(err)
					}
				}
				return
			}
		retry:
			err := bulkBuf.Add(op)
			switch err {
			case nil:
			case BulkBodyFull:
				log.Println("Bulk body full!")
				// FIXME: What to do here if we get an error? It would be a blocking loop
				if err := client.SendBulk(bulkBuf); err != nil {
					log.Println(err)
				}
				goto retry
			default:
				log.Println(err)
			}
		case <-bulkTicker.C:
			if bulkBuf.Len() > 0 {
				log.Println("It is time!")
				if err := client.SendBulk(bulkBuf); err != nil {
					log.Println(err)
				}
			}
		}
	}
}
