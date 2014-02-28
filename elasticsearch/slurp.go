// Package elasticsearch handles indexing documents to an ES server.
package elasticsearch

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

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
	// What action to perform, index or update.
	Action() (string, error)
}

// Transaction as in one complete set of values to perform an operation towards elasticsearch.
type Transaction interface {
	Operationer
	Documenter
	Identifier
	Timestamper
}

// EsClient is used for sending the actual requests to elasticsearch.
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

// SendBulk will accept a populated BulkBody that will be sent using POST.
// If the Post doesn't return any errors, the BulkBody will be Reset to accept new operations.
// Will return an error on non-200 return codes.
func (c EsClient) SendBulk(b *BulkBody) error {
	b.Done()
	log.Println("Send that buffer!", string(b.Bytes()))
	resp, err := c.Post(c.url, "application/x-www-form-urlencoded", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b.Reset()

	// XXX: Do we really need to iterate all items returned to see if all has ok: true?
	if code := resp.StatusCode; code != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return errors.New(fmt.Sprintf("Unexpected status code: %d\n%s", code, string(body)))
	}
	return nil
}

// Slurp collects transactions that will be sent towards elasticsearch in batches.
// Closing the channel will make the function return. Any pending transactions will be flushed before
// returning.
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
			err := bulkBuf.Add(op)
			switch err {
			case nil:
			case BulkBodyFull:
				log.Println("Bulk body full!")
				if err := client.SendBulk(bulkBuf); err != nil {
					log.Println(err)
					// XXX: There is no limit on the amount of pending go routines doing it like this
					// but at least we won't block
					go func() { esc <- op }()
				}
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
