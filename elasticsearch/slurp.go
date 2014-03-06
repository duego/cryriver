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

type Operationer interface { // Actioner?
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

type BulkSender interface {
	BulkSend(*BulkBody) error
}

// Client is used for sending the actual requests to elasticsearch.
type Client struct {
	*http.Client
	url string
}

func NewClient(url string, maxConn int) *Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: maxConn,
	}
	return &Client{
		&http.Client{Transport: tr},
		url,
	}
}

// BulkSend will accept a populated BulkBody that will be sent using POST.
// If the Post doesn't return any errors, the BulkBody will be Reset to accept new operations.
// Will return an error on non-200 return codes.
func (c Client) BulkSend(b *BulkBody) error {
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
func Slurp(client BulkSender, esc chan Transaction) {
	defer log.Println("Slurper stopped")

	bulkBuf := NewBulkBody(MB)
	bulkTicker := time.NewTicker(time.Second)

	// Loop all incoming operations and send them to the bulk indexer.
	for {
		select {
		case op := <-esc:
			if op == nil {
				if bulkBuf.Len() > 0 {
					if err := client.BulkSend(bulkBuf); err != nil {
						log.Println(err)
					}
				}
				return // Just return? No logging no error no nothing?
			}
			err := bulkBuf.Add(op)
			switch err {
			case nil:
			case BulkBodyFull:
				log.Println("Bulk body full!")
				if err := client.BulkSend(bulkBuf); err != nil {
					log.Println(err)
					// XXX: There is no limit on the amount of pending go routines doing it like this
					// but at least we won't block
					go func() { esc <- op }() // Uhm, so this is for... uhm putting things back in the chan?
				}
			default:
				log.Println(err)
			}
		case <-bulkTicker.C:
			if bulkBuf.Len() > 0 {
				log.Println("It is time!")
				if err := client.BulkSend(bulkBuf); err != nil {
					log.Println(err)
				}
			}
		}
	}
}
