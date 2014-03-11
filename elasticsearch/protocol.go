package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
)

type ByteSize float64

const (
	_           = iota
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
)
const newline byte = 10

// BulkEntry is one complete entry for elasticsearch bulk requests
type BulkEntry interface {
	Operationer
	Identifier
	Documenter
}

// BulkBodyFull will be returned when the configured max ByteSize has been reached
var BulkBodyFull = errors.New("No more operations can be added")

// BulkBody creates valid bulk data to be used by ES _bulk requests.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html
type BulkBody struct {
	bytes.Buffer
	max  ByteSize
	done bool
}

// indexHeader is the first part of a bulk request, the second part is the values
type indexHeader struct {
	Name string `json:"_index"`
	Type string `json:"_type"`
	Id   string `json:"_id"`
}

// NewBulkBody will return a new BulkBody configured to return an error upon adding more bytes than
// max.
func NewBulkBody(max ByteSize) *BulkBody {
	return &BulkBody{max: max}
}

// Add will write one new bulk operation to the buffer. Returns BulkBodyFull when maxed out.
// If BulkBodyFull has been returned, the buffer should be sent and Reset() until more operations
// can be added.
func (bulk *BulkBody) Add(v BulkEntry) error {
	// Clear done bool on resets
	if bulk.Len() == 0 && bulk.done {
		bulk.done = false
	}
	// Don't allow more additions if we are full
	if bulk.done {
		return BulkBodyFull
	}
	if ByteSize(bulk.Len()) >= bulk.max {
		bulk.Done()
		return BulkBodyFull
	}

	// First part is a header identifying what to do
	header := indexHeader{}
	if i, err := v.Index(); err != nil {
		return err
	} else {
		header.Name = i
	}
	if t, err := v.Type(); err != nil {
		return err
	} else {
		header.Type = t
	}
	if id, err := v.Id(); err != nil {
		return err
	} else {
		header.Id = id
	}
	action, err := v.Action()
	if err != nil {
		return err
	}

	parts := make([][]byte, 0, 3)
	if headerJson, err := json.Marshal(map[string]interface{}{action: header}); err != nil {
		return err
	} else {
		parts = append(parts, headerJson)
	}

	// Then is the values that should be applied
	doc, err := v.Document()
	if err != nil {
		return err
	}
	// Updates needs to be wrapped with additional options
	if action == "update" {
		// No need to send meaningless operations
		if len(doc) == 0 {
			return errors.New("Empty document, nothing would get changed!")
		}
		doc = map[string]interface{}{
			"doc":           doc,
			"doc_as_upsert": true,
		}
	}

	// Deletes doesn't need to provide values
	if action != "delete" {
		valuesJson, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		parts = append(parts, valuesJson)
	}

	// Header, values (in case they exist) and final delimeter is separated by newlines
	parts = append(parts, nil)
	entry := bytes.Join(parts, []byte{newline})
	_, err = (*bulk).Write(entry)

	return err
}

// Done will append the final byte to mark the end of a bulk body. Should be called after all
// operations has been added.
func (bulk *BulkBody) Done() error {
	if !bulk.done {
		if err := bulk.WriteByte(newline); err != nil {
			return err
		}
		bulk.done = true
	}
	return nil
}
