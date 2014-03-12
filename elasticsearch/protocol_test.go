package elasticsearch

import (
	"bytes"
	"io/ioutil"
	"testing"
)

type rawEntry struct {
	action string
	index  string
	_type  string
	id     string
	values map[string]interface{}
}

func (r *rawEntry) Action() (string, error) {
	return r.action, nil
}

func (r *rawEntry) Document() (map[string]interface{}, error) {
	return r.values, nil
}

func (r *rawEntry) Id() (string, error) {
	return r.id, nil
}

func (r *rawEntry) Type() (string, error) {
	return r._type, nil
}

func (r *rawEntry) Index() (string, error) {
	return r.index, nil
}

func TestBulkBodyAddIndex(t *testing.T) {
	bulk := NewBulkBody(MB)
	if err := bulk.Add(&rawEntry{
		"index",
		"testing",
		"user",
		"123",
		map[string]interface{}{
			"alias": "Johnny",
		},
	}); err != nil {
		t.Fatal(err)
	}
	valid := []byte(`{"index":{"_index":"testing","_type":"user","_id":"123"}}
{"alias":"Johnny"}
`)

	b, err := ioutil.ReadAll(bulk)
	if err != nil {
		t.Error(nil)
	}
	if !bytes.Equal(valid, b) {
		t.Errorf("\n'%s'\nNot equal to:\n'%s'", string(b), string(valid))
	}
}

func TestBulkBodyAddUpdate(t *testing.T) {
	bulk := NewBulkBody(MB)
	if err := bulk.Add(&rawEntry{
		"update",
		"testing",
		"user",
		"123",
		map[string]interface{}{
			"alias": "New Johnny",
		},
	}); err != nil {
		t.Fatal(err)
	}
	valid := []byte(`{"update":{"_index":"testing","_type":"user","_id":"123"}}
{"doc":{"alias":"New Johnny"},"doc_as_upsert":true}
`)

	b, err := ioutil.ReadAll(bulk)
	if err != nil {
		t.Error(nil)
	}
	if !bytes.Equal(valid, b) {
		t.Errorf("\n'%s'\nNot equal to:\n'%s'", string(b), string(valid))
	}
}

func TestBulkBodyMax(t *testing.T) {
	bulk := NewBulkBody(10)
	stuff := rawEntry{
		"index",
		"testing",
		"user",
		"123",
		map[string]interface{}{
			"alias": "Johnny",
		},
	}

	if err := bulk.Add(&stuff); err != nil && err != BulkBodyFull {
		t.Fatal(err)
	}
	if err := bulk.Add(&stuff); err != BulkBodyFull {
		t.Fatal("Expected bulk to return body full error")
	}
	if bulk.Len() < 10 {
		t.Fatal("Expected bulk buffer to exceed 10 bytes")
	}
	if tail := bulk.Bytes()[bulk.Len()-2:]; !bytes.Equal([]byte{newline, newline}, tail) {
		t.Error("Expected an additional newline on the final bulk bytes, got:", tail)
	}
}

func TestBulkDone(t *testing.T) {
	bulk := NewBulkBody(MB)
	stuff := rawEntry{
		"index",
		"testing",
		"user",
		"123",
		map[string]interface{}{
			"foo": "bar",
		},
	}
	bulk.Add(&stuff)
	if bulk.Len() == 0 {
		t.Fatal("Expected some bytes")
	}
	bulk.Done()
	if !bulk.done {
		t.Fatal("Expected to be marked done")
	}

	bulk.Reset()
	bulk.Add(&stuff)
	if bulk.done {
		t.Fatal("Expected done flag to be reset on new addition")
	}
}
