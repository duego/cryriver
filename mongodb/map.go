package mongodb

import (
	"errors"
	"fmt"
	"github.com/duego/cryriver/elasticsearch"
	"labix.org/v2/mgo/bson"
)

// Manipulator is used for changing documents in specific ways. These can get added to
// the EsMapper to have changes applied on all mapped operations.
type Manipulator interface {
	Manipulate(doc *bson.M) error
}

// EsMapper maps mongodb operations to elasticsearch operations.
type EsMapper struct {
	Index        string
	Manipulators []Manipulator
}

func NewEsMapper(index string) *EsMapper {
	return &EsMapper{index, DefaultManipulators}
}

func (e *EsMapper) EsMap(m *Operation) (*elasticsearch.Operation, error) {
	op := new(elasticsearch.Operation)
	id, err := m.Id()
	if err != nil {
		return op, errors.New(fmt.Sprint("Could not find an id in", m))
	}
	switch m.Op {
	// FIXME: Updates might be full objects (not $set) where we want to treat it as with Inserts
	// to make sure removed fields don't stick along.
	case Update:
		changes, err := m.Changes()
		if err != nil {
			return op, err
		}
		op = &elasticsearch.Operation{
			Id:        id,
			Timestamp: m.Timestamp.Time(),
			Index:     e.Index,
			Type:      "users",
			TTL:       "",
			Op:        elasticsearch.Update,
			Document: elasticsearch.D{
				// The object in the mongo $set which is a subset of the full document.
				"doc": changes,
				// doc_as_upsert makes sure we will create additional fields should they show up.
				"doc_as_upsert": true,
			},
		}
	case Insert:
		op = &elasticsearch.Operation{
			Id:        id,
			Timestamp: m.Timestamp.Time(),
			Index:     e.Index,
			Type:      "users",
			TTL:       "",
			Op:        elasticsearch.Index,
			Document:  elasticsearch.D(m.Object),
		}
	default:
		return op, errors.New(fmt.Sprint("Operation of type", m.Op, "is not supported yet"))
	}

	// Run the document through the manipulators to make it look like we want it to before it hits ES
	doc := bson.M(op.Document)
	for _, manip := range e.Manipulators {
		err = manip.Manipulate(&doc)
		if err != nil {
			return op, err
		}
	}
	// Silly conversion from bson.M to elasticsearch.D, they are really the same structure
	op.Document = elasticsearch.D(doc)
	return op, nil
}

var DefaultManipulators = make([]Manipulator, 0, 100)
