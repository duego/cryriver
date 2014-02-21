package mongodb

import (
	"errors"
	"fmt"
	"github.com/duego/cryriver/elasticsearch"
)

// EsMapper maps mongodb operations to elasticsearch operations.
// XXX: Should it also apply transformer?
type EsMapper struct {
	*Operation
	Index string
}

func (m *EsMapper) EsMap() (elasticsearch.Operation, error) {
	var op elasticsearch.Operation
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
		return elasticsearch.Operation{
			Id:        id,
			Timestamp: m.Timestamp.Time(),
			Index:     m.Index,
			Type:      "users",
			TTL:       "",
			Op:        elasticsearch.Update,
			Document: elasticsearch.D{
				// The object in the mongo $set which is a subset of the full document.
				"doc": changes,
				// doc_as_upsert makes sure we will create additional fields should they show up.
				"doc_as_upsert": true,
			},
		}, nil
	case Insert:
		return elasticsearch.Operation{
			Id:        id,
			Timestamp: m.Timestamp.Time(),
			Index:     m.Index,
			Type:      "users",
			TTL:       "",
			Op:        elasticsearch.Index,
			Document:  elasticsearch.D(m.Object),
		}, nil
	default:
		return op, errors.New(fmt.Sprint("Operation of type", m.Op, "is not supported yet"))
	}
}
