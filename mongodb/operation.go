package mongodb

import (
	"errors"
	"fmt"
	"labix.org/v2/mgo/bson"
)

// OplogOperation defines different kinds of operations an oplog entry can have.
type OplogOperation string

const (
	Update  OplogOperation = "u"
	Insert  OplogOperation = "i"
	Delete  OplogOperation = "d"
	Command OplogOperation = "c"
)

// Operation maps one oplog entry into a structure.
type Operation struct {
	Timestamp Timestamp `bson:"ts"`
	Namespace string    `bson:"ns"`
	Op        OplogOperation

	// The object to be inserted or the parts to be updated, or an id of what to delete.
	Object bson.M `bson:"o"`

	// The target document on update queires, should contain an id.
	UpdateObject bson.M `bson:"o2"`
}

// Id returns the object id as a hex string for the current Operation.
func (op *Operation) Id() (string, error) {
	var object bson.M

	switch op.Op {
	case Update:
		object = op.UpdateObject
	default:
		object = op.Object
	}

	id, ok := object["_id"]
	if !ok {
		return "", errors.New("_id does not exist in object")
	}

	bid, ok := id.(bson.ObjectId)
	if !ok {
		return "", errors.New("Could not find a bson objectid")
	}

	return bid.Hex(), nil
}

// Changes returns the changed document for Insert or Update.
func (op *Operation) Changes() (bson.M, error) {
	var changes bson.M
	switch op.Op {
	case Update:
		// Partial update
		if v, ok := op.Object["$set"]; ok {
			return v.(bson.M), nil
		}
		// TODO: Unsetting fields
		if _, ok := op.Object["$unset"]; ok {
			return changes, errors.New(fmt.Sprint("$unset not yet supported for: ", op))
		}
		// All other updates is a full document(?)
		return op.Object, nil
	case Insert:
		return op.Object, nil
	default:
		return changes, errors.New(fmt.Sprint("Unsupported operation: ", op.Op))
	}
}
