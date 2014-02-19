// Package mongodb handles tailing of the oplog on one server.
package mongodb

import (
	"errors"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
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
func (op *Operation) Changes() (interface{}, error) {
	switch op.Op {
	case Update:
		// Partial update
		if v, ok := op.Object["$set"]; ok {
			return v, nil
		}
		// TODO: Unsetting fields
		if _, ok := op.Object["$unset"]; ok {
			return "", errors.New(fmt.Sprint("$unset not yet supported for: ", op))
		}
		// All other updates is a full document(?)
		return op.Object, nil
	case Insert:
		return op.Object, nil
	default:
		return "", errors.New(fmt.Sprint("Unsupported operation: ", op.Op))
	}
}

type Timestamp bson.MongoTimestamp

// Time converts a mongo timestamp to Time with UTC selected as timezone.
func (t *Timestamp) Time() *time.Time {
	time := time.Unix(int64(*t), 0).In(time.UTC)
	return &time
}

// Tail sends mongodb operations for the namespace on the specified channel.
// To stop tailing, send an error channel to closing, also used for returning errors after opc closes.
func Tail(server, namespace string, opc chan<- *Operation, closing chan chan error) {
	session, err := mgo.Dial(server + "?connect=direct")
	if err != nil {
		close(opc)
		<-closing <- err
		return
	}
	defer session.Close()

	col := session.DB("local").C("oplog.rs")
	result := new(Operation)

	// Start tailing, sorted by forward natural order by default in capped collections.
	// TODO: Provide what id/timestamp to start from.
	iter := col.Find(bson.M{"ns": namespace}).Tail(-1)
	go func() {
		for iter.Next(result) {
			opc <- result
		}
		close(opc)
	}()

	// Block until closing and use the returned error channel to return any errors from iter close.
	<-closing <- iter.Close()
}

// InitialImport gets all records from a namespace suitable for initial import.
// TODO: func InitialImport(server, namespace, query string)
