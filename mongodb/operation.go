package mongodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"labix.org/v2/mgo/bson"
	"strings"
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

// OperationError formats errors to have a pretty printed json object to accompany the message.
type OperationError struct {
	// The error message
	msg string

	// Something that we want to json marshall for a pretty object output
	op interface{}
}

func (oe OperationError) Error() string {
	return fmt.Sprintf("%s\n%s", oe.msg, oe.op)
}

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

func (op Operation) String() string {
	if b, err := json.MarshalIndent(op, "", "\t"); err != nil {
		return fmt.Sprint(op)
	} else {
		return string(b)
	}
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
		return "", OperationError{"_id does not exist in object", op}
	}

	bid, ok := id.(bson.ObjectId)
	if !ok {
		return "", OperationError{"Could not find a bson objectid", op}
	}

	return bid.Hex(), nil
}

// Implements the rest of elasticsearch interface for easy export of Operation.
type EsOperation struct {
	*Operation
	IndexMap       map[string]string
	Manipulators   []Manipulator
	namespaceSplit *[2]string
}

func (op *EsOperation) Action() (string, error) {
	switch op.Op {
	case Update:
		return "update", nil
	case Insert:
		return "index", nil
	default:
		return "", OperationError{"Unsupported operation", op}
	}
}

// Document returns the changed document for Insert or Update.
func (op *EsOperation) Document() (map[string]interface{}, error) {
	var (
		changes bson.M
		err     error
	)
	switch op.Op {
	case Update:
		// Partial update
		if v, ok := op.Object["$set"]; ok {
			changes = v.(bson.M)
			break
		}
		// TODO: Unsetting fields
		if _, ok := op.Object["$unset"]; ok {
			err = OperationError{"$unset not supported yet", op}
			break
		}
		// All other updates is a full document(?)
		changes = bson.M(op.Object)
	case Insert:
		changes = bson.M(op.Object)
	default:
		err = OperationError{"Unsupported operation", op}
	}
	if err != nil {
		return changes, err
	}

	// Run the document through the manipulators to make it look like we want it to before it hits ES
	for _, manip := range op.Manipulators {
		if err := manip.Manipulate(&changes); err != nil {
			return changes, err
		}
	}

	// Return as a map so that ES doesn't have to know about bson.M
	return map[string]interface{}(changes), nil
}

// nsSplit is used for splitting the namespace for Index() and Type().
func (op *EsOperation) nsSplit() (string, string, error) {
	if op.namespaceSplit != nil {
		return op.namespaceSplit[0], op.namespaceSplit[1], nil
	}
	parts := strings.Split(op.Namespace, ".")
	if len(parts) != 2 {
		return "", "", OperationError{"Invalid namespace", op}
	}
	return parts[0], parts[1], nil
}

func (op *EsOperation) Index() (string, error) {
	i, _, e := op.nsSplit()
	if e != nil {
		return i, e
	}
	if mapped, ok := op.IndexMap[i]; !ok {
		return i, errors.New(fmt.Sprint("No mapped index found for:", i))
	} else {
		return mapped, nil
	}
}
func (op *EsOperation) Type() (string, error) {
	_, t, e := op.nsSplit()
	return t, e
}

func (op *EsOperation) Time() *time.Time {
	return op.Timestamp.Time()
}

// Manipulator is used for changing documents in specific ways. These can get added to
// the EsOperation to have changes applied on all mapped operations.
type Manipulator interface {
	Manipulate(doc *bson.M) error
}

// ManipulateFunc makes a function into a Manipulator
type ManipulateFunc func(doc *bson.M) error

func (m ManipulateFunc) Manipulate(doc *bson.M) error {
	return m(doc)
}

type ManipulatorFunc func(*map[string]interface{}) error

func (f ManipulatorFunc) Manipulate(doc *map[string]interface{}) error {
	return f(doc)
}

var DefaultManipulators = make([]Manipulator, 0, 100)
