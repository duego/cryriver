package mongodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duego/cryriver/stats"
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

func (op *Operation) ObjectId() (bson.ObjectId, error) {
	var object bson.M

	switch op.Op {
	case Update:
		object = op.UpdateObject
	default:
		object = op.Object
	}

	id, ok := object["_id"]
	if !ok {
		return bson.ObjectId(""), OperationError{"_id does not exist in object", op}
	}

	bid, ok := id.(bson.ObjectId)
	if !ok {
		return bson.ObjectId(""), OperationError{"Could not find a bson objectid", op}
	}
	return bid, nil
}

// Implements the rest of elasticsearch interface for easy export of Operation.
type EsOperation struct {
	*Operation
	manipulators   []Manipulator
	indexMap       map[string]string
	namespaceSplit *[2]string
	doc            map[string]interface{}
	action         string
}

func NewEsOperation(indexes map[string]string, manips []Manipulator, op *Operation) *EsOperation {
	if manips == nil {
		manips = DefaultManipulators
	}
	esOp := EsOperation{
		Operation:    op,
		manipulators: manips,
		indexMap:     indexes,
	}

	// Return delete operation if object delete == true.
	doc, err := esOp.Document()
	if err == nil {
		if v, ok := doc["deleted"]; ok {
			if deleted, ok := v.(bool); deleted && ok {
				esOp = EsOperation{
					Operation: op,
					action:    "delete",
					doc:       make(map[string]interface{}),
					indexMap:  indexes,
				}
			}
		}
	}
	return &esOp
}

// Id returns the object id as a hex string for the current Operation.
func (op *EsOperation) Id() (string, error) {
	id, err := op.Operation.ObjectId()
	if err != nil {
		return "", err
	}
	return id.Hex(), nil
}

func (op *EsOperation) Action() (string, error) {
	if op.action != "" {
		return op.action, nil
	}
	switch op.Op {
	case Update:
		op.action = "update"
	case Insert:
		op.action = "index"
	case Delete:
		op.action = "delete"
	default:
		return "", OperationError{"Unsupported operation", op}
	}
	return op.action, nil
}

// Document returns the changed document for Insert or Update.
func (op *EsOperation) Document() (map[string]interface{}, error) {
	if op.doc != nil {
		return op.doc, nil
	}
	op.doc = make(map[string]interface{})

	var changes bson.M

	switch op.Op {
	case Update:
		// Partial update
		sets, ok := op.Object["$set"]
		if ok {
			stats.Sets.Add(1)
		}
		if unsets, ok := op.Object["$unset"]; ok {
			// Instead of having to find the full object in MongoDB or passing a script to ES
			// we pretend there's a $set with null value which is enough in most cases.
			stats.Unsets.Add(1)
			if sets == nil {
				sets = make(bson.M)
			}
			for key, _ := range unsets.(bson.M) {
				sets.(bson.M)[key] = nil
			}
		}
		if sets != nil {
			changes = sets.(bson.M)
			break
		}
		// All other updates is a full document(?)
		fallthrough
	case Insert:
		stats.Complete.Add(1)
		changes = bson.M(op.Object)
	default:
		return nil, OperationError{"Unsupported operation", op}
	}

	// Run the document through the manipulators to make it look like we want it to before it hits ES
	for _, manip := range op.manipulators {
		if err := manip.Manipulate(&changes); err != nil {
			return nil, err
		}
	}
	// Stored as a map so that ES doesn't have to know about bson.M which is the same.
	op.doc = map[string]interface{}(changes)

	return op.doc, nil
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
	if mapped, ok := op.indexMap[i]; !ok {
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

var DefaultManipulators = make([]Manipulator, 0, 100)
