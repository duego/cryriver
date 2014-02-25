package mongodb

import (
	"labix.org/v2/mgo/bson"
	"testing"
	"time"
)

// Not sure of a better way to go through the unmarshalling like is done by cursors,
// here we marshall it so that unmarshall reads it properly and tries to populate our type.
func bsonToOperation(t *testing.T, v *bson.M) *Operation {
	var op Operation
	b, err := bson.Marshal(v)
	if err != nil {
		t.Fatal(err)
		return &op
	}

	if err := bson.Unmarshal(b, &op); err != nil {
		t.Fatal(err)
	}

	return &op
}

func TestDeleteOperation(t *testing.T) {
	op := bsonToOperation(t, &bson.M{
		"ts": bson.MongoTimestamp(5982836443431567364),
		"h":  int64(2778471229732698240),
		"v":  2,
		"op": "d",
		"ns": "test_api.dashboards",
		"b":  true,
		"o": bson.M{
			"_id": bson.ObjectIdHex("52e7e160f4eb2740dda12844"),
		},
	})

	if op.Op != Delete {
		t.Error("Expected operation to be", Delete)
	}
	if id, ok := op.Object["_id"]; !ok {
		t.Error("Expected _id to be set")
	} else {
		if h := id.(bson.ObjectId).Hex(); h != "52e7e160f4eb2740dda12844" {
			t.Error("Invalid object id", h)
		}
	}
	if ns := op.Namespace; ns != "test_api.dashboards" {
		t.Error("Invalid namespace", ns)
	}
	if ts, valid := *op.Timestamp.Time(), time.Date(2014, time.February, 21, 13, 01, 00, 0, time.UTC); ts != valid {
		t.Error("Invalid time", ts, "Expected", valid)
	}
}

func TestUpdateOperation(t *testing.T) {
	op := bsonToOperation(t, &bson.M{
		"ts": bson.MongoTimestamp(5973982510084789348),
		"h":  int64(-7293803671238204358),
		"v":  2,
		"op": "u",
		"ns": "test_api.users",
		"o2": bson.M{
			"_id": bson.ObjectIdHex("52e7db73f4eb27371874b289"),
		},
		"o": bson.M{
			"$set": bson.M{
				"photo_tally": bson.M{
					"total": 1,
				},
			},
		},
	})

	if op.Op != Update {
		t.Error("Expected operation to be", Update)
	}
	if id, err := op.Id(); err != nil {
		t.Error(err)
	} else {
		if id != "52e7db73f4eb27371874b289" {
			t.Error("Invalid Id", id)
		}
	}
	if ns := op.Namespace; ns != "test_api.users" {
		t.Error("Invalid namespace", ns)
	}
	if ts, valid := *op.Timestamp.Time(), time.Date(2014, time.January, 28, 16, 23, 13, 0, time.UTC); ts != valid {
		t.Error("Invalid time", ts, "Expected", valid)
	}
	if _, ok := op.Object["$set"]; !ok {
		t.Error("Expected $set to be available")
	}
	if c, err := op.Changes(); err != nil {
		t.Error(err)
	} else {
		if c["photo_tally"].(bson.M)["total"].(int) != 1 {
			t.Error("Incorrect changeset:", c)
		}
	}
}

func TestInsertOperation(t *testing.T) {
	op := bsonToOperation(t, &bson.M{
		//  2014-01-28 16:23:1q3 +0000 UTC
		"ts": bson.MongoTimestamp(5973982510084788956),
		"h":  int64(-9122761770815979503),
		"v":  2,
		"op": "i",
		"ns": "api.conversations",
		"o": bson.M{
			"_cls":       "Conversation",
			"_id":        bson.ObjectIdHex("50eadae392cd864e50cd0dbc"),
			"created_at": time.Date(2013, time.January, 07, 14, 25, 39, 941e6, time.UTC),
			// ... snip snip ...
		},
	})

	if op.Op != Insert {
		t.Error("Expected operation to be", Insert)
	}
	if id, err := op.Id(); err != nil {
		t.Error(err)
	} else {
		if id != "50eadae392cd864e50cd0dbc" {
			t.Error("Invalid Id", id)
		}
	}
	if ns := op.Namespace; ns != "api.conversations" {
		t.Error("Invalid namespace", ns)
	}
	if createdAt, ok := op.Object["created_at"]; !ok {
		t.Error("Expected to find created_at field")
	} else {
		if ok := createdAt.(time.Time).Equal(time.Date(2013, time.January, 7, 14, 25, 39, 941e6, time.UTC)); !ok {
			t.Error("Invalid created at time", createdAt)
		}
	}
	if c, err := op.Changes(); err != nil {
		t.Error(err)
	} else {
		if c["_cls"].(string) != "Conversation" {
			t.Error("Incorrect changeset:", c)
		}
	}
}
