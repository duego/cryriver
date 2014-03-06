package mongodb

import (
	"labix.org/v2/mgo/bson"
	"testing"
)

func TestTraverser(t *testing.T) {
	doc := bson.M{
		"one": bson.M{
			"two": bson.M{
				"three": "four",
			},
		},
	}
	if traverser := NewBsonTraverser(doc).Next("one").Next("two").Next("three"); traverser.Value() != "four" {
		t.Error("Expected to find the value")
	} else if traverser.Next("foo").Value() != nil {
		t.Error("Expected invalid keys to set nil as value")
	}
}
