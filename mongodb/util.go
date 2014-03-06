package mongodb

import (
	"labix.org/v2/mgo/bson"
)

type BsonTraverser struct {
	bson.M
	value interface{}
}

func NewBsonTraverser(m bson.M) *BsonTraverser {
	return &BsonTraverser{M: m}
}

func (b BsonTraverser) Next(key string) BsonTraverser {
	if next, ok := b.M[key]; ok {
		switch t := next.(type) {
		case bson.M:
			b = BsonTraverser{M: t}
		case map[string]interface{}:
			b = BsonTraverser{M: bson.M(t)}
		default:
			b.value = t
		}
	} else {
		// Reached something else than a string map but we are trying to traverse it, set nil
		b.value = nil
	}
	return b
}

func (b BsonTraverser) Value() interface{} {
	return b.value
}
