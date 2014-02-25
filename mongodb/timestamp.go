package mongodb

import (
	"fmt"
	"labix.org/v2/mgo/bson"
	"time"
)

type Timestamp bson.MongoTimestamp

// Time converts a mongo timestamp to Time with UTC selected as timezone.
func (t *Timestamp) Time() *time.Time {
	// Mongo special timestamp: First 32 bits are seconds, last 32 bits are a counter in a second.
	// originally used to keep timestamps unique in oplog. We only need the seconds part.
	i := int64(*t >> 32)
	time := time.Unix(i, 0).In(time.UTC)
	return &time
}

// Ordinal is the counter part of the special Mongo timestamp. This increments for operations on the
// same second to make sure the value is unique.
func (t *Timestamp) Ordinal() int32 {
	return int32(*t << 32)
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%s Ordinal: %d", t.Time(), t.Ordinal())
}
