package mongodb

import (
	"fmt"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"strconv"
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

// GetBSON helps bson marshal understand that we're really a MongoTimestamp
func (t Timestamp) GetBSON() (interface{}, error) {
	return bson.MongoTimestamp(t), nil
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%s Ordinal: %d", t.Time(), t.Ordinal())
}

// Save writes the timestamp as a string using io.Writer.
func (t Timestamp) Save(w io.Writer) error {
	ts := []byte(strconv.FormatInt(int64(t), 10))

	if _, err := w.Write(ts); err != nil {
		return err
	}

	return nil
}

// Load sets the timestamp provided by an io.Reader.
func (t *Timestamp) Load(r io.Reader) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	i, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	*t = Timestamp(i)
	return nil
}

// Optime is marked "internal" by 10gen and is also broken as it reports the wrong bson type.
// Work around this by providing the interface to unmarshal it properly.
type optime Timestamp

func (o *optime) SetBSON(raw bson.Raw) error {
	b := raw.Data
	*o = optime((uint64(b[0]) << 0) |
		(uint64(b[1]) << 8) |
		(uint64(b[2]) << 16) |
		(uint64(b[3]) << 24) |
		(uint64(b[4]) << 32) |
		(uint64(b[5]) << 40) |
		(uint64(b[6]) << 48) |
		(uint64(b[7]) << 56))
	return nil
}
