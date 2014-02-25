package mongodb

import (
	"bytes"
	"testing"
	"time"
)

func TestTimestamp(t *testing.T) {
	// 5984286097973182465 2014-02-25 10:46:24 +0000 UTC
	ts := Timestamp(5984286097973182465)
	valid := time.Date(2014, time.February, 25, 10, 46, 24, 0, time.UTC)

	if tsTime := ts.Time(); !valid.Equal(*tsTime) {
		t.Error("Times does not match, expected", valid, "got", tsTime)
	}

	if v := int32(0); ts.Ordinal() != v {
		t.Error("Expected ordinal to be", v)
	}
}

func TestSaveTimestamp(t *testing.T) {
	storage := bytes.NewBuffer(nil)
	ts := Timestamp(5984286097973182465)
	if err := ts.Save(storage); err != nil {
		t.Error(err)
	}
	if storage.String() != "5984286097973182465" {
		t.Error("Expected timestamp to be stored as a string number")
	}
}

func TestLoadTimestamp(t *testing.T) {
	storage := bytes.NewBuffer([]byte("5984286097973182465"))
	var ts Timestamp
	ts.Load(storage)

	if int64(ts) != 5984286097973182465 {
		t.Error("Expected string to be parsed as int")
	}

	valid := time.Date(2014, time.February, 25, 10, 46, 24, 0, time.UTC)
	if tsTime := ts.Time(); !valid.Equal(*tsTime) {
		t.Error("Times does not match, expected", valid, "got", tsTime)
	}

}
