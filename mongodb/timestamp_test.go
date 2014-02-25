package mongodb

import (
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
