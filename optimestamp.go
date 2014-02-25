package main

import (
	"expvar"
	"github.com/duego/cryriver/mongodb"
)

var lastEsSeen mongodb.Timestamp

func init() {
	// Expose and keep track of what the latest timestamp we've forwarded to ES is
	expvar.Publish("lastEsSeen", &lastEsSeen)
}
