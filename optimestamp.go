package main

import (
	"expvar"
	"github.com/duego/cryriver/mongodb"
	"log"
	"os"
)

var lastEsSeen *mongodb.Timestamp
var lastEsSeenC = make(chan *mongodb.Timestamp, 1)

func init() {
	// Restore any previously saved timestamp
	if f, err := os.Open(*optimeStore); err != nil {
		log.Println("Failed to load previous lastEsSeen timestamp:", err)
	} else {
		lastEsSeen = new(mongodb.Timestamp)
		lastEsSeen.Load(f)
		f.Close()
	}
	go saveLastEsSeen()

	// Expose and keep track of what the latest timestamp we've forwarded to ES is
	expvar.Publish("lastEsSeen", lastEsSeen)
}

// saveLastEsSeen loops the channel to save our progress on what timestamp we have seen so far
func saveLastEsSeen() {
	for {
		lastEsSeen := <-lastEsSeenC
		if f, err := os.Create(*optimeStore); err != nil {
			log.Println("Error saving oplog timestamp:", err)
		} else {
			if err := lastEsSeen.Save(f); err != nil {
				log.Println("Error saving oplog timestamp:", err)
			}
			f.Close()
		}
	}
}
