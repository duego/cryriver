// Package mongodb handles tailing of the oplog on one server.
package mongodb

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"strings"
)

// Optime returns the Timestamp for mongo getoptime command, session should be a direct session.
func Optime(s *mgo.Session) (*Timestamp, error) {
	ts := new(Timestamp)

	stats := struct {
		Optime optime
	}{}

	if err := s.Run("getoptime", &stats); err != nil {
		return ts, err
	}

	*ts = Timestamp(stats.Optime)
	return ts, nil
}

// Tail sends mongodb operations for the namespace on the specified channel.
// Interrupts tailing if exit chan closes.
func Tail(server, ns string, initial bool, lastTs *Timestamp, opc chan<- *Operation, exit chan bool) error {
	defer close(opc)

	session, err := mgo.Dial(server + "?connect=direct")
	if err != nil {
		return err
	}
	defer session.Close()

	if initial {
		// If we are doing an intitial import, replace the oplog timestamp with the most current
		// as it doesn't make sense to apply the same objects multple times.
		if ts, err := Optime(session); err != nil {
			return err
		} else {
			lastTs = ts
		}
		nsParts := strings.Split(ns, ".")
		if len(nsParts) != 2 {
			return errors.New("Exected namespace provided as database.collection") // Expected?
		}
		col := session.DB(nsParts[0]).C(nsParts[1])
		iter := col.Find(nil).Iter()
		initialDone := make(chan bool)
		go func() {
			log.Println("Doing initial import, this may take a while...")
			count := 0
			for {
				var result bson.M
				if iter.Next(&result) {
					select {
					case opc <- &Operation{ // Same here I think it looks a bit clunky to have the whole thing inside the case thing.
						Namespace: ns,
						Op:        Insert,
						Object:    result,
					}:
						count++
					case <-exit:
						break
					}
				} else {
					break
				}
			}
			log.Println("Initial import object count:", count)
			close(initialDone)
		}()

	waitInitialSync:
		for {
			select {
			case <-initialDone:
				if err := iter.Close(); err != nil {
					return err
				}
				break waitInitialSync
			case <-exit:
				log.Println("Initial import was interrupted")
				err := iter.Close()
				<-initialDone
				return err
			}
		}
		log.Println("Initial import has completed")
	}

	// Start tailing oplog
	col := session.DB("local").C("oplog.rs")

	query := bson.M{"ns": ns}
	if lastTs != nil {
		log.Println("Resuming oplog from timestamp:", *lastTs)
		query["ts"] = bson.M{"$gt": *lastTs}
	}
	// Start tailing, sorted by forward natural order by default in capped collections.
	iter := col.Find(query).Tail(-1)
	go func() {
		for {
			var result Operation
			if iter.Next(&result) {
				opc <- &result
			} else {
				break
			}
		}
	}()

	// Block until we are supposed to exit, close the iterator when that happens
	<-exit
	return iter.Close()
}
