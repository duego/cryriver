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
// To stop tailing, send an error channel to closing, also used for returning errors after opc closes.
func Tail(server, ns string, initial bool, lastTs *Timestamp, opc chan<- *Operation, closing chan chan error) {
	session, err := mgo.Dial(server + "?connect=direct")
	if err != nil {
		close(opc)
		<-closing <- err
		return
	}
	defer session.Close()

	if initial {
		// If we are doing an intitial import, replace the oplog timestamp with the most current
		// as it doesn't make sense to apply the same objects multple times.
		if ts, err := Optime(session); err != nil {
			close(opc)
			<-closing <- err
			return
		} else {
			lastTs = ts
		}
		nsParts := strings.Split(ns, ".")
		if len(nsParts) != 2 {
			close(opc)
			<-closing <- errors.New("Exected namespace provided as database.collection")
			return
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
					opc <- &Operation{
						Namespace: ns,
						Op:        Insert,
						Object:    result,
					}
					count++
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
					close(opc)
					<-closing <- err
					return
				}
				break waitInitialSync
			case errc := <-closing:
				log.Println("Initial import was interrupted")
				errc <- iter.Close()
				return
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
		close(opc)
	}()

	// Block until closing and use the returned error channel to return any errors from iter close.
	<-closing <- iter.Close()
}
