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
func Tail(session *mgo.Session, ns string, initial bool, lastTs *Timestamp, opc chan<- *Operation, exit chan bool) error {
	defer close(opc)
	defer session.Close()

	// Always do initial import in case a previous optime doesn't exist.
	if lastTs == nil || int64(*lastTs) == 0 {
		initial = true
	}

	if initial {
		// If we are doing an intitial import, replace the oplog timestamp with the most current
		// as it doesn't make sense to apply the same objects multiple times.
		if ts, err := Optime(session); err != nil {
			return err
		} else {
			lastTs = ts
		}
		nsParts := strings.Split(ns, ".")
		if len(nsParts) != 2 {
			return errors.New("Exected namespace provided as database.collection")
		}
		col := session.DB(nsParts[0]).C(nsParts[1])
		iter := col.Find(nil).Iter()
		initialDone := make(chan bool)
		go func() {
			log.Println("Doing initial import, this may take a while...")
			var count uint64
			for {
				var result bson.M
				if iter.Next(&result) {
					select {
					case opc <- &Operation{
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

	log.Println("Resuming oplog from timestamp:", *lastTs)
	log.Println("It could take a moment for MongoDB to scan through the oplog collection...")
	query := bson.M{"ns": ns, "ts": bson.M{"$gt": *lastTs}}

	// Start tailing, sorted by forward natural order by default in capped collections.
	iter := col.Find(query).Tail(-1)
	iterClosed := make(chan bool)
	go func() {
		for {
			var result Operation
			if iter.Next(&result) {
				select {
				case opc <- &result:
				case <-exit:
					break
				}
			} else {
				break
			}
		}
		close(iterClosed)
	}()

	// Block until we are supposed to exit, close the iterator when that happens
	select {
	case <-exit:
	case <-iterClosed:
	}
	err := iter.Close()
	// Make sure iterator has stoped pumping into opc since it will be closed on defered func
	<-iterClosed
	return err
}
