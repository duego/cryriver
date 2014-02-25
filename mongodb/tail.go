// Package mongodb handles tailing of the oplog on one server.
package mongodb

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

// Tail sends mongodb operations for the namespace on the specified channel.
// To stop tailing, send an error channel to closing, also used for returning errors after opc closes.
func Tail(server, namespace string, lastTs *Timestamp, opc chan<- *Operation, closing chan chan error) {
	session, err := mgo.Dial(server + "?connect=direct")
	if err != nil {
		close(opc)
		<-closing <- err
		return
	}
	defer session.Close()

	col := session.DB("local").C("oplog.rs")
	result := new(Operation)

	query := bson.M{"ns": namespace}
	if lastTs != nil {
		log.Println("Resuming from timestamp:", *lastTs)
		query["ts"] = bson.M{"$gt": *lastTs}
	}
	// Start tailing, sorted by forward natural order by default in capped collections.
	iter := col.Find(query).Tail(-1)
	go func() {
		for iter.Next(result) {
			opc <- result
		}
		close(opc)
	}()

	// Block until closing and use the returned error channel to return any errors from iter close.
	<-closing <- iter.Close()
}

// InitialImport gets all records from a namespace suitable for initial import.
// TODO: func InitialImport(server, namespace, query string)
